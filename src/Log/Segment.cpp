#include <Log/Segment.h>

#include <Log/WalRecordCodec.h>

#include <algorithm>
#include <limits>
#include <mutex>
#include <stdexcept>
#include <utility>

Segment::Segment(
    std::uint64_t base_lsn,
    int store_fd,
    int index_fd,
    Config config)
    : config_(config),
      base_lsn_(base_lsn),
      next_lsn_(base_lsn),
      store_(store_fd),
      index_(index_fd) {
    // Validate limits before exposing the Segment. Store and Index already own
    // their descriptors at this point, so normal member destruction closes
    // both descriptors if validation or recovery throws.
    config_.validate();
    if (base_lsn_ == 0) {
        throw std::invalid_argument("Segment base LSN zero is reserved for none");
    }

    // Opening a Segment is also its recovery boundary. This reconciles the
    // derived Index with the authoritative Store and reconstructs next_lsn_.
    recover();
}

void Segment::append(const WalRecord &record) {
    // Store and Index must move forward as one logical append. Serialize the
    // two physical writes with readers and other writers.
    std::unique_lock lock(mutex_);

    // A failed append may have written the Store but not the Index. Do not
    // guess which bytes reached disk; reopening runs the recovery procedure.
    if (recovery_required_) {
        throw std::runtime_error("Segment must be reopened and recovered before appending");
    }

    // The future Log layer allocates absolute LSNs. Segment accepts only the
    // next value so Store records and positional Index entries remain dense.
    if (record.lsn != next_lsn_) {
        throw std::invalid_argument("WAL record LSN must equal Segment next_lsn");
    }

    // Index numbering restarts at zero for each segment. Its on-disk key is a
    // 32-bit relative LSN even though records retain their absolute 64-bit LSN.
    const std::uint64_t relative_lsn = record.lsn - base_lsn_;
    if (relative_lsn > std::numeric_limits<std::uint32_t>::max()) {
        throw std::length_error("Segment relative LSN exceeds the Index representation");
    }

    const std::vector<char> encoded = WalRecordCodec::encode(record);
    try {
        // Store is authoritative, so write it first. If the following Index
        // append fails, recovery can recreate that missing mapping from Store.
        const std::uint64_t store_offset = store_.append(encoded);
        index_.append(static_cast<std::uint32_t>(relative_lsn), store_offset);
    } catch (...) {
        // Even when a lower layer rolls back its own partial write, keep the
        // conservative boundary: no more appends until the pair is rescanned.
        recovery_required_ = true;
        throw;
    }

    // Publish the next allocatable LSN only after both writes have completed.
    next_lsn_ += 1;
}

WalRecord Segment::read(std::uint64_t lsn) const {
    std::shared_lock lock(mutex_);

    if (lsn < base_lsn_ || lsn >= next_lsn_) {
        throw std::out_of_range("Absolute LSN is outside this Segment");
    }

    // Translate the public absolute LSN into the segment-local Index position,
    // then follow the stored byte offset into the authoritative Store.
    const std::uint64_t relative_lsn = lsn - base_lsn_;
    const std::uint64_t store_offset =
        index_.read(static_cast<std::uint32_t>(relative_lsn));
    const std::vector<char> encoded = store_.read(store_offset);
    WalRecord record = WalRecordCodec::decode(encoded);

    // Do not return a record merely because the Index pointed at valid framing;
    // the absolute LSN in the record must identify the value requested.
    if (record.lsn != lsn) {
        throw std::runtime_error("Index entry points to a Store record with a different LSN");
    }
    return record;
}

std::vector<WalRecord> Segment::scan() const {
    std::shared_lock lock(mutex_);

    // Sequential scans walk Store framing directly because Store is the source
    // of truth. Index is deliberately unnecessary for this operation.
    const StoreScanResult store_scan = store_.scan();
    std::vector<WalRecord> records;
    records.reserve(static_cast<std::size_t>(store_scan.record_count));

    std::uint64_t store_offset = 0;
    std::uint64_t expected_lsn = base_lsn_;
    while (store_offset < store_scan.valid_size) {
        const std::vector<char> encoded = store_.read(store_offset);
        WalRecord record = WalRecordCodec::decode(encoded);

        // Dense LSNs let recovery derive both Index positions and next_lsn_
        // without maintaining a second persisted counter.
        if (record.lsn != expected_lsn) {
            throw std::runtime_error("Store records do not form a dense absolute-LSN sequence");
        }

        // Store::read returns only the payload, so advance past both the outer
        // four-byte length prefix and the encoded payload.
        store_offset += Store::RECORD_LENGTH_SIZE + encoded.size();
        expected_lsn += 1;
        records.push_back(std::move(record));
    }

    return records;
}

void Segment::sync() {
    {
        std::shared_lock lock(mutex_);
        if (recovery_required_) {
            throw std::runtime_error("Segment must be recovered before synchronization");
        }
    }
    // Not held across the fsyncs below: a concurrent append only adds bytes
    // beyond what the caller asked to make durable.

    // Preserve the same authority ordering used by append: once the derived
    // Index is durable, every entry it contains must have durable Store bytes.
    store_.sync();
    index_.sync();
}

bool Segment::is_maxed() const {
    std::shared_lock lock(mutex_);

    // The caller checks this after append. Equality is allowed; the complete
    // record that first crosses either limit stays in this segment, and the
    // following record starts the next segment.
    return store_.size() > config_.max_store_bytes ||
        index_.size() > config_.max_index_bytes;
}

std::uint64_t Segment::base_lsn() const {
    std::shared_lock lock(mutex_);
    return base_lsn_;
}

std::uint64_t Segment::next_lsn() const {
    std::shared_lock lock(mutex_);
    return next_lsn_;
}

bool Segment::recovery_required() const {
    std::shared_lock lock(mutex_);
    return recovery_required_;
}

void Segment::recover() {
    // Phase 1: make the authoritative Store end at a complete frame. A crash
    // can leave either a partial length prefix or a partial record payload.
    StoreScanResult store_scan = store_.scan();
    bool store_changed = false;
    if (store_scan.status == StoreScanStatus::IncompleteTail) {
        store_.repair_tail();
        store_changed = true;
        store_scan = store_.scan();
    }

    // Phase 2: identify the complete prefix present in both files. The crash
    // model trusts every structurally complete Index entry; corruption inside
    // a complete entry is outside recovery scope and must fail open.
    const IndexScanResult index_scan = index_.scan();
    if (index_scan.status == IndexScanStatus::Corrupt) {
        throw std::runtime_error("Index contains a corrupt complete entry");
    }

    // Starting at the smaller record count excludes an Index entry whose Store
    // frame did not survive, or Store records whose Index entries did not
    // survive. Both are expected append-ordering outcomes after a crash.
    std::uint64_t common_entry_count =
        std::min(index_scan.entry_count, store_scan.record_count);
    std::uint64_t store_suffix_offset = 0;

    // The final retained Index entry tells us where the missing Store suffix
    // begins. If there is no retained prefix, recovery starts at Store offset
    // zero and recreates every missing mapping.
    if (common_entry_count > 0) {
        const std::uint64_t relative_lsn = common_entry_count - 1;
        const std::uint64_t indexed_store_offset =
            index_.read(static_cast<std::uint32_t>(relative_lsn));
        const std::vector<char> encoded = store_.read(indexed_store_offset);
        const WalRecord record = WalRecordCodec::decode(encoded);
        const std::uint64_t next_store_offset =
            indexed_store_offset + Store::RECORD_LENGTH_SIZE + encoded.size();

        // This is a consistency assertion, not a repair path. A complete entry
        // with damaged offset bytes is outside the supported crash model.
        if (record.lsn != base_lsn_ + relative_lsn ||
            next_store_offset > store_scan.valid_size) {
            throw std::runtime_error("Index entry does not identify the expected Store record");
        }

        store_suffix_offset = next_store_offset;
    }

    // Phase 3: validate the authoritative suffix and remember each frame
    // offset. Finish all Store validation before truncating or rewriting Index,
    // so an invalid Store never destroys a still-useful derived prefix.
    std::vector<std::uint64_t> missing_store_offsets;
    std::uint64_t relative_lsn = common_entry_count;
    std::uint64_t cursor = store_suffix_offset;
    while (cursor < store_scan.valid_size) {
        const std::vector<char> encoded = store_.read(cursor);
        const WalRecord record = WalRecordCodec::decode(encoded);
        if (record.lsn != base_lsn_ + relative_lsn) {
            throw std::runtime_error("Store records do not form a dense absolute-LSN sequence");
        }
        if (relative_lsn > std::numeric_limits<std::uint32_t>::max()) {
            throw std::length_error("Segment contains more records than its Index can represent");
        }

        missing_store_offsets.push_back(cursor);
        cursor += Store::RECORD_LENGTH_SIZE + encoded.size();
        relative_lsn += 1;
    }

    // The number of decoded dense records must agree with the framing scan.
    // A disagreement means a structurally complete retained entry was invalid,
    // which is corruption rather than an incomplete crash tail.
    if (relative_lsn != store_scan.record_count) {
        throw std::runtime_error("Store framing count disagrees with recovered WAL records");
    }

    // Phase 4: discard only bytes explained by a crash: a partial final Index
    // entry or complete entries beyond the repaired Store record count.
    const bool index_needs_truncation =
        index_scan.status != IndexScanStatus::Complete ||
        index_scan.entry_count != common_entry_count;
    bool index_changed = false;
    if (index_needs_truncation) {
        index_.truncate_to(common_entry_count);
        index_changed = true;
    }

    // Recreate only mappings missing after the common prefix. An empty or fully
    // incomplete Index naturally rebuilds from Store offset zero.
    for (std::uint64_t offset : missing_store_offsets) {
        index_.append(static_cast<std::uint32_t>(common_entry_count), offset);
        common_entry_count += 1;
        index_changed = true;
    }

    // Phase 5: make recovery durable in authority order. Sync Store first even
    // when only Index changed, so a durable mapping never outruns its record.
    if (store_changed || index_changed) {
        store_.sync();
    }
    if (index_changed) {
        index_.sync();
    }

    // Store count is now the persistent allocator source. No separate on-disk
    // next-LSN counter is needed or allowed to disagree with these records.
    next_lsn_ = base_lsn_ + store_scan.record_count;
    recovery_required_ = false;
}
