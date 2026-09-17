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

void Segment::sync_store() {
    {
        std::shared_lock lock(mutex_);
        if (recovery_required_) {
            throw std::runtime_error("Segment must be recovered before synchronization");
        }
    }
    store_.sync();
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

    // Phase 2: walk the authoritative Store and record where every frame
    // starts, validating the dense absolute-LSN sequence as we go. Store is
    // synced before any record it holds is acknowledged; the Index is not
    // synced on that path at all, so nothing in it is trusted until checked
    // against these offsets.
    std::vector<std::uint64_t> store_offsets;
    store_offsets.reserve(static_cast<std::size_t>(store_scan.record_count));
    std::uint64_t cursor = 0;
    while (cursor < store_scan.valid_size) {
        const std::uint64_t relative_lsn = store_offsets.size();
        const std::vector<char> encoded = store_.read(cursor);
        const WalRecord record = WalRecordCodec::decode(encoded);
        if (record.lsn != base_lsn_ + relative_lsn) {
            throw std::runtime_error("Store records do not form a dense absolute-LSN sequence");
        }
        if (relative_lsn > std::numeric_limits<std::uint32_t>::max()) {
            throw std::length_error("Segment contains more records than its Index can represent");
        }
        store_offsets.push_back(cursor);
        cursor += Store::RECORD_LENGTH_SIZE + encoded.size();
    }

    // The number of decoded dense records must agree with the framing scan.
    // A disagreement means a structurally complete retained entry was invalid,
    // which is corruption rather than an incomplete crash tail.
    if (store_offsets.size() != store_scan.record_count) {
        throw std::runtime_error("Store framing count disagrees with recovered WAL records");
    }

    // Phase 3: keep the longest Index prefix that agrees with Store. The scan
    // already stops at the first entry whose ordinal is wrong - which is how
    // blocks a crash never wrote, reading back as zeros, show up - and each
    // retained entry must also point at the right frame. Everything after the
    // first disagreement is derived data and is rebuilt below.
    const IndexScanResult index_scan = index_.scan();
    const std::uint64_t candidate_count =
        std::min<std::uint64_t>(index_scan.entry_count, store_offsets.size());
    std::uint64_t common_entry_count = 0;
    while (common_entry_count < candidate_count &&
           index_.read(static_cast<std::uint32_t>(common_entry_count)) ==
               store_offsets[common_entry_count]) {
        common_entry_count += 1;
    }

    // Phase 4: drop the disagreeing or unmatched Index suffix, then recreate
    // every mapping after the common prefix from Store.
    bool index_changed = false;
    if (index_scan.status != IndexScanStatus::Complete ||
        index_scan.entry_count != common_entry_count) {
        index_.truncate_to(common_entry_count);
        index_changed = true;
    }
    for (std::uint64_t ordinal = common_entry_count; ordinal < store_offsets.size(); ++ordinal) {
        index_.append(static_cast<std::uint32_t>(ordinal), store_offsets[ordinal]);
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
