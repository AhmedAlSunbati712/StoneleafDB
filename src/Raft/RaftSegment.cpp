#include <Raft/RaftSegment.h>

#include <Endian.h>
#include <Raft/RaftEntryCodec.h>

#include <algorithm>
#include <limits>
#include <mutex>
#include <stdexcept>
#include <utility>

RaftSegment::RaftSegment(
    std::uint64_t base_index,
    int store_fd,
    int index_fd,
    Config config)
    : config_(config),
      base_index_(base_index),
      next_index_(base_index),
      store_(store_fd),
      index_(index_fd) {
    config_.validate();
    if (base_index_ == 0) {
        throw std::invalid_argument("Raft segment base index zero is reserved for none");
    }
    recover();
}

void RaftSegment::append(const RaftMutationEntry& entry) {
    std::unique_lock lock(mutex_);
    if (recovery_required_) {
        throw std::runtime_error("Raft segment must be reopened before appending");
    }
    if (entry.idx != next_index_) {
        throw std::invalid_argument("Raft entry index must equal segment next index");
    }
    const std::uint64_t ordinal = entry.idx - base_index_;
    if (ordinal > std::numeric_limits<std::uint32_t>::max()) {
        throw std::length_error("Raft segment exceeds the Index representation");
    }

    const std::vector<char> encoded = RaftEntryCodec::encode(entry);
    try {
        const std::uint64_t offset = store_.append(encoded);
        index_.append(static_cast<std::uint32_t>(ordinal), offset);
    } catch (...) {
        recovery_required_ = true;
        throw;
    }
    next_index_ += 1;
}

RaftMutationEntry RaftSegment::read(std::uint64_t index) const {
    std::shared_lock lock(mutex_);
    if (index < base_index_ || index >= next_index_) {
        throw std::out_of_range("Raft index is outside this segment");
    }
    const auto ordinal = static_cast<std::uint32_t>(index - base_index_);
    RaftMutationEntry entry = RaftEntryCodec::decode(store_.read(index_.read(ordinal)));
    if (entry.idx != index) {
        throw std::runtime_error("Raft Index points to an entry with a different index");
    }
    return entry;
}

std::vector<RaftMutationEntry> RaftSegment::scan() const {
    std::shared_lock lock(mutex_);
    const StoreScanResult store_scan = store_.scan();
    std::vector<RaftMutationEntry> entries;
    entries.reserve(static_cast<std::size_t>(store_scan.record_count));

    std::uint64_t offset = 0;
    std::uint64_t expected_index = base_index_;
    while (offset < store_scan.valid_size) {
        const std::vector<char> encoded = store_.read(offset);
        RaftMutationEntry entry = RaftEntryCodec::decode(encoded);
        if (entry.idx != expected_index) {
            throw std::runtime_error("Raft Store entries are not a dense index sequence");
        }
        offset += Store::RECORD_LENGTH_SIZE + encoded.size();
        expected_index += 1;
        entries.push_back(std::move(entry));
    }
    return entries;
}

std::uint64_t RaftSegment::term_at(std::uint64_t index) const {
    std::shared_lock lock(mutex_);
    if (index < base_index_ || index >= next_index_) {
        throw std::out_of_range("Raft index is outside this segment");
    }
    const auto ordinal = static_cast<std::uint32_t>(index - base_index_);
    const auto prefix = store_.read_prefix(
        index_.read(ordinal), sizeof(std::uint64_t));
    return get_u64_be(prefix.data());
}

void RaftSegment::truncate_suffix(std::uint64_t from_index) {
    std::unique_lock lock(mutex_);
    if (recovery_required_) {
        throw std::runtime_error("Raft segment must be reopened before truncating");
    }
    if (from_index < base_index_ || from_index > next_index_) {
        throw std::out_of_range("Raft truncation boundary is outside this segment");
    }
    if (from_index == next_index_) return;

    const std::uint64_t retained = from_index - base_index_;
    try {
        store_.truncate_to(retained);
        index_.truncate_to(retained);
    } catch (...) {
        recovery_required_ = true;
        throw;
    }
    next_index_ = from_index;
}

void RaftSegment::sync() {
    std::unique_lock lock(mutex_);
    if (recovery_required_) {
        throw std::runtime_error("Raft segment must be recovered before synchronization");
    }
    store_.sync();
    index_.sync();
}

bool RaftSegment::is_maxed() const {
    std::shared_lock lock(mutex_);
    return store_.size() > config_.max_store_bytes ||
        index_.size() > config_.max_index_bytes;
}

std::uint64_t RaftSegment::base_index() const {
    std::shared_lock lock(mutex_);
    return base_index_;
}

std::uint64_t RaftSegment::next_index() const {
    std::shared_lock lock(mutex_);
    return next_index_;
}

bool RaftSegment::recovery_required() const {
    std::shared_lock lock(mutex_);
    return recovery_required_;
}

void RaftSegment::recover() {
    StoreScanResult store_scan = store_.scan();
    bool store_changed = false;
    if (store_scan.status == StoreScanStatus::IncompleteTail) {
        store_.repair_tail();
        store_changed = true;
        store_scan = store_.scan();
    }

    const IndexScanResult index_scan = index_.scan();
    if (index_scan.status == IndexScanStatus::Corrupt) {
        throw std::runtime_error("Raft Index contains a corrupt complete entry");
    }

    std::uint64_t common_count = std::min(index_scan.entry_count, store_scan.record_count);
    std::uint64_t store_suffix_offset = 0;
    if (common_count > 0) {
        const std::uint64_t ordinal = common_count - 1;
        const std::uint64_t indexed_offset =
            index_.read(static_cast<std::uint32_t>(ordinal));
        const std::vector<char> encoded = store_.read(indexed_offset);
        const RaftMutationEntry entry = RaftEntryCodec::decode(encoded);
        const std::uint64_t next_offset =
            indexed_offset + Store::RECORD_LENGTH_SIZE + encoded.size();
        if (entry.idx != base_index_ + ordinal || next_offset > store_scan.valid_size) {
            throw std::runtime_error("Raft Index does not identify the expected Store entry");
        }
        store_suffix_offset = next_offset;
    }

    std::vector<std::uint64_t> missing_offsets;
    std::uint64_t ordinal = common_count;
    std::uint64_t cursor = store_suffix_offset;
    while (cursor < store_scan.valid_size) {
        const std::vector<char> encoded = store_.read(cursor);
        const RaftMutationEntry entry = RaftEntryCodec::decode(encoded);
        if (entry.idx != base_index_ + ordinal) {
            throw std::runtime_error("Raft Store entries are not a dense index sequence");
        }
        if (ordinal > std::numeric_limits<std::uint32_t>::max()) {
            throw std::length_error("Raft segment exceeds the Index representation");
        }
        missing_offsets.push_back(cursor);
        cursor += Store::RECORD_LENGTH_SIZE + encoded.size();
        ordinal += 1;
    }
    if (ordinal != store_scan.record_count) {
        throw std::runtime_error("Raft Store framing count disagrees with decoded entries");
    }

    bool index_changed = false;
    if (index_scan.status != IndexScanStatus::Complete ||
        index_scan.entry_count != common_count) {
        index_.truncate_to(common_count);
        index_changed = true;
    }
    for (std::uint64_t offset : missing_offsets) {
        index_.append(static_cast<std::uint32_t>(common_count), offset);
        common_count += 1;
        index_changed = true;
    }

    if (store_changed || index_changed) store_.sync();
    if (index_changed) index_.sync();
    next_index_ = base_index_ + store_scan.record_count;
    recovery_required_ = false;
}
