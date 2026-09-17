#pragma once

#include <Log/Config.h>
#include <Log/Store.h>
#include <Raft/RaftEntry.h>
#include <storage/Index.h>

#include <cstdint>
#include <shared_mutex>
#include <vector>

class RaftSegment {
public:
    RaftSegment(std::uint64_t base_index, int store_fd, int index_fd, Config config);

    RaftSegment(const RaftSegment&) = delete;
    RaftSegment& operator=(const RaftSegment&) = delete;
    RaftSegment(RaftSegment&&) = delete;
    RaftSegment& operator=(RaftSegment&&) = delete;

    void append(const RaftMutationEntry& entry);
    RaftMutationEntry read(std::uint64_t index) const;
    std::vector<RaftMutationEntry> scan() const;
    std::uint64_t term_at(std::uint64_t index) const;
    void truncate_suffix(std::uint64_t from_index);
    void sync();
    // As Segment::sync_store: durability needs only the Store.
    void sync_store();

    bool is_maxed() const;
    std::uint64_t base_index() const;
    std::uint64_t next_index() const;
    bool recovery_required() const;

private:
    Config config_;
    std::uint64_t base_index_ = 0;
    std::uint64_t next_index_ = 0;
    bool recovery_required_ = false;
    Store store_;
    Index index_;
    mutable std::shared_mutex mutex_;

    void recover();
};
