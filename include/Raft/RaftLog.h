#pragma once

#include <Log/Config.h>
#include <Raft/RaftEntry.h>
#include <Raft/RaftSegment.h>

#include <condition_variable>
#include <memory>
#include <shared_mutex>
#include <span>
#include <string>
#include <vector>

class RaftLog {
public:
    explicit RaftLog(Config config);
    ~RaftLog() noexcept;

    RaftLog(const RaftLog&) = delete;
    RaftLog& operator=(const RaftLog&) = delete;
    RaftLog(RaftLog&&) = delete;
    RaftLog& operator=(RaftLog&&) = delete;

    void open(const std::string& directory);
    void close();
    bool is_open() const noexcept;

    std::uint64_t append(std::uint64_t term, std::vector<MutationOp> operations);
    void append_from_leader(
        std::uint64_t first_index,
        std::span<const RaftMutationEntry> entries);
    RaftMutationEntry read(std::uint64_t index) const;
    std::vector<RaftMutationEntry> scan_from(std::uint64_t index) const;
    // At most max_count entries starting at first_index, read through the
    // index one entry at a time. Unlike scan_from(), the cost is bounded by
    // max_count rather than by the size of every segment from first_index on.
    // first_index may equal last_index() + 1, which yields nothing.
    std::vector<RaftMutationEntry> read_range(std::uint64_t first_index, std::size_t max_count) const;

    std::uint64_t last_index() const noexcept;
    std::uint64_t last_term() const noexcept;
    std::uint64_t term_at(std::uint64_t index) const;
    void truncate_suffix(std::uint64_t from_index);
    void sync_through(std::uint64_t index);
    std::uint64_t durable_index() const noexcept;

private:
    Config config_;
    std::string directory_;
    std::vector<std::unique_ptr<RaftSegment>> segments_;
    std::uint64_t next_index_ = 0;
    std::uint64_t last_term_ = 0;
    std::uint64_t durable_index_ = 0;
    bool recovery_required_ = false;
    bool directory_dirty_ = false;
    mutable std::shared_mutex mutex_;

    // Group commit, as in Log: one thread fsyncs at a time with mutex_
    // released; truncate_suffix(), close() and the destructor wait for it,
    // since it holds raw segment pointers and its result must not be applied
    // over a truncation.
    bool sync_in_progress_ = false;
    std::condition_variable_any sync_done_;

    void create_segment(std::uint64_t base_index);
    void wait_for_sync_to_finish(std::unique_lock<std::shared_mutex>& lock);
};
