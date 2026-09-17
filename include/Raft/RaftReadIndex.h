#pragma once

#include <Raft/RaftState.h>

#include <chrono>
#include <cstdint>

enum class ReadIndexStatus : std::uint8_t {
    Ready,      // this node may answer the read from local state
    NotLeader,  // not the leader, or deposed while waiting: the client must retry elsewhere
    Timeout,    // leadership could not be confirmed in time, or the node is shutting down
};

// Raft's read-index rule (§6.4/§8): a leader may answer a read from local
// state only after confirming, since the read arrived, that it is still the
// leader. Its own state cannot tell it: a partitioned leader is never informed
// that it was deposed, so serving locally would hand back data a newer leader
// has already moved past.
//
// The confirmation is a round of ordinary heartbeats - no log entry, no WAL
// record and no fsync - so a consistent read costs one network round trip and
// never touches the disk. One round confirms every read waiting when it opens,
// so the cost per read falls as concurrency rises.
class RaftReadIndex {
public:
    explicit RaftReadIndex(RaftState& state);

    RaftReadIndex(const RaftReadIndex&) = delete;
    RaftReadIndex& operator=(const RaftReadIndex&) = delete;

    // Blocks until this node may serve a consistent read, in three steps:
    //   1. wait for this leadership's no-op to commit, so commit_index is the
    //      true committed prefix (Figure 8);
    //   2. open a confirmation round and wait for a majority to acknowledge it;
    //   3. wait for the state machine to apply through the commit_index taken
    //      in step 1, so the read cannot miss an already committed write.
    ReadIndexStatus wait_until_readable();

    // Bounded well below COMMIT_TIMEOUT: a read is cheap to retry, and a
    // client waiting on a partitioned leader wants to be told quickly.
    static constexpr auto READ_TIMEOUT = std::chrono::milliseconds(1000);

private:
    RaftState& state_;
};
