#pragma once

#include <Raft/RaftEntry.h>
#include <Raft/RaftLog.h>
#include <Raft/RaftState.h>

#include <cstdint>
#include <mutex>
#include <vector>

enum class ProposeStatus : std::uint8_t {
    // The entry applied to this node's state machine. Committed on a majority,
    // so it is durable cluster-wide.
    Committed = 0,
    // Definitively did not commit: a later leader overwrote our index. Safe to
    // retry.
    Failed,
    // Proposed, but its fate was not established before COMMIT_TIMEOUT. It may
    // still commit. NOT safe to blindly retry - the one answer a client without
    // deduplication cannot act on.
    Unknown,
    // This node is not the leader, so nothing was proposed at all.
    NotLeader,
};

// The session side of replication: turns a transaction's buffered writes into a
// Raft entry and waits for it to apply.
//
// Split out of CommandServer so the commit invariants can be tested without a
// socket, in the same spirit as RaftApplier::apply_pending_batch().
class RaftProposer {
public:
    RaftProposer(RaftState& state, RaftLog& raft_log);

    RaftProposer(const RaftProposer&) = delete;
    RaftProposer& operator=(const RaftProposer&) = delete;

    // Appends the operations as one Raft entry and blocks until it applies, is
    // provably discarded, or COMMIT_TIMEOUT elapses.
    //
    // The leadership check, the term read and the append happen under ONE hold
    // of state_mutex. Split them and a deposed server stamps an entry with a
    // stale term, which the new leader truncates while this session waits on an
    // index that will never apply.
    //
    // An empty operation list still produces an entry. A transaction that
    // committed having written nothing is still a transaction, and the apply
    // loop handles a no-op entry.
    ProposeStatus propose(std::vector<MutationOp> operations);

private:
    // Caller holds the lock; it is released while waiting. See *Returning the
    // commit result* - the order of the checks is load-bearing.
    ProposeStatus await_commit(std::unique_lock<std::mutex>& lock,
                               std::uint64_t index,
                               std::uint64_t my_term);

    RaftState& state_;
    RaftLog& raft_log_;
};
