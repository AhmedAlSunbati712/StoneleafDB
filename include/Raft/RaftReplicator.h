#pragma once

#include <Raft/NodeAddress.h>
#include <Raft/RaftLog.h>
#include <Raft/RaftState.h>

#include <cstddef>
#include <cstdint>

// Forward declared: RaftPeerClients.h pulls in the generated gRPC headers, and
// this header is included by server.cpp and the tests.
class RaftPeerClients;

// Startup step 6: one instance, and one thread, per peer. Ships log entries to
// that peer and heartbeats it when there is nothing to ship.
//
// One long-lived thread per follower, never a thread per tick. If the tick
// period is shorter than RPC latency - which it is during catch-up - a
// thread-per-tick design puts several AppendEntries in flight to the same peer,
// their replies land out of order, and a late failure decrements send_next for a
// follower that has already caught up. That corrupts replication progress with
// no visible error. One thread per peer keeps at most one RPC outstanding and
// makes the progress updates trivially serialized.
//
// A heartbeat is not a special case: it is simply an AppendEntries whose entries
// list is empty. It still carries prev_index, prev_term and leader_commit, so it
// still runs the consistency check and still advances the follower's commit
// index.
class RaftReplicator {
public:
    // Bounds one RPC's payload. The whole batch is one fsync on the follower, so
    // larger batches are cheaper per entry, but an unbounded batch on a far
    // behind follower would build a message beyond the size limit.
    static constexpr std::size_t DEFAULT_MAX_ENTRIES_PER_BATCH = 64;

    RaftReplicator(RaftState& state,
                   RaftLog& raft_log,
                   RaftPeerClients& peers,
                   NodeAddress peer,
                   std::size_t max_entries_per_batch = DEFAULT_MAX_ENTRIES_PER_BATCH);

    RaftReplicator(const RaftReplicator&) = delete;
    RaftReplicator& operator=(const RaftReplicator&) = delete;

    // Thread body: parked on replication_cv while not Leader, otherwise sends on
    // every new entry and at least once per HEARTBEAT_INTERVAL. Returns only
    // once shutting_down is set, so it can be joined.
    void run();

    // One AppendEntries to this peer. Returns true if the peer accepted it.
    // Never waits on the timer, so tests drive it directly. Returns false
    // without sending when not Leader.
    bool replicate_once();

private:
    RaftState& state_;
    RaftLog& raft_log_;
    RaftPeerClients& peers_;
    NodeAddress peer_;
    std::size_t max_entries_per_batch_;
};
