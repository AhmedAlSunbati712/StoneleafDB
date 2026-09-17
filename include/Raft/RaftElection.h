#pragma once

#include <Raft/RaftLog.h>
#include <Raft/RaftState.h>

// Forward declared rather than included: RaftPeerClients.h pulls in the
// generated gRPC headers, and this header is included by server.cpp and the
// tests. Only a reference is held, so the definition is needed in the .cpp
// alone.
class RaftPeerClients;

// Startup step 7: the one thread that decides this node should campaign, and
// runs the campaign when it does.
//
// It is parked on election_cv while this node is Leader - a leader never
// campaigns - and otherwise sleeps until election_deadline. Every wake re-reads
// that deadline, because pushing it forward is precisely how an election is
// suppressed: the AppendEntries receiver resets it on a valid leader's RPC, and
// RequestVote resets it on granting a vote.
//
// Nothing here holds state_mutex across an RPC. Term, log tail and peer list are
// snapshotted under the lock, the lock is released, the votes are collected in
// parallel, and the lock is re-acquired to record each reply. This is distinct
// from the AppendEntries receiver, which deliberately holds the lock across
// local I/O but never across a network call.
class RaftElection {
public:
    RaftElection(RaftState& state, RaftLog& raft_log, RaftPeerClients& peers);

    RaftElection(const RaftElection&) = delete;
    RaftElection& operator=(const RaftElection&) = delete;

    // Thread body: park while leading, otherwise wait out the election deadline
    // and campaign. Returns only once shutting_down is set, so it can be joined.
    void run();

    // One complete election: become a candidate, request votes from every peer
    // in parallel, and return true if this node won. Never waits on the timer,
    // so tests drive it directly. Returns false without campaigning if the node
    // is shutting down or already Leader.
    //
    // Campaign helper threads are joined before this returns - never detached.
    // A detached voter outliving RaftState would be a use-after-free at
    // shutdown, and joining is bounded by the RPC deadline, which is itself
    // below ELECTION_TIMEOUT_MIN.
    bool campaign();

private:
    // Appends one entry with no operations, carrying the new leadership's
    // term, and records its index as leader_term_first_index(). Called after
    // the vote threads join, never from inside one: the append takes
    // append_mutex, and a voter already holds state_mutex.
    void append_leader_noop();

    RaftState& state_;
    RaftLog& raft_log_;
    RaftPeerClients& peers_;
};
