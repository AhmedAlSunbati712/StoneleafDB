#pragma once

#include <Raft/RaftLog.h>
#include <Raft/RaftState.h>

// The single place commit_index moves on a leader. The caller must hold
// state_mutex.
//
// This lives in its own header and translation unit, apart from RaftReplicator,
// for a linkage reason rather than a stylistic one. It has two callers that are
// not each other:
//
//   - every replication thread, after a peer accepts entries;
//   - the proposing session, once its own copy reaches disk.
//
// The second is not an optimization. A single-node cluster has no replication
// threads at all, so without that caller nothing would ever commit; and even in
// a larger cluster the leader's own fsync completing is frequently what forms
// the majority. But RaftReplicator's implementation includes the generated gRPC
// headers, so defining this alongside it would drag protobuf and gRPC into
// $(LIB) and therefore into every test binary that links the library. Keeping it
// proto-free is what lets the session path use it without that dependency.
//
// It enforces the current-term rule (Raft 5.4.2 / Figure 8): a leader may only
// commit an entry from its OWN term by counting replicas. Earlier terms' entries
// commit transitively once a current-term entry does.
void advance_commit_index(RaftState& state, RaftLog& raft_log);
