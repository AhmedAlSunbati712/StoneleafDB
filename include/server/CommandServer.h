#pragma once

#include <KeyStore.h>
#include <Raft/TransactionWriteBuffer.h>
#include <TransactionManager/TransactionManager.h>

class RaftProposer;
class RaftReadIndex;

struct SessionContext {
    TransactionHandle active_transaction;

    // Only used when this connection is replicating. The session's local
    // transaction then exists purely to own the logical key locks: the writes
    // themselves live here until they are serialized into a Raft entry, and the
    // B-tree is touched only later, by the apply loop, on every node.
    TransactionWriteBuffer write_buffer;
};

namespace CommandServer {

// proposer == nullptr serves the connection exactly as before: writes go
// straight through KeyStore and are visible immediately. That is what the
// standalone engine and the CommandServer tests use, and it is why replication
// is a parameter rather than a requirement - a node with no cluster must still
// be able to accept a write.
//
// With a proposer, writes are buffered and replicated: a write is acknowledged
// only once its Raft entry has been committed by a majority and applied here.
//
// Required rather than defaulted because callers pass this function to
// std::thread by address, and a default argument does not apply through a
// function pointer.
void serve_connection(
    int socket_fd,
    KeyStore &key_store,
    TransactionManager &transaction_manager,
    RaftProposer *proposer,
    // Gate for consistent reads on a replicated node. nullptr serves reads
    // from local state without confirming leadership, which is what a
    // standalone node does and all a follower can offer.
    RaftReadIndex *read_index) noexcept;

} // namespace CommandServer
