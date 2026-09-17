#include <server/CommandServer.h>

#include <Raft/RaftReadIndex.h>

#include <Command.h>
#include <LockManager/LockManager.h>
#include <NetCodec.h>
#include <Raft/RaftProposer.h>

#include <arpa/inet.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <limits>
#include <stdexcept>
#include <thread>
#include <vector>

namespace CommandServer {

namespace {

constexpr std::size_t PAYLOAD_SIZE_BYTES = sizeof(std::uint32_t);
constexpr std::uint32_t MAX_COMMAND_PAYLOAD_SIZE = 16 * 1024 * 1024;

void receive_all(int socket_fd, std::uint8_t *buffer, std::size_t size) {
    std::size_t bytes_received = 0;

    while (bytes_received < size) {
        const ssize_t result = ::recv(socket_fd, buffer + bytes_received, size - bytes_received, 0);
        if (result > 0) {
            bytes_received += static_cast<std::size_t>(result);
            continue;
        }

        if (result < 0 && errno == EINTR) {
            continue;
        }

        if (result == 0) {
            throw std::runtime_error("[ERROR] Client closed the connection");
        }

        throw std::runtime_error("[ERROR] Failed to receive command");
    }
}

void send_all(int socket_fd, const std::uint8_t *buffer, std::size_t size) {
    std::size_t bytes_sent = 0;

    while (bytes_sent < size) {
        const ssize_t result = ::send(socket_fd, buffer + bytes_sent, size - bytes_sent, MSG_NOSIGNAL);
        if (result > 0) {
            bytes_sent += static_cast<std::size_t>(result);
            continue;
        }

        if (result < 0 && errno == EINTR) {
            continue;
        }

        throw std::runtime_error("[ERROR] Failed to send command response");
    }
}

Command read_command(int socket_fd) {
    // CommandCodec validates the complete packet, so retain the four-byte size
    // prefix while reading the payload in a separate exact-read operation.
    std::vector<std::uint8_t> packet(PAYLOAD_SIZE_BYTES);
    receive_all(socket_fd, packet.data(), packet.size());

    std::uint32_t network_payload_size = 0;
    std::memcpy(&network_payload_size, packet.data(), sizeof(network_payload_size));
    const std::uint32_t host_payload_size = ntohl(network_payload_size);
    if (host_payload_size > MAX_COMMAND_PAYLOAD_SIZE) {
        throw std::runtime_error("[ERROR] Command payload is too large");
    }

    packet.resize(PAYLOAD_SIZE_BYTES + static_cast<std::size_t>(host_payload_size));
    receive_all(socket_fd, packet.data() + PAYLOAD_SIZE_BYTES, host_payload_size);
    return CommandCodec::deserialize(packet);
}

void send_operation_response(int socket_fd, KeyStoreStatus status) {
    const std::uint8_t response = status == KeyStoreStatus::Success ? 1 : 0;
    send_all(socket_fd, &response, sizeof(response));
}

void send_get_response(int socket_fd, const KeyStoreGetResult &result) {
    if (result.status == KeyStoreStatus::KeyNotFound) {
        const std::uint32_t network_response_size = htonl(0);
        send_all(socket_fd, reinterpret_cast<const std::uint8_t *>(&network_response_size), sizeof(network_response_size));
        return;
    }

    if (result.status != KeyStoreStatus::Success || !result.value.has_value()) {
        throw std::runtime_error("[ERROR] Failed to execute get command");
    }

    const std::vector<std::uint8_t> network_value = NetCodec::serialize_value(*result.value);
    if (network_value.size() > std::numeric_limits<std::uint32_t>::max()) {
        throw std::runtime_error("[ERROR] Serialized value response is too large");
    }

    const std::uint32_t network_response_size = htonl(static_cast<std::uint32_t>(network_value.size()));
    send_all(socket_fd, reinterpret_cast<const std::uint8_t *>(&network_response_size), sizeof(network_response_size));
    send_all(socket_fd, network_value.data(), network_value.size());
}

TransactionHandle command_transaction(
    SessionContext &context,
    TransactionManager &transaction_manager,
    bool &implicit
) {
    implicit = !context.active_transaction;
    return implicit
        ? transaction_manager.begin()
        : context.active_transaction;
}

void finish_implicit_transaction(
    const TransactionHandle &transaction,
    TransactionManager &transaction_manager,
    bool success,
    AbortReason failure_reason
) {
    if (success) {
        if (transaction_manager.commit(transaction) != CommitStatus::Success) {
            throw std::runtime_error("[ERROR] Failed to commit implicit transaction");
        }
        return;
    }

    if (transaction_manager.abort(
            transaction,
            failure_reason) != AbortStatus::Success) {
        throw std::runtime_error("[ERROR] Failed to abort implicit transaction");
    }
}

// --- Replicated path ------------------------------------------------------

// Takes the exclusive logical lock without touching the B-tree. The write
// itself is buffered and only reaches storage later, through the apply loop, so
// the lock is what provides write-write safety in the meantime: the proposing
// session holds it until its entry has applied, and the apply loop deliberately
// takes none (Locking::Skip) so it cannot deadlock against that session.
//
// Mirrors KeyStore's own status handling, including treating a lock this
// transaction already holds as success - a second write to one key is normal.
KeyStoreStatus lock_for_write(
    TransactionManager &transaction_manager,
    const TransactionHandle &transaction,
    const Key &key
) {
    const LockManagerStatus status =
        transaction_manager.lock_manager().lock_exclusive(transaction->id(), key);
    if (status == LockManagerStatus::Deadlock) return KeyStoreStatus::Deadlock;
    if (status == LockManagerStatus::TransactionNotFound) {
        return KeyStoreStatus::TransactionNotFound;
    }
    if (status != LockManagerStatus::Success &&
        status != LockManagerStatus::TxnHoldsExclusive) {
        return KeyStoreStatus::WriteFailed;
    }
    return KeyStoreStatus::Success;
}

KeyStoreStatus to_status(ProposeStatus status) {
    switch (status) {
        case ProposeStatus::Committed: return KeyStoreStatus::Success;
        case ProposeStatus::Failed:    return KeyStoreStatus::CommitFailed;
        case ProposeStatus::NotLeader: return KeyStoreStatus::NotLeader;
        case ProposeStatus::Unknown:   break;
    }
    return KeyStoreStatus::CommitUnknown;
}

// Proposes the session's buffered writes and settles its local transaction.
// The local transaction wrote nothing - it existed only to hold locks - so
// committing it simply releases them.
KeyStoreStatus propose_and_settle(
    SessionContext &context,
    TransactionManager &transaction_manager,
    RaftProposer &proposer
) {
    const ProposeStatus status = proposer.propose(context.write_buffer.to_operations());
    context.write_buffer.clear();

    // Either way the locks must go: the entry's fate no longer depends on them,
    // and holding them would block every other session on those keys.
    if (status == ProposeStatus::Committed) {
        transaction_manager.commit(context.active_transaction);
    } else {
        transaction_manager.abort(context.active_transaction, AbortReason::StatementFailure);
    }
    context.active_transaction.reset();

    return to_status(status);
}

// Maps the read-index gate onto the statuses the wire protocol already carries.
KeyStoreStatus to_status(ReadIndexStatus status) {
    switch (status) {
        case ReadIndexStatus::Ready:     return KeyStoreStatus::Success;
        case ReadIndexStatus::NotLeader: return KeyStoreStatus::NotLeader;
        case ReadIndexStatus::Timeout:   break;
    }
    return KeyStoreStatus::ReadFailed;
}

void execute_replicated_command(
    int socket_fd,
    KeyStore &key_store,
    TransactionManager &transaction_manager,
    SessionContext &context,
    RaftProposer &proposer,
    RaftReadIndex *read_index,
    const Command &command
) {
    if (command.op == Operator::BEGIN_TXN) {
        if (context.active_transaction) {
            send_operation_response(socket_fd, KeyStoreStatus::TransactionAlreadyActive);
            return;
        }
        context.active_transaction = transaction_manager.begin();
        context.write_buffer.clear();
        send_operation_response(socket_fd, KeyStoreStatus::Success);
        return;
    }

    if (command.op == Operator::COMMIT) {
        if (!context.active_transaction) {
            send_operation_response(socket_fd, KeyStoreStatus::NoActiveTransaction);
            return;
        }
        if (context.write_buffer.empty()) {
            // A read-only transaction has nothing to replicate. Proposing an
            // empty entry would cost a Raft round trip, and on a follower it
            // would be rejected outright - so reads commit locally, anywhere.
            const CommitStatus status = transaction_manager.commit(context.active_transaction);
            context.active_transaction.reset();
            send_operation_response(
                socket_fd,
                status == CommitStatus::Success
                    ? KeyStoreStatus::Success
                    : KeyStoreStatus::CommitFailed);
            return;
        }
        send_operation_response(
            socket_fd,
            propose_and_settle(context, transaction_manager, proposer));
        return;
    }

    if (command.op == Operator::ROLLBACK) {
        if (!context.active_transaction) {
            send_operation_response(socket_fd, KeyStoreStatus::NoActiveTransaction);
            return;
        }
        // Nothing was proposed, so discarding the buffer is the whole rollback.
        context.write_buffer.clear();
        const AbortStatus status = transaction_manager.abort(
            context.active_transaction,
            AbortReason::ClientRequest);
        if (status == AbortStatus::Success) context.active_transaction.reset();
        send_operation_response(
            socket_fd,
            status == AbortStatus::Success
                ? KeyStoreStatus::Success
                : KeyStoreStatus::RollbackFailed);
        return;
    }

    bool implicit = false;
    const TransactionHandle transaction = command_transaction(
        context, transaction_manager, implicit);
    if (implicit) context.active_transaction = transaction;

    if (command.op == Operator::GET) {
        // Read-your-own-writes: the buffer is consulted first, and a tombstone
        // there is a miss rather than a fall-through - otherwise a key this
        // transaction deleted would still be served from the B-tree.
        Value buffered;
        const TransactionWriteBuffer::Lookup found =
            context.write_buffer.lookup(*command.key, buffered);

        KeyStoreGetResult result;
        if (found == TransactionWriteBuffer::Lookup::Written) {
            result.status = KeyStoreStatus::Success;
            result.value = buffered;
        } else if (found == TransactionWriteBuffer::Lookup::Deleted) {
            result.status = KeyStoreStatus::KeyNotFound;
        } else {
            // Anything not answered from this session's own buffer comes from
            // replicated state, so leadership is confirmed first: a deposed
            // leader would otherwise serve a value a newer leader has moved
            // past. The buffer cases above are this session's own writes.
            const KeyStoreStatus gate = read_index
                ? to_status(read_index->wait_until_readable())
                : KeyStoreStatus::Success;
            result = gate == KeyStoreStatus::Success
                ? key_store.get(transaction, *command.key)
                : KeyStoreGetResult{.status = gate};
        }

        if (implicit) {
            const bool success = result.status == KeyStoreStatus::Success ||
                result.status == KeyStoreStatus::KeyNotFound;
            finish_implicit_transaction(
                transaction, transaction_manager, success,
                result.status == KeyStoreStatus::Deadlock
                    ? AbortReason::DeadlockVictim
                    : AbortReason::StatementFailure);
            context.active_transaction.reset();
            context.write_buffer.clear();
        }
        send_get_response(socket_fd, result);
        return;
    }

    // PUT and DELETE: lock, buffer, and - if there is no explicit transaction -
    // propose immediately.
    const KeyStoreStatus lock_status =
        lock_for_write(transaction_manager, transaction, *command.key);
    if (lock_status != KeyStoreStatus::Success) {
        if (implicit) {
            transaction_manager.abort(
                transaction,
                lock_status == KeyStoreStatus::Deadlock
                    ? AbortReason::DeadlockVictim
                    : AbortReason::StatementFailure);
            context.active_transaction.reset();
        } else if (lock_status == KeyStoreStatus::Deadlock) {
            transaction_manager.abort(transaction, AbortReason::DeadlockVictim);
            context.active_transaction.reset();
            context.write_buffer.clear();
        }
        send_operation_response(socket_fd, lock_status);
        return;
    }

    if (command.op == Operator::PUT) {
        context.write_buffer.put(*command.key, *command.value);
    } else {
        context.write_buffer.remove(*command.key);
    }

    if (!implicit) {
        // Buffered; it reaches the cluster at COMMIT.
        send_operation_response(socket_fd, KeyStoreStatus::Success);
        return;
    }

    send_operation_response(
        socket_fd,
        propose_and_settle(context, transaction_manager, proposer));
}

// --- Non-replicated path (unchanged) --------------------------------------

void execute_transactional_command(
    int socket_fd,
    KeyStore &key_store,
    TransactionManager &transaction_manager,
    SessionContext &context,
    const Command &command
) {
    if (command.op == Operator::BEGIN_TXN) {
        if (context.active_transaction) {
            send_operation_response(socket_fd, KeyStoreStatus::TransactionAlreadyActive);
            return;
        }
        context.active_transaction = transaction_manager.begin();
        send_operation_response(socket_fd, KeyStoreStatus::Success);
        return;
    }

    if (command.op == Operator::COMMIT) {
        if (!context.active_transaction) {
            send_operation_response(socket_fd, KeyStoreStatus::NoActiveTransaction);
            return;
        }
        const CommitStatus status = transaction_manager.commit(context.active_transaction);
        if (status == CommitStatus::Success) context.active_transaction.reset();
        send_operation_response(
            socket_fd,
            status == CommitStatus::Success
                ? KeyStoreStatus::Success
                : KeyStoreStatus::CommitFailed);
        return;
    }

    if (command.op == Operator::ROLLBACK) {
        if (!context.active_transaction) {
            send_operation_response(socket_fd, KeyStoreStatus::NoActiveTransaction);
            return;
        }
        const AbortStatus status = transaction_manager.abort(
            context.active_transaction,
            AbortReason::ClientRequest);
        if (status == AbortStatus::Success) context.active_transaction.reset();
        send_operation_response(
            socket_fd,
            status == AbortStatus::Success
                ? KeyStoreStatus::Success
                : KeyStoreStatus::RollbackFailed);
        return;
    }

    bool implicit = false;
    const TransactionHandle transaction = command_transaction(
        context,
        transaction_manager,
        implicit);

    if (command.op == Operator::GET) {
        KeyStoreGetResult result = key_store.get(transaction, *command.key);
        const bool success = result.status == KeyStoreStatus::Success ||
            result.status == KeyStoreStatus::KeyNotFound;
        if (implicit) finish_implicit_transaction(
            transaction,
            transaction_manager,
            success,
            result.status == KeyStoreStatus::Deadlock
                ? AbortReason::DeadlockVictim
                : AbortReason::StatementFailure);
        if (!implicit && result.status == KeyStoreStatus::Deadlock) {
            transaction_manager.abort(transaction, AbortReason::DeadlockVictim);
            context.active_transaction.reset();
        }
        // TODO this is an issue i believe if we end up implementing replication
        // We should retry inside the engine to avoid replicating the whole transaction
        // entry again which can cause redundancy.
        send_get_response(socket_fd, result);
        return;
    }

    if (command.op == Operator::PUT) {
        KeyStoreStatus result = key_store.put(
            transaction,
            *command.key,
            *command.value);
        if (implicit) {
            finish_implicit_transaction(
                transaction,
                transaction_manager,
                result == KeyStoreStatus::Success,
                result == KeyStoreStatus::Deadlock
                    ? AbortReason::DeadlockVictim
                    : AbortReason::StatementFailure);
        } else if (result == KeyStoreStatus::Deadlock) {
            transaction_manager.abort(transaction, AbortReason::DeadlockVictim);
            context.active_transaction.reset();
        }
        send_operation_response(socket_fd, result);
        return;
    }

    KeyStoreRemoveResult result = key_store.remove(transaction, *command.key);
    const bool success = result.status == KeyStoreStatus::Success ||
        result.status == KeyStoreStatus::KeyNotFound;
    if (implicit) {
        finish_implicit_transaction(
            transaction,
            transaction_manager,
            success,
            result.status == KeyStoreStatus::Deadlock
                ? AbortReason::DeadlockVictim
                : AbortReason::StatementFailure);
    } else if (result.status == KeyStoreStatus::Deadlock) {
        transaction_manager.abort(transaction, AbortReason::DeadlockVictim);
        context.active_transaction.reset();
    }
    send_operation_response(socket_fd, result.status);
}

} // namespace

void serve_connection(
    int socket_fd,
    KeyStore &key_store,
    TransactionManager &transaction_manager,
    RaftProposer *proposer,
    RaftReadIndex *read_index
) noexcept {
    SessionContext context{};

    try {
        while (true) {
            const Command command = read_command(socket_fd);
            if (proposer) {
                execute_replicated_command(
                    socket_fd,
                    key_store,
                    transaction_manager,
                    context,
                    *proposer,
                    read_index,
                    command);
            } else {
                execute_transactional_command(
                    socket_fd,
                    key_store,
                    transaction_manager,
                    context,
                    command);
            }
        }
    } catch (...) {
        if (context.active_transaction) {
            try {
                transaction_manager.abort(
                    context.active_transaction,
                    AbortReason::ClientRequest);
            } catch (...) {
            }
        }
        ::close(socket_fd);
    }
}

} // namespace CommandServer
