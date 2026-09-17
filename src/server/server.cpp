#include <KeyStore.h>
#include <LockManager/LockManager.h>
#include <Log/Log.h>
#include <Log/WalPayloadCodec.h>
#include <Log/WalRecords.h>
#include <TransactionManager/TransactionManager.h>
#include <Raft/ClusterConfig.h>
#include <Raft/RaftApplier.h>
#include <Raft/RaftHardStateStore.h>
#include <Raft/RaftElection.h>
#include <Raft/RaftLog.h>
#include <Raft/RaftPeerClients.h>
#include <Raft/RaftProposer.h>
#include <Raft/RaftReplicator.h>
#include <Raft/RaftServiceImpl.h>
#include <Raft/RaftState.h>
#include <Recovery.h>
#include <server/CommandServer.h>
#include <DiskIO.h>
#include <queue>
#include <utility>

#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <poll.h>
#include <span>
#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <cerrno>
#include <csignal>
#include <cstdint>
#include <functional>
#include <iostream>
#include <optional>
#include <string>
#include <thread>
#include <filesystem>
#include <Log/WalRecordCodec.h>
#include <unordered_map>
#include <unordered_set>
#include <vector>
#include <mutex>

namespace {

enum class StartupStatus : std::uint8_t {
    SUCCESS = 0,
    FAILED,
};

Config setup_config(const std::string& database_directory) {
    // Log::open() requires initial_lsn to equal the smallest existing
    // segment's base LSN when segments already exist. A fresh database (no
    // WAL directory yet, or an empty one) has no existing segment to match,
    // so it starts a brand new log at LSN 1.
    const std::string wal_directory = database_directory + ".wal";
    std::optional<std::uint64_t> smallest_base;

    if (std::filesystem::exists(wal_directory)) {
        for (const auto& entry : std::filesystem::directory_iterator(wal_directory)) {
            const auto name = entry.path().filename().string();
            const auto [base, is_store] = parse_wal_segment_name(name);
            // Every segment has exactly one .store file, so counting only
            // those naturally avoids double-counting its paired .index file.
            if (!is_store) continue;
            if (!smallest_base || base < *smallest_base) smallest_base = base;
        }
    }

    return Config{
        .max_index_bytes = 1024 * Index::ENTRY_SIZE,
        .max_store_bytes = 64 * 1024 * 1024,
        .initial_lsn = smallest_base.value_or(1),
    };
}


// KeyStore, Log, and TransactionManager all have deleted copy/move
// constructors, so they can't be built here and handed back by value - the
// caller (main) constructs them in the scope that needs to outlive this
// call (the connection-accept loop), and this function only configures and
// opens them.
StartupStatus setup_database(
    const std::string &db_file,
    KeyStore &key_store,
    Log &log,
    TransactionManager &transaction_manager,
    std::uint64_t &last_applied_raft_index
) {
    key_store.attach_transaction_manager(transaction_manager);

    try {
        log.open(db_file + ".wal");
    } catch (const std::exception &error) {
        std::cerr << "[ERROR] Failed to open WAL: " << error.what() << std::endl;
        return StartupStatus::FAILED;
    }

    try {
        // Redo runs against the raw database file, before KeyStore/BTree/Pager
        // are ever opened - opening them first would let Pager cache header
        // state that redo's direct writes would then leave stale underneath it.
        std::unordered_map<TransactionId, Lsn> unresolved_transactions;
        aries_recovery_redo(log, db_file, unresolved_transactions, last_applied_raft_index);

        if (key_store.open(db_file) != KeyStoreStatus::Success) {
            std::cerr << "[ERROR] Failed to open database: " << db_file << std::endl;
            return StartupStatus::FAILED;
        }

        // Undo needs the live KeyStore/BTree to actually navigate and mutate
        // the tree.
        aries_recovery_undo(log, key_store, unresolved_transactions);
        finish_recovery(transaction_manager, db_file, last_applied_raft_index);
    } catch (const std::exception &error) {
        std::cerr << "[ERROR] Recovery failed: " << error.what() << std::endl;
        return StartupStatus::FAILED;
    }

    return StartupStatus::SUCCESS;
}

// Shutdown is delivered to the accept loop through a self-pipe rather than by
// interrupting accept(). A signal handler cannot safely do much, and neither
// available shortcut works here: shutdown() on a LISTENING socket returns
// ENOTCONN on macOS without waking accept(), and a handler installed through
// std::signal leaves the syscall to be restarted. So the loop polls the
// listener and the pipe together, and the handler just writes one byte.
std::atomic<bool> shutdown_requested{false};
std::atomic<int> shutdown_pipe_write{-1};

void request_shutdown(int) {
    shutdown_requested.store(true);
    const int pipe_fd = shutdown_pipe_write.load();
    if (pipe_fd >= 0) {
        const char byte = 1;
        (void)::write(pipe_fd, &byte, 1);   // write() is async-signal-safe
    }
}

// Session threads must be joinable, or shutdown cannot wait for them to roll
// back their transactions before storage closes.
struct SessionRegistry {
    std::mutex mutex;
    std::vector<std::thread> threads;
    std::unordered_set<int> open_sockets;
};

int create_listener(std::uint16_t port) {
    // Keep socket setup in one place so every startup failure closes the
    // partially initialized descriptor before returning to main.
    const int listener_fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (listener_fd < 0) return -1;

    const int reuse_address = 1;
    if (::setsockopt(listener_fd, SOL_SOCKET, SO_REUSEADDR, &reuse_address, sizeof(reuse_address)) != 0) {
        ::close(listener_fd);
        return -1;
    }

    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_port = htons(port);
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);

    if (::bind(listener_fd, reinterpret_cast<const sockaddr *>(&address), sizeof(address)) != 0) {
        ::close(listener_fd);
        return -1;
    }

    if (::listen(listener_fd, SOMAXCONN) != 0) {
        ::close(listener_fd);
        return -1;
    }

    return listener_fd;
}

} // namespace

int main(int argc, char *argv[]) {
    std::string db_file;
    std::string config_path;
    std::string self_text;
    for (int arg = 1; arg < argc; ++arg) {
        const std::string current = argv[arg];
        if (current == "--config" && arg + 1 < argc) {
            config_path = argv[++arg];
        } else if (current == "--self" && arg + 1 < argc) {
            self_text = argv[++arg];
        } else if (db_file.empty() && !current.starts_with("--")) {
            db_file = current;
        } else {
            db_file.clear();
            break;
        }
    }

    if (db_file.empty() || config_path.empty() || self_text.empty()) {
        std::cerr << "Usage: " << argv[0]
                  << " <db-file> --config <cluster.conf> --self <host:port>" << std::endl;
        return 1;
    }

    // --self is this node's DATABASE SERVER address; the cluster config maps it
    // to the raft address that is this node's identity.
    std::vector<ClusterMember> cluster;
    NodeAddress self_client_address;
    try {
        cluster = parse_cluster_config(config_path);
        self_client_address = NodeAddress::from_string(self_text);
    } catch (const std::exception &error) {
        std::cerr << "[ERROR] " << error.what() << std::endl;
        return 1;
    }

    // RaftState enforces this too, but catching it here names the address and
    // the column, which is what an operator needs to fix the mistake.
    const bool self_in_cluster = std::any_of(
        cluster.begin(), cluster.end(), [&](const ClusterMember &member) {
            return member.database_server == self_client_address;
        });
    if (!self_in_cluster) {
        std::cerr << "[ERROR] --self " << self_text
                  << " matches no database_server address in " << config_path << std::endl;
        return 1;
    }

    KeyStore key_store;
    Config config = setup_config(db_file);
    Log log(config);
    LockManager lock_manager;
    TransactionManager transaction_manager(log, lock_manager, key_store);

    // 1. ARIES recovery: the database reaches a transactionally consistent
    //    state, and reports how far the Raft log had been applied.
    std::uint64_t last_applied_raft_index = 0;
    if (setup_database(db_file, key_store, log, transaction_manager,
                       last_applied_raft_index) == StartupStatus::FAILED) {
        return 1;
    }

    // 2. Open the Raft log and the persisted term/vote. Both live in <db>.raft;
    //    RaftLog ignores files that are not segments.
    const Config raft_config{
        .max_index_bytes = 1000 * Index::ENTRY_SIZE,
        .max_store_bytes = 16 * 1024 * 1024,
        .initial_lsn = 1,
    };
    RaftLog raft_log(raft_config);
    RaftHardStateStore hard_state;
    std::unique_ptr<RaftState> raft_state;
    try {
        raft_log.open(db_file + ".raft");
        hard_state.open(db_file + ".raft");

        // 3. Raft state, seeded from recovery rather than from zero. Starts as
        //    a Follower.
        raft_state = std::make_unique<RaftState>(
            cluster, self_client_address, hard_state, last_applied_raft_index);
    } catch (const std::exception &error) {
        std::cerr << "[ERROR] Failed to start Raft: " << error.what() << std::endl;
        key_store.close();
        return 1;
    }

    // 4. The apply loop, on every server regardless of role.
    RaftApplier applier(*raft_state, raft_log, key_store, transaction_manager);
    std::thread apply_thread([&applier] {
        try {
            applier.run();
        } catch (const std::exception &error) {
            // A committed entry must be applied on every node, so a node that
            // cannot apply one must not keep running as if it had.
            std::cerr << "[FATAL] Apply loop failed: " << error.what() << std::endl;
            std::abort();
        }
    });

    // 5. The Raft RPC plane, before anything that could campaign: a node that
    //    campaigns before it can answer its peers cannot win.
    NodeAddress self_raft_address;
    std::vector<NodeAddress> peers;
    {
        std::lock_guard lock(raft_state->state_mutex);
        self_raft_address = raft_state->self_raft_address();
        peers = raft_state->peers();
    }

    RaftServiceImpl raft_service(*raft_state, raft_log);
    std::unique_ptr<grpc::Server> raft_server =
        RaftRpc::start_server(self_raft_address.port, raft_service);
    if (!raft_server) {
        std::cerr << "[ERROR] Failed to listen for Raft RPCs on port "
                  << self_raft_address.port << std::endl;
        {
            std::lock_guard lock(raft_state->state_mutex);
            raft_state->shutting_down = true;
        }
        raft_state->apply_cv.notify_all();
        apply_thread.join();
        raft_log.close();
        key_store.close();
        return 1;
    }

    // The outbound half. Channels connect lazily, so every peer being down right
    // now - the normal case at cluster boot - cannot fail startup.
    RaftPeerClients peer_clients(peers);
    std::cout << "Serving Raft RPCs on 0.0.0.0:" << self_raft_address.port
              << " as " << self_raft_address.to_string() << std::endl;

    // 6. The replication threads, one per peer, on every server. They park
    //    immediately - we start as a follower - and the election thread wakes
    //    them the moment this node wins. Created once and parked rather than
    //    spawned on promotion: joining them in become_follower() would block a
    //    step-down on an in-flight RPC to an unreachable peer.
    std::vector<std::unique_ptr<RaftReplicator>> replicators;
    std::vector<std::thread> replication_threads;
    replicators.reserve(peers.size());
    replication_threads.reserve(peers.size());
    for (const NodeAddress &peer : peers) {
        replicators.push_back(std::make_unique<RaftReplicator>(
            *raft_state, raft_log, peer_clients, peer));
        RaftReplicator &replicator = *replicators.back();
        replication_threads.emplace_back([&replicator, peer] {
            try {
                replicator.run();
            } catch (const std::exception &error) {
                // An unreachable peer is absorbed inside the loop; reaching here
                // means something structural failed. The node can still lead for
                // its other peers and can still follow, so this is not fatal.
                std::cerr << "[ERROR] Replication to " << peer.to_string()
                          << " stopped: " << error.what() << std::endl;
            }
        });
    }

    // The session side of replication: turns a committed transaction's buffered
    // writes into a Raft entry and waits for it to apply.
    RaftProposer proposer(*raft_state, raft_log);

    // 7. The election timer. LAST of the Raft threads on purpose: this is the
    //    first moment the node can campaign, so nothing a campaign depends on -
    //    the RPC server, the peer stubs - may start after it.
    RaftElection election(*raft_state, raft_log, peer_clients);
    std::thread election_thread([&election] {
        try {
            election.run();
        } catch (const std::exception &error) {
            // Unlike the apply loop, a failed election is not fatal: the node
            // stays a follower and another server leads. Log it and let the
            // thread end rather than halting a process that can still serve.
            std::cerr << "[ERROR] Election thread stopped: " << error.what() << std::endl;
        }
    });

    // 8. The client acceptor is last: clients must not connect before the node
    //    can serve them.
    const std::uint16_t client_port = self_client_address.port;
    const int listener_fd = create_listener(client_port);
    if (listener_fd < 0) {
        std::cerr << "[ERROR] Failed to listen on 127.0.0.1:" << client_port << std::endl;
        {
            std::lock_guard lock(raft_state->state_mutex);
            raft_state->shutting_down = true;
        }
        raft_state->apply_cv.notify_all();
        raft_state->election_cv.notify_all();
        raft_state->replication_cv.notify_all();
        // Same order as the normal path: the outbound callers stop first, then
        // our own server, and only then the log they were writing to.
        for (std::thread &replication : replication_threads) {
            if (replication.joinable()) replication.join();
        }
        election_thread.join();
        raft_server->Shutdown();
        raft_server->Wait();
        apply_thread.join();
        raft_log.close();
        key_store.close();
        return 1;
    }
    int shutdown_pipe[2] = {-1, -1};
    if (::pipe(shutdown_pipe) != 0) {
        std::cerr << "[ERROR] Failed to create the shutdown pipe" << std::endl;
        ::close(listener_fd);
        {
            std::lock_guard lock(raft_state->state_mutex);
            raft_state->shutting_down = true;
        }
        raft_state->apply_cv.notify_all();
        raft_state->election_cv.notify_all();
        raft_state->replication_cv.notify_all();
        // Same order as the normal path: the outbound callers stop first, then
        // our own server, and only then the log they were writing to.
        for (std::thread &replication : replication_threads) {
            if (replication.joinable()) replication.join();
        }
        election_thread.join();
        raft_server->Shutdown();
        raft_server->Wait();
        apply_thread.join();
        raft_log.close();
        key_store.close();
        return 1;
    }
    ::fcntl(shutdown_pipe[0], F_SETFL, O_NONBLOCK);
    ::fcntl(shutdown_pipe[1], F_SETFL, O_NONBLOCK);
    shutdown_pipe_write.store(shutdown_pipe[1]);

    std::signal(SIGINT, request_shutdown);
    std::signal(SIGTERM, request_shutdown);
    std::signal(SIGPIPE, SIG_IGN);

    {
        std::lock_guard lock(raft_state->state_mutex);
        std::cout << "Raft address " << raft_state->self_raft_address().to_string()
                  << ", last applied " << raft_state->last_applied() << std::endl;
    }
    std::cout << "Listening on 127.0.0.1:" << client_port << std::endl;

    SessionRegistry sessions;
    while (!shutdown_requested.load()) {
        // Wait for either a client or the shutdown byte, so a signal that
        // arrives between the check above and the wait is never missed.
        pollfd waiting[2]{};
        waiting[0] = {.fd = listener_fd, .events = POLLIN, .revents = 0};
        waiting[1] = {.fd = shutdown_pipe[0], .events = POLLIN, .revents = 0};
        if (::poll(waiting, 2, -1) < 0) {
            if (errno == EINTR) continue;
            std::cerr << "[ERROR] Failed to wait for connections" << std::endl;
            break;
        }
        if (waiting[1].revents != 0) break;            // shutdown requested
        if ((waiting[0].revents & POLLIN) == 0) continue;

        sockaddr_in client_address{};
        socklen_t client_address_size = sizeof(client_address);
        const int socket_fd = ::accept(
            listener_fd, reinterpret_cast<sockaddr *>(&client_address), &client_address_size);
        if (socket_fd < 0) {
            if (shutdown_requested.load()) break;
            if (errno == EINTR) continue;
            std::cerr << "[ERROR] Failed to accept client connection" << std::endl;
            continue;
        }

        try {
            std::lock_guard lock(sessions.mutex);
            sessions.open_sockets.insert(socket_fd);
            sessions.threads.emplace_back([&sessions, socket_fd, &key_store, &transaction_manager, &proposer] {
                CommandServer::serve_connection(socket_fd, key_store, transaction_manager, &proposer);
                std::lock_guard lock(sessions.mutex);
                sessions.open_sockets.erase(socket_fd);
            });
        } catch (...) {
            ::close(socket_fd);
        }
    }

    // --- Shutdown, in the order the design requires ---------------------------
    // 1. Stop accepting. The signal handler already shut the listener down.
    ::close(listener_fd);
    shutdown_pipe_write.store(-1);
    ::close(shutdown_pipe[0]);
    ::close(shutdown_pipe[1]);
    std::cout << "Shutting down" << std::endl;

    // 2. Wake every parked thread. A thread waiting on a condition variable
    //    cannot be stopped any other way.
    {
        std::lock_guard lock(raft_state->state_mutex);
        raft_state->shutting_down = true;
    }
    raft_state->election_cv.notify_all();
    raft_state->replication_cv.notify_all();
    raft_state->apply_cv.notify_all();
    raft_state->applied_cv.notify_all();

    // 3. Sessions: wake any thread blocked reading its socket, then join. The
    //    acceptor has stopped, so no descriptor is being handed out any more.
    {
        std::lock_guard lock(sessions.mutex);
        for (const int socket_fd : sessions.open_sockets) ::shutdown(socket_fd, SHUT_RDWR);
    }
    for (std::thread &session : sessions.threads) {
        if (session.joinable()) session.join();
    }

    // 4. Join the replication threads. They are parked or between RPCs, so this
    //    is bounded by the RPC deadline.
    for (std::thread &replication : replication_threads) {
        if (replication.joinable()) replication.join();
    }

    // 5. Join the election thread. Both outbound callers stop BEFORE the RPC
    //    server does: shutting our own server first would leave them talking to
    //    peers that can no longer answer.
    election_thread.join();

    // 6. Stop serving Raft RPCs. Shutdown() returns only once every in-flight
    //    handler has returned, which is what makes closing RaftLog below safe -
    //    the AppendEntries handler appends to it and truncates it.
    raft_server->Shutdown();
    raft_server->Wait();

    // 7. The apply loop finishes its current batch before returning.
    apply_thread.join();

    // 8. Storage last, once nothing is running against it.
    raft_log.close();
    if (key_store.close() != KeyStoreStatus::Success) {
        std::cerr << "[ERROR] Failed to close the database cleanly" << std::endl;
        return 1;
    }

    return 0;
}
