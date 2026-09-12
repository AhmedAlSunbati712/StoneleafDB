#include <KeyStore.h>
#include <LockManager/LockManager.h>
#include <Log/Log.h>
#include <Log/WalPayloadCodec.h>
#include <Log/WalRecords.h>
#include <TransactionManager/TransactionManager.h>
#include <Recovery.h>
#include <server/CommandServer.h>
#include <DiskIO.h>
#include <queue>
#include <utility>

#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <span>
#include <sys/socket.h>
#include <unistd.h>

#include <cerrno>
#include <cstdint>
#include <functional>
#include <iostream>
#include <optional>
#include <string>
#include <thread>
#include <filesystem>
#include <Log/WalRecordCodec.h>
#include <unordered_map>

namespace {

constexpr std::uint16_t SERVER_PORT = 8080;
enum class StartupStatus : std::uint8_t {
    SUCCESS = 0,
    FAILED,
};

// Returns {base_lsn, is_store}. Mirrors Log.cpp's own parse_segment_name:
// a name starting with "segment-" must have the exact "<20 digits>.store" or
// "<20 digits>.index" shape, or it's treated as corruption, not ignored.
std::pair<std::uint64_t, bool> parse_segment_offset(const std::string& name) {
    constexpr std::size_t prefix_size = 8;
    constexpr std::size_t digits_size = 20;
    if (!name.starts_with("segment-")) return {0, false};
    const bool store = name.size() == prefix_size + digits_size + 6 && name.ends_with(".store");
    const bool index = name.size() == prefix_size + digits_size + 6 && name.ends_with(".index");
    if (!store && !index) throw std::runtime_error("Malformed WAL segment filename");
    const auto digits = name.substr(prefix_size, digits_size);
    if (digits.find_first_not_of("0123456789") != std::string::npos) throw std::runtime_error("Malformed WAL segment filename");
    try { return {std::stoull(digits), store}; }
    catch (...) { throw std::runtime_error("Malformed WAL segment filename"); }
}

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
            const auto [base, is_store] = parse_segment_offset(name);
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


// Deletes every WAL segment that isn't the currently active one. Safe only
// once redo has replayed the entire retained log onto the database file and
// flush() has made every page undo dirtied durable there too - at that
// point nothing outside the active segment is needed to recover again, even
// if the active segment rolled over partway through the undo pass.
void cleanup_finalized_segments(const std::string& db_file_name) {
    const std::string wal_directory = db_file_name + ".wal";
    if (!std::filesystem::exists(wal_directory)) return;

    // The active segment is the one with the largest base LSN - segments are
    // created with strictly increasing base LSNs, and Log always appends
    // into the most recently created one.
    std::optional<std::uint64_t> active_base;
    for (const auto& entry : std::filesystem::directory_iterator(wal_directory)) {
        const auto name = entry.path().filename().string();
        if (!name.starts_with("segment-")) continue;
        const auto [base, is_store] = parse_segment_offset(name);
        if (!is_store) continue;
        if (!active_base || base > *active_base) active_base = base;
    }
    // No segments at all means nothing to clean up.
    if (!active_base) return;

    // Delete both the .store and .index file for every non-active base LSN.
    for (const auto& entry : std::filesystem::directory_iterator(wal_directory)) {
        const auto name = entry.path().filename().string();
        if (!name.starts_with("segment-")) continue;
        const auto [base, is_store] = parse_segment_offset(name);
        (void)is_store;
        if (base == *active_base) continue;
        std::filesystem::remove(entry.path());
    }
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
    TransactionManager &transaction_manager
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
        aries_recovery_redo(log, db_file, unresolved_transactions);

        if (key_store.open(db_file) != KeyStoreStatus::Success) {
            std::cerr << "[ERROR] Failed to open database: " << db_file << std::endl;
            return StartupStatus::FAILED;
        }

        // Undo needs the live KeyStore/BTree to actually navigate and mutate
        // the tree.
        aries_recovery_undo(log, key_store, unresolved_transactions);
        cleanup_finalized_segments(db_file);
    } catch (const std::exception &error) {
        std::cerr << "[ERROR] Recovery failed: " << error.what() << std::endl;
        return StartupStatus::FAILED;
    }

    return StartupStatus::SUCCESS;
}

int create_listener() {
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
    address.sin_port = htons(SERVER_PORT);
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
    if (argc != 2) {
        std::cerr << "Usage: " << argv[0] << " <db-file>" << std::endl;
        return 1;
    }
    const std::string db_file = argv[1];

    KeyStore key_store;
    Config config = setup_config(db_file);
    Log log(config);
    LockManager lock_manager;
    TransactionManager transaction_manager(log, lock_manager, key_store);

    StartupStatus status = setup_database(db_file, key_store, log, transaction_manager);
    if (status == StartupStatus::FAILED) {
        return 1;
    }
    const int listener_fd = create_listener();
    if (listener_fd < 0) {
        std::cerr << "[ERROR] Failed to listen on 127.0.0.1:" << SERVER_PORT << std::endl;
        return 1;
    }

    std::cout << "Listening on 127.0.0.1:" << SERVER_PORT << std::endl;

    while (true) {
        sockaddr_in client_address{};
        socklen_t client_address_size = sizeof(client_address);
        const int socket_fd = ::accept(listener_fd, reinterpret_cast<sockaddr *>(&client_address), &client_address_size);
        if (socket_fd < 0) {
            if (errno == EINTR) continue;
            std::cerr << "[ERROR] Failed to accept client connection" << std::endl;
            continue;
        }

        // The dispatcher owns the accepted descriptor. Its executor threads
        // remain joined, while this connection-level thread runs independently.
        try {
            std::thread dispatcher(
                CommandServer::serve_connection,
                socket_fd,
                std::ref(key_store),
                std::ref(transaction_manager));
            dispatcher.detach();
        } catch (...) {
            ::close(socket_fd);
        }
    }
}
