#include <gtest/gtest.h>

#include <Command.h>
#include <KeyCodec.h>
#include <KeyStore.h>
#include <LockManager/LockManager.h>
#include <Log/Log.h>
#include <Session.h>
#include <ValueCodec.h>
#include <server/CommandServer.h>

#include <arpa/inet.h>
#include <sys/socket.h>
#include <unistd.h>

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <stdexcept>
#include <string>
#include <system_error>
#include <thread>
#include <vector>

namespace {

class Connection {
    public:
        Connection(
            KeyStore &key_store,
            TransactionManager &transaction_manager
        ) {
            int sockets[2] = {-1, -1};
            if (::socketpair(AF_UNIX, SOCK_STREAM, 0, sockets) != 0) {
                throw std::runtime_error("Failed to create test socket pair");
            }

            session = std::make_unique<Session>(sockets[0], 0);
            dispatcher = std::thread(
                CommandServer::serve_connection,
                sockets[1],
                std::ref(key_store),
                std::ref(transaction_manager),
                // No proposer and no read index: this fixture has no cluster,
                // so writes go straight through KeyStore and reads are served
                // from local state, exactly as they always have.
                nullptr,
                nullptr);
        }

        ~Connection() {
            close();
        }

        Session &client() {
            return *session;
        }

        void close() {
            if (session) {
                session->close();
                session.reset();
            }

            if (dispatcher.joinable()) dispatcher.join();
        }

    private:
        std::unique_ptr<Session> session;
        std::thread dispatcher;
};

class RawConnection {
    public:
        RawConnection(
            KeyStore &key_store,
            TransactionManager &transaction_manager
        ) {
            int sockets[2] = {-1, -1};
            if (::socketpair(AF_UNIX, SOCK_STREAM, 0, sockets) != 0) {
                throw std::runtime_error("Failed to create test socket pair");
            }

            client_fd = sockets[0];
            dispatcher = std::thread(
                CommandServer::serve_connection,
                sockets[1],
                std::ref(key_store),
                std::ref(transaction_manager),
                // No proposer and no read index: this fixture has no cluster,
                // so writes go straight through KeyStore and reads are served
                // from local state, exactly as they always have.
                nullptr,
                nullptr);
        }

        ~RawConnection() {
            close();
        }

        int fd() const {
            return client_fd;
        }

        void close() {
            if (client_fd >= 0) {
                ::close(client_fd);
                client_fd = -1;
            }

            if (dispatcher.joinable()) dispatcher.join();
        }

    private:
        int client_fd = -1;
        std::thread dispatcher;
};

void send_bytes(int socket_fd, const std::uint8_t *buffer, std::size_t size) {
    std::size_t bytes_sent = 0;

    while (bytes_sent < size) {
        const ssize_t result = ::send(socket_fd, buffer + bytes_sent, size - bytes_sent, MSG_NOSIGNAL);
        ASSERT_GT(result, 0);
        if (result <= 0) return;
        bytes_sent += static_cast<std::size_t>(result);
    }
}

class CommandServerIntegrationTest : public ::testing::Test {
    protected:
        void SetUp() override {
            const std::string suffix = std::to_string(std::chrono::steady_clock::now().time_since_epoch().count());
            temp_dir = std::filesystem::temp_directory_path() / ("stoneleafdb_command_server_test_" + suffix);
            db_path = temp_dir / "test.db";
            std::filesystem::create_directories(temp_dir);
            log = std::make_unique<Log>(Config{
                .max_index_bytes = 100 * Index::ENTRY_SIZE,
                .max_store_bytes = 1024 * 1024,
                .initial_lsn = 1,
            });
            log->open((temp_dir / "wal").string());
            transaction_manager = std::make_unique<TransactionManager>(
                *log,
                lock_manager,
                key_store);
            key_store.attach_transaction_manager(*transaction_manager);
            ASSERT_EQ(key_store.open(db_path.string()), KeyStoreStatus::Success);
        }

        void TearDown() override {
            key_store.close();
            transaction_manager.reset();
            log.reset();

            std::error_code error;
            std::filesystem::remove_all(temp_dir, error);
        }

        KeyStore key_store;
        LockManager lock_manager;
        std::unique_ptr<Log> log;
        std::unique_ptr<TransactionManager> transaction_manager;
        std::filesystem::path temp_dir;
        std::filesystem::path db_path;
};

TEST_F(CommandServerIntegrationTest, ExecutesPutGetAndRemoveCommands) {
    Connection connection(key_store, *transaction_manager);
    Session &session = connection.client();
    const KeyInput key = std::string{"name"};
    const ValueInput value = std::string{"stoneleaf"};

    session.put(key, value);
    const std::optional<ValueInput> result = session.get(key);
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(*result, value);

    session.remove(key);
    EXPECT_FALSE(session.get(key).has_value());
    EXPECT_THROW(session.remove(key), std::runtime_error);

    session.put(key, ValueInput{std::string{"still connected"}});
    EXPECT_TRUE(session.get(key).has_value());
}

TEST_F(CommandServerIntegrationTest, UsesConnectionLocalTransactionContext) {
    Connection connection(key_store, *transaction_manager);
    Session &session = connection.client();
    const KeyInput key = std::string{"session-key"};

    // A standalone command receives an implicit transaction.
    session.put(key, ValueInput{std::string{"committed"}});
    EXPECT_EQ(
        session.get(key),
        std::optional<ValueInput>{ValueInput{std::string{"committed"}}});

    // An explicit transaction is retained only in this connection context.
    session.begin_transaction();
    session.put(key, ValueInput{std::string{"temporary"}});
    session.rollback();
    EXPECT_EQ(
        session.get(key),
        std::optional<ValueInput>{ValueInput{std::string{"committed"}}});
}

TEST_F(CommandServerIntegrationTest, CommitsAndRollsBackExplicitTransactions) {
    Connection connection(key_store, *transaction_manager);
    Session &session = connection.client();
    const KeyInput key = std::uint64_t{42};

    session.begin_transaction();
    session.put(key, ValueInput{std::string{"committed"}});
    session.commit();

    session.begin_transaction();
    session.put(key, ValueInput{std::string{"rolled back"}});
    session.rollback();

    const std::optional<ValueInput> result = session.get(key);
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(*result, ValueInput{std::string{"committed"}});
}

TEST_F(CommandServerIntegrationTest, RollsBackActiveTransactionWhenClientDisconnects) {
    const KeyInput key = std::string{"temporary"};
    {
        Connection connection(key_store, *transaction_manager);
        connection.client().begin_transaction();
        connection.client().put(key, ValueInput{std::string{"uncommitted"}});
        EXPECT_THROW(connection.client().begin_transaction(), std::runtime_error);
        connection.close();
    }

    Connection verification_connection(key_store, *transaction_manager);
    EXPECT_FALSE(verification_connection.client().get(key).has_value());
}

TEST_F(CommandServerIntegrationTest, ReadsACommandDeliveredOneByteAtATime) {
    RawConnection connection(key_store, *transaction_manager);
    const Command command{
        .operator_type = OperatorType::BINARY,
        .op = Operator::PUT,
        .key = KeyCodec::make_string("fragmented"),
        .value = ValueCodec::make_char("value")
    };
    const std::vector<std::uint8_t> packet = CommandCodec::serialize(command);

    for (const std::uint8_t byte : packet) send_bytes(connection.fd(), &byte, sizeof(byte));

    std::uint8_t response = 0;
    ASSERT_EQ(::recv(connection.fd(), &response, sizeof(response), 0), 1);
    EXPECT_EQ(response, 1);
}

TEST_F(CommandServerIntegrationTest, ClosesConnectionForMalformedOrOversizedCommand) {
    {
        RawConnection connection(key_store, *transaction_manager);
        const std::vector<std::uint8_t> malformed = {0, 0, 0, 2, 0, 99};
        send_bytes(connection.fd(), malformed.data(), malformed.size());

        std::uint8_t response = 0;
        EXPECT_EQ(::recv(connection.fd(), &response, sizeof(response), 0), 0);
    }

    {
        RawConnection connection(key_store, *transaction_manager);
        const std::uint32_t network_payload_size = htonl(16 * 1024 * 1024 + 1);
        send_bytes(connection.fd(), reinterpret_cast<const std::uint8_t *>(&network_payload_size), sizeof(network_payload_size));

        std::uint8_t response = 0;
        EXPECT_EQ(::recv(connection.fd(), &response, sizeof(response), 0), 0);
    }
}

} // namespace
