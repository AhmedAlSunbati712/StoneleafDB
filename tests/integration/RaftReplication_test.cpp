#include <gtest/gtest.h>

#include <KeyCodec.h>
#include <KeyStore.h>
#include <LockManager/LockManager.h>
#include <Log/Log.h>
#include <Raft/RaftApplier.h>
#include <Raft/RaftHardStateStore.h>
#include <Raft/RaftLog.h>
#include <Raft/RaftPeerClients.h>
#include <Raft/RaftProposer.h>
#include <Raft/RaftReplicator.h>
#include <Raft/RaftServiceImpl.h>
#include <Raft/RaftState.h>
#include <Raft/TransactionWriteBuffer.h>
#include <Session.h>
#include <ValueCodec.h>
#include <server/CommandServer.h>
#include <storage/Index.h>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <chrono>
#include <cstdint>
#include <filesystem>
#include <future>
#include <memory>
#include <optional>
#include <string>
#include <system_error>
#include <thread>
#include <vector>

namespace {

Config raft_config() {
    return {.max_index_bytes = 1000 * Index::ENTRY_SIZE,
            .max_store_bytes = 16 * 1024 * 1024,
            .initial_lsn = 1};
}

Key key_for(std::uint64_t id) { return KeyCodec::encode(KeyInput{id}).value(); }
Value value_for(const std::string& text) { return ValueCodec::encode(ValueInput{text}).value(); }

MutationOp put_op(std::uint64_t id, const std::string& text) {
    return {.type = RaftMutationType::Put, .operation = PutMutation{key_for(id), value_for(text)}};
}

// See RaftTransport_test for why ports are reserved rather than taken from the
// running servers.
std::uint16_t reserve_port() {
    const int probe = ::socket(AF_INET, SOCK_STREAM, 0);
    if (probe < 0) return 0;

    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    address.sin_port = 0;

    std::uint16_t port = 0;
    socklen_t size = sizeof(address);
    if (::bind(probe, reinterpret_cast<sockaddr*>(&address), sizeof(address)) == 0 &&
        ::getsockname(probe, reinterpret_cast<sockaddr*>(&address), &size) == 0) {
        port = ntohs(address.sin_port);
    }
    ::close(probe);
    return port;
}

struct Node {
    std::filesystem::path directory;
    std::unique_ptr<RaftLog> log;
    std::unique_ptr<RaftHardStateStore> hard_state;
    std::unique_ptr<RaftState> state;
    std::unique_ptr<RaftServiceImpl> service;
    std::unique_ptr<grpc::Server> server;

    void stop() {
        if (server) {
            server->Shutdown();
            server->Wait();
            server.reset();
        }
        service.reset();
        state.reset();
        hard_state.reset();
        log.reset();
        if (!directory.empty()) {
            std::error_code error;
            std::filesystem::remove_all(directory, error);
        }
    }
};

// Log-level fixture: real servers and channels, but no state machines. Commit
// and replication progress are observable here without standing up a WAL and a
// KeyStore per node; the full stack is exercised by SingleNodeReplicationTest
// below and by the manual three-server run.
class ReplicationTest : public ::testing::Test {
protected:
    void TearDown() override {
        replicators.clear();
        clients.reset();
        for (auto& node : nodes) node->stop();
        nodes.clear();
    }

    void build(std::size_t size) {
        for (std::size_t i = 0; i < size; ++i) {
            raft_addresses.push_back(NodeAddress{"127.0.0.1", reserve_port()});
            client_addresses.push_back(NodeAddress{"127.0.0.1", reserve_port()});
            ASSERT_NE(raft_addresses[i].port, 0);
        }
        for (std::size_t i = 0; i < size; ++i) {
            cluster.push_back({.raft = raft_addresses[i],
                               .database_server = client_addresses[i]});
        }

        for (std::size_t i = 0; i < size; ++i) {
            auto node = std::make_unique<Node>();
            const std::string suffix =
                std::to_string(std::chrono::steady_clock::now().time_since_epoch().count());
            node->directory = std::filesystem::temp_directory_path() /
                              ("stoneleafdb_raft_replication_" + std::to_string(i) + "_" + suffix);
            std::filesystem::create_directories(node->directory);

            node->log = std::make_unique<RaftLog>(raft_config());
            node->log->open((node->directory / "raft").string());
            node->hard_state = std::make_unique<RaftHardStateStore>();
            node->hard_state->open((node->directory / "hardstate").string());
            node->state = std::make_unique<RaftState>(
                cluster, client_addresses[i], *node->hard_state, 0);
            node->service = std::make_unique<RaftServiceImpl>(*node->state, *node->log);
            node->server = RaftRpc::start_server(raft_addresses[i].port, *node->service);
            ASSERT_NE(node->server, nullptr);
            nodes.push_back(std::move(node));
        }

        std::vector<NodeAddress> peers(raft_addresses.begin() + 1, raft_addresses.end());
        clients = std::make_unique<RaftPeerClients>(peers);
        for (const NodeAddress& peer : peers) {
            replicators.push_back(std::make_unique<RaftReplicator>(
                *nodes[0]->state, *nodes[0]->log, *clients, peer));
        }
    }

    // Promotes node 0 without running an election, so the tests below are not
    // at the mercy of the timer.
    void make_leader(std::uint64_t term = 1) {
        std::lock_guard lock(nodes[0]->state->state_mutex);
        nodes[0]->state->advance_term(term, raft_addresses[0]);
        nodes[0]->state->become_leader(nodes[0]->log->last_index());
    }

    std::uint64_t commit_index_of(std::size_t i) {
        std::lock_guard lock(nodes[i]->state->state_mutex);
        return nodes[i]->state->commit_index();
    }

    // A fresh leader sets send_next optimistically to last_index + 1, so the
    // first AppendEntries to a follower with a shorter log carries no entries,
    // fails the consistency check, and backs send_next off by one. Converging
    // therefore costs one round trip per missing entry - the O(entries)
    // behaviour the paper's conflicting-term hint would replace.
    //
    // Production converges on its own without this helper: run() waits for the
    // heartbeat only while send_next > last_index, so a backing-off replicator
    // retries immediately rather than once per HEARTBEAT_INTERVAL.
    bool replicate_until_caught_up(std::size_t replicator_index) {
        const std::uint64_t bound = nodes[0]->log->last_index() + 2;
        for (std::uint64_t attempt = 0; attempt <= bound; ++attempt) {
            if (replicators[replicator_index]->replicate_once()) return true;
        }
        return false;
    }

    std::vector<NodeAddress> raft_addresses;
    std::vector<NodeAddress> client_addresses;
    std::vector<ClusterMember> cluster;
    std::vector<std::unique_ptr<Node>> nodes;
    std::unique_ptr<RaftPeerClients> clients;
    std::vector<std::unique_ptr<RaftReplicator>> replicators;
};

TEST_F(ReplicationTest, AHeartbeatKeepsAFollowerFromCampaigning) {
    build(3);
    make_leader();

    std::chrono::steady_clock::time_point before;
    {
        std::lock_guard lock(nodes[1]->state->state_mutex);
        before = nodes[1]->state->election_deadline();
    }
    // Let the follower's deadline lapse so a reset is unambiguous rather than a
    // coin flip between two draws from the same range.
    std::this_thread::sleep_for(RaftState::ELECTION_TIMEOUT_MAX + std::chrono::milliseconds(20));

    // An empty AppendEntries - there is nothing in the log to send.
    EXPECT_TRUE(replicators[0]->replicate_once());

    std::lock_guard lock(nodes[1]->state->state_mutex);
    EXPECT_GT(nodes[1]->state->election_deadline(), before);
    EXPECT_GT(nodes[1]->state->election_deadline(), std::chrono::steady_clock::now());
    EXPECT_EQ(nodes[1]->state->state(), State::Follower);
    EXPECT_EQ(nodes[1]->state->leader_raft_address(),
              std::optional<NodeAddress>{raft_addresses[0]});
}

TEST_F(ReplicationTest, EntriesReachAFollowersLog) {
    build(3);
    nodes[0]->log->append(1, {put_op(1, "one")});
    nodes[0]->log->append(1, {put_op(2, "two")});
    make_leader();

    EXPECT_TRUE(replicate_until_caught_up(0));

    EXPECT_EQ(nodes[1]->log->last_index(), 2u);
    EXPECT_EQ(nodes[1]->log->term_at(1), 1u);
    EXPECT_EQ(nodes[1]->log->read(2).operations.size(), 1u);
    // Untouched: only peer 0 was replicated to.
    EXPECT_EQ(nodes[2]->log->last_index(), 0u);
}

TEST_F(ReplicationTest, ReplicationContinuesAcrossASegmentRollover) {
    // raft_config() rolls a segment every 1000 entries. Batches are read and
    // appended one entry at a time across that boundary on both sides.
    build(3);
    make_leader();
    constexpr std::uint64_t entry_count = 1100;
    for (std::uint64_t i = 1; i <= entry_count; ++i) {
        nodes[0]->log->append(1, {put_op(i, "a")});
    }
    nodes[0]->log->sync_through(entry_count);

    std::uint64_t attempts = 0;
    while (nodes[1]->log->last_index() < entry_count && attempts < 200) {
        replicators[0]->replicate_once();
        ++attempts;
    }

    ASSERT_EQ(nodes[1]->log->last_index(), entry_count) << "after " << attempts << " attempts";
    EXPECT_EQ(nodes[1]->log->durable_index(), entry_count);
    EXPECT_EQ(nodes[1]->log->read(1000).idx, 1000u);
    EXPECT_EQ(nodes[1]->log->read(1001).idx, 1001u);
    EXPECT_EQ(commit_index_of(0), entry_count);
}

TEST_F(ReplicationTest, AHeartbeatRoundConfirmsTheLeadersReadRound) {
    build(3);
    make_leader();

    std::uint64_t round = 0;
    {
        std::lock_guard lock(nodes[0]->state->state_mutex);
        round = nodes[0]->state->start_read_round();
        EXPECT_EQ(nodes[0]->state->confirmed_read_round(), 0u);
    }

    // An empty AppendEntries is all a confirmation needs: leader + one peer is
    // a majority of three.
    EXPECT_TRUE(replicators[0]->replicate_once());

    std::lock_guard lock(nodes[0]->state->state_mutex);
    EXPECT_EQ(nodes[0]->state->acked_read_round(raft_addresses[1]), round);
    EXPECT_EQ(nodes[0]->state->confirmed_read_round(), round);
}

TEST_F(ReplicationTest, AFailedConsistencyCheckStillConfirmsTheRound) {
    // The follower rejected the entry but answered in our term, which is what
    // leadership confirmation asks. Refusing to count it would stall reads
    // whenever a follower is catching up.
    build(3);
    nodes[0]->log->append(1, {put_op(1, "a")});
    nodes[0]->log->append(1, {put_op(2, "b")});
    make_leader();   // send_next starts past the follower's log, so the first RPC fails

    std::uint64_t round = 0;
    {
        std::lock_guard lock(nodes[0]->state->state_mutex);
        round = nodes[0]->state->start_read_round();
    }
    EXPECT_FALSE(replicators[0]->replicate_once());

    std::lock_guard lock(nodes[0]->state->state_mutex);
    EXPECT_EQ(nodes[0]->state->confirmed_read_round(), round);
}

TEST_F(ReplicationTest, AnUnreachableMajorityLeavesTheRoundUnconfirmed) {
    build(3);
    make_leader();
    nodes[1]->stop();
    nodes[2]->stop();

    std::uint64_t round = 0;
    {
        std::lock_guard lock(nodes[0]->state->state_mutex);
        round = nodes[0]->state->start_read_round();
    }
    EXPECT_FALSE(replicators[0]->replicate_once());
    EXPECT_FALSE(replicators[1]->replicate_once());

    std::lock_guard lock(nodes[0]->state->state_mutex);
    EXPECT_EQ(nodes[0]->state->confirmed_read_round(), 0u)
        << "a partitioned leader must not confirm a read round";
}

TEST_F(ReplicationTest, ReplicationDoesNotWaitForAnInFlightAppend) {
    // A leader's append runs under append_mutex, not state_mutex. Heartbeats
    // and replication must keep going while one is in progress - on a shared
    // volume that write can stall for milliseconds.
    build(3);
    nodes[0]->log->append(1, {put_op(1, "a")});
    nodes[0]->log->sync_through(1);
    make_leader();

    std::unique_lock held(nodes[0]->state->append_mutex);
    auto round = std::async(std::launch::async, [&] { return replicate_until_caught_up(0); });
    ASSERT_EQ(round.wait_for(std::chrono::seconds(5)), std::future_status::ready);
    EXPECT_TRUE(round.get());
    EXPECT_EQ(nodes[1]->log->last_index(), 1u);
}

TEST_F(ReplicationTest, TheAppendEntriesReceiverWaitsForAnInFlightAppend) {
    // A proposal on the receiving node - a leader just deposed - must not
    // append between the receiver's consistency check and its truncation.
    // Leader first, so send_next starts at 1 and the RPC carries the entry.
    build(3);
    make_leader();
    nodes[0]->log->append(1, {put_op(1, "a")});
    nodes[0]->log->sync_through(1);

    {
        std::unique_lock held(nodes[1]->state->append_mutex);
        replicators[0]->replicate_once();   // times out against the blocked handler
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
        EXPECT_EQ(nodes[1]->log->last_index(), 0u);
    }

    EXPECT_TRUE(replicate_until_caught_up(0));
    EXPECT_EQ(nodes[1]->log->last_index(), 1u);
}

TEST_F(ReplicationTest, CommitIndexAdvancesOnceAMajorityHoldsTheEntry) {
    build(3);
    nodes[0]->log->append(1, {put_op(1, "one")});
    nodes[0]->log->append(1, {put_op(2, "two")});
    // The leader's own copy counts only once durable.
    nodes[0]->log->sync_through(2);
    make_leader();

    ASSERT_EQ(commit_index_of(0), 0u);
    EXPECT_TRUE(replicate_until_caught_up(0));

    // Leader plus one follower is two of three.
    EXPECT_EQ(commit_index_of(0), 2u);
}

TEST_F(ReplicationTest, CommitIndexDoesNotAdvanceWithoutAMajority) {
    build(5);
    nodes[0]->log->append(1, {put_op(1, "one")});
    nodes[0]->log->sync_through(1);
    make_leader();

    // One follower of four is two of five - short of a majority.
    EXPECT_TRUE(replicate_until_caught_up(0));
    EXPECT_EQ(commit_index_of(0), 0u);

    EXPECT_TRUE(replicate_until_caught_up(1));
    EXPECT_EQ(commit_index_of(0), 1u);
}

TEST_F(ReplicationTest, APriorTermEntryIsNotCommittedByReplicaCountAlone) {
    build(3);
    // An entry inherited from term 1, while we now lead in term 5.
    nodes[0]->log->append(1, {put_op(1, "inherited")});
    nodes[0]->log->sync_through(1);
    make_leader(5);

    EXPECT_TRUE(replicate_until_caught_up(0));
    EXPECT_TRUE(replicate_until_caught_up(1));

    // Raft 5.4.2 / Figure 8: every node holds it, but it is not from our term,
    // so counting replicas must not commit it. Committing here can lose
    // acknowledged data.
    EXPECT_EQ(commit_index_of(0), 0u);
}

TEST_F(ReplicationTest, ACurrentTermEntryCommitsTheInheritedOneTransitively) {
    build(3);
    nodes[0]->log->append(1, {put_op(1, "inherited")});
    make_leader(5);
    nodes[0]->log->append(5, {put_op(2, "ours")});
    nodes[0]->log->sync_through(2);

    EXPECT_TRUE(replicate_until_caught_up(0));

    // Committing index 2 commits everything before it.
    EXPECT_EQ(commit_index_of(0), 2u);
}

TEST_F(ReplicationTest, SendNextBacksOffUntilADivergedFollowerMatches) {
    build(3);
    // The follower holds a conflicting entry at index 1 from a dead term.
    nodes[1]->log->append(9, {put_op(99, "diverged")});

    nodes[0]->log->append(1, {put_op(1, "one")});
    nodes[0]->log->append(1, {put_op(2, "two")});
    nodes[0]->log->sync_through(2);
    make_leader(10);

    // First attempt sends from index 3's predecessor and fails the consistency
    // check, backing send_next off rather than corrupting anything.
    EXPECT_FALSE(replicators[0]->replicate_once());
    EXPECT_EQ(nodes[1]->log->last_index(), 1u);

    // Retry until it converges. Bounded: send_next only ever walks back to 1.
    for (int attempt = 0; attempt < 5 && nodes[1]->log->last_index() != 2; ++attempt) {
        replicators[0]->replicate_once();
    }

    EXPECT_EQ(nodes[1]->log->last_index(), 2u);
    // The diverged entry was replaced, not kept.
    EXPECT_EQ(nodes[1]->log->term_at(1), 1u);
}

TEST_F(ReplicationTest, ProposingOnAFollowerIsRejected) {
    build(3);
    RaftProposer proposer(*nodes[0]->state, *nodes[0]->log);

    EXPECT_EQ(proposer.propose({put_op(1, "one")}), ProposeStatus::NotLeader);
    EXPECT_EQ(nodes[0]->log->last_index(), 0u);
}

// --- Full stack, one node -------------------------------------------------

// A single-node cluster is a majority of one, so a proposal commits without any
// peer. With the apply loop running this is the whole path end to end: buffer,
// propose, replicate (vacuously), commit, apply, read back.
class SingleNodeReplicationTest : public ::testing::Test {
protected:
    void SetUp() override {
        const std::string suffix =
            std::to_string(std::chrono::steady_clock::now().time_since_epoch().count());
        temp_dir = std::filesystem::temp_directory_path() /
                   ("stoneleafdb_raft_single_" + suffix);
        std::filesystem::create_directories(temp_dir);

        wal = std::make_unique<Log>(raft_config());
        wal->open((temp_dir / "wal").string());
        transaction_manager = std::make_unique<TransactionManager>(*wal, lock_manager, store);
        store.attach_transaction_manager(*transaction_manager);
        ASSERT_EQ(store.open((temp_dir / "test.db").string()), KeyStoreStatus::Success);

        raft_log = std::make_unique<RaftLog>(raft_config());
        raft_log->open((temp_dir / "raft").string());
        hard_state = std::make_unique<RaftHardStateStore>();
        hard_state->open((temp_dir / "hardstate").string());

        const NodeAddress self_raft{"127.0.0.1", 41001};
        const NodeAddress self_client{"127.0.0.1", 41002};
        state = std::make_unique<RaftState>(
            std::vector<ClusterMember>{{.raft = self_raft, .database_server = self_client}},
            self_client, *hard_state, 0);

        applier = std::make_unique<RaftApplier>(
            *state, *raft_log, store, *transaction_manager);
        apply_thread = std::thread([this] { applier->run(); });

        {
            std::lock_guard lock(state->state_mutex);
            state->advance_term(1, self_raft);
            state->become_leader(raft_log->last_index());
        }
    }

    void TearDown() override {
        {
            std::lock_guard lock(state->state_mutex);
            state->shutting_down = true;
        }
        state->apply_cv.notify_all();
        state->applied_cv.notify_all();
        if (apply_thread.joinable()) apply_thread.join();

        applier.reset();
        state.reset();
        hard_state.reset();
        raft_log.reset();
        store.close();
        transaction_manager.reset();
        wal.reset();

        std::error_code error;
        std::filesystem::remove_all(temp_dir, error);
    }

    std::filesystem::path temp_dir;
    KeyStore store;
    LockManager lock_manager;
    std::unique_ptr<Log> wal;
    std::unique_ptr<TransactionManager> transaction_manager;
    std::unique_ptr<RaftLog> raft_log;
    std::unique_ptr<RaftHardStateStore> hard_state;
    std::unique_ptr<RaftState> state;
    std::unique_ptr<RaftApplier> applier;
    std::thread apply_thread;
};

TEST_F(SingleNodeReplicationTest, AProposalCommitsAndReachesTheStateMachine) {
    RaftProposer proposer(*state, *raft_log);

    // No replication threads exist here at all. If commit advancement lived
    // only in the replicator, this would hang until COMMIT_TIMEOUT.
    EXPECT_EQ(proposer.propose({put_op(1, "one"), put_op(2, "two")}),
              ProposeStatus::Committed);

    TransactionHandle transaction = transaction_manager->begin();
    const KeyStoreGetResult result = store.get(transaction, key_for(1));
    EXPECT_EQ(result.status, KeyStoreStatus::Success);
    ASSERT_TRUE(result.value.has_value());
    EXPECT_EQ(ValueCodec::decode(*result.value),
              std::optional<ValueInput>{ValueInput{std::string{"one"}}});
    ASSERT_EQ(transaction_manager->commit(transaction), CommitStatus::Success);
}

TEST_F(SingleNodeReplicationTest, TheWriteBufferCollapsesRepeatedWritesToOneKey) {
    TransactionWriteBuffer buffer;
    buffer.put(key_for(1), value_for("first"));
    buffer.put(key_for(1), value_for("second"));
    buffer.remove(key_for(2));

    const std::vector<MutationOp> operations = buffer.to_operations();
    ASSERT_EQ(operations.size(), 2u);

    RaftProposer proposer(*state, *raft_log);
    EXPECT_EQ(proposer.propose(operations), ProposeStatus::Committed);

    TransactionHandle transaction = transaction_manager->begin();
    const KeyStoreGetResult result = store.get(transaction, key_for(1));
    ASSERT_EQ(result.status, KeyStoreStatus::Success);
    ASSERT_TRUE(result.value.has_value());
    // Last write wins, and only one operation was ever replicated for the key.
    EXPECT_EQ(ValueCodec::decode(*result.value),
              std::optional<ValueInput>{ValueInput{std::string{"second"}}});
    ASSERT_EQ(transaction_manager->commit(transaction), CommitStatus::Success);
}

TEST_F(SingleNodeReplicationTest, AReadOnlyTransactionCommitsWithoutProposing) {
    RaftProposer proposer(*state, *raft_log);
    int sockets[2] = {-1, -1};
    ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM, 0, sockets), 0);
    std::thread dispatcher(CommandServer::serve_connection, sockets[1],
                           std::ref(store), std::ref(*transaction_manager), &proposer);
    {
        Session session(sockets[0], 0);
        session.begin_transaction();
        session.put(KeyInput{std::uint64_t{1}}, ValueInput{std::string{"a"}});
        session.commit();
        const std::uint64_t entries_after_write = raft_log->last_index();

        // Nothing buffered, so there is nothing to replicate: COMMIT must
        // succeed and must not append an entry to the Raft log.
        session.begin_transaction();
        EXPECT_EQ(session.get(KeyInput{std::uint64_t{1}}),
                  std::optional<ValueInput>{ValueInput{std::string{"a"}}});
        EXPECT_NO_THROW(session.commit());
        EXPECT_EQ(raft_log->last_index(), entries_after_write);
        session.close();
    }
    dispatcher.join();
}

} // namespace
