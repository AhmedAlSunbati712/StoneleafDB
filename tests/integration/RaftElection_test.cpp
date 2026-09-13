#include <gtest/gtest.h>

#include <Raft/RaftElection.h>
#include <Raft/RaftHardStateStore.h>
#include <Raft/RaftLog.h>
#include <Raft/RaftPeerClients.h>
#include <Raft/RaftServiceImpl.h>
#include <Raft/RaftState.h>
#include <storage/Index.h>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <optional>
#include <string>
#include <system_error>
#include <thread>
#include <vector>

// Elections driven over the real transport: real servers, real channels, real
// RequestVote. Campaigns are driven directly through campaign() so the tests do
// not race the timer; run() is exercised separately for its parking and exit.
namespace {

Config raft_config() {
    return {.max_index_bytes = 1000 * Index::ENTRY_SIZE,
            .max_store_bytes = 16 * 1024 * 1024,
            .initial_lsn = 1};
}

// See RaftTransport_test: the cluster config must name every raft port before
// any RaftState exists, and a gRPC server only reports its port once running.
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

class RaftElectionTest : public ::testing::Test {
protected:
    void TearDown() override {
        clients.reset();
        for (auto& node : nodes) node->stop();
        nodes.clear();
    }

    // Builds a cluster of `size` members and opens storage for each. Servers are
    // started only for the indexes in `serving`; the rest exist in the config
    // and never answer, which is how an unreachable peer is modelled.
    void build(std::size_t size, const std::vector<std::size_t>& serving) {
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
                              ("stoneleafdb_raft_election_" + std::to_string(i) + "_" + suffix);
            std::filesystem::create_directories(node->directory);

            node->log = std::make_unique<RaftLog>(raft_config());
            node->log->open((node->directory / "raft").string());
            node->hard_state = std::make_unique<RaftHardStateStore>();
            node->hard_state->open((node->directory / "hardstate").string());
            node->state = std::make_unique<RaftState>(
                cluster, client_addresses[i], *node->hard_state, 0);

            const bool serves =
                std::find(serving.begin(), serving.end(), i) != serving.end();
            if (serves) {
                node->service = std::make_unique<RaftServiceImpl>(*node->state, *node->log);
                node->server = RaftRpc::start_server(raft_addresses[i].port, *node->service);
                ASSERT_NE(node->server, nullptr);
            }
            nodes.push_back(std::move(node));
        }

        // Node 0 is always the campaigner; its peers are everyone else.
        std::vector<NodeAddress> peers(raft_addresses.begin() + 1, raft_addresses.end());
        clients = std::make_unique<RaftPeerClients>(peers);
    }

    State state_of(std::size_t i) {
        std::lock_guard lock(nodes[i]->state->state_mutex);
        return nodes[i]->state->state();
    }
    std::uint64_t term_of(std::size_t i) {
        std::lock_guard lock(nodes[i]->state->state_mutex);
        return nodes[i]->state->current_term();
    }

    std::vector<NodeAddress> raft_addresses;
    std::vector<NodeAddress> client_addresses;
    std::vector<ClusterMember> cluster;
    std::vector<std::unique_ptr<Node>> nodes;
    std::unique_ptr<RaftPeerClients> clients;
};

TEST_F(RaftElectionTest, WinsWithOneGrantingPeerOfTwo) {
    // Three nodes, two reachable. Two votes of three is a majority once our own
    // is counted, so node 0 wins without node 2 ever answering.
    build(3, {0, 1});
    RaftElection election(*nodes[0]->state, *nodes[0]->log, *clients);

    EXPECT_TRUE(election.campaign());
    EXPECT_EQ(state_of(0), State::Leader);
    EXPECT_EQ(term_of(0), 1u);
    // The granting peer recorded the vote durably, not just in its reply.
    std::lock_guard lock(nodes[1]->state->state_mutex);
    EXPECT_EQ(nodes[1]->state->voted_for(), std::optional<NodeAddress>{raft_addresses[0]});
}

TEST_F(RaftElectionTest, CannotWinAloneInAThreeNodeCluster) {
    // Only node 0 is up. One vote of three is not a majority.
    build(3, {0});
    RaftElection election(*nodes[0]->state, *nodes[0]->log, *clients);

    EXPECT_FALSE(election.campaign());
    EXPECT_EQ(state_of(0), State::Candidate);
    // It still advanced the term and voted for itself, durably.
    EXPECT_EQ(term_of(0), 1u);
    std::lock_guard lock(nodes[0]->state->state_mutex);
    EXPECT_EQ(nodes[0]->state->voted_for(), std::optional<NodeAddress>{raft_addresses[0]});
}

TEST_F(RaftElectionTest, ASingleNodeClusterWinsWithoutAskingAnyone) {
    // Majority of one. There is no peer to ask, so this must not hang.
    build(1, {0});
    RaftElection election(*nodes[0]->state, *nodes[0]->log, *clients);

    EXPECT_TRUE(election.campaign());
    EXPECT_EQ(state_of(0), State::Leader);
}

TEST_F(RaftElectionTest, AHigherTermReplyStepsTheCandidateDown) {
    build(3, {0, 1});
    {
        // Node 1 is far ahead. Its reply carries term 10.
        std::lock_guard lock(nodes[1]->state->state_mutex);
        nodes[1]->state->advance_term(10);
    }

    RaftElection election(*nodes[0]->state, *nodes[0]->log, *clients);
    EXPECT_FALSE(election.campaign());

    EXPECT_EQ(state_of(0), State::Follower);
    EXPECT_EQ(term_of(0), 10u);
}

TEST_F(RaftElectionTest, ALeaderDoesNotCampaign) {
    build(3, {0, 1});
    RaftElection election(*nodes[0]->state, *nodes[0]->log, *clients);
    ASSERT_TRUE(election.campaign());
    ASSERT_EQ(state_of(0), State::Leader);

    const std::uint64_t term_while_leading = term_of(0);
    EXPECT_FALSE(election.campaign());
    // No term burned: a leader campaigning would depose itself for nothing.
    EXPECT_EQ(term_of(0), term_while_leading);
    EXPECT_EQ(state_of(0), State::Leader);
}

TEST_F(RaftElectionTest, RepeatedCampaignsAdvanceTheTermEachTime) {
    build(3, {0});
    RaftElection election(*nodes[0]->state, *nodes[0]->log, *clients);

    for (std::uint64_t expected = 1; expected <= 3; ++expected) {
        EXPECT_FALSE(election.campaign());
        EXPECT_EQ(term_of(0), expected);
    }
    // Every campaign joined its voters; nothing was left running.
    EXPECT_EQ(state_of(0), State::Candidate);
}

TEST_F(RaftElectionTest, TheThreadCampaignsOnceTheDeadlineElapses) {
    build(3, {0, 1});
    RaftElection election(*nodes[0]->state, *nodes[0]->log, *clients);
    std::thread thread([&election] { election.run(); });

    // The constructor already armed the timer, so this needs no prodding: wait
    // out at most a few timeouts for the node to campaign and win.
    const auto give_up = std::chrono::steady_clock::now() + std::chrono::seconds(3);
    while (state_of(0) != State::Leader && std::chrono::steady_clock::now() < give_up) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    EXPECT_EQ(state_of(0), State::Leader);
    EXPECT_GE(term_of(0), 1u);

    {
        std::lock_guard lock(nodes[0]->state->state_mutex);
        nodes[0]->state->shutting_down = true;
    }
    nodes[0]->state->election_cv.notify_all();
    thread.join();   // hangs the suite if the wait predicate ignores shutting_down
}

TEST_F(RaftElectionTest, TheThreadExitsPromptlyWhileParkedAsAFollower) {
    build(3, {0});
    RaftElection election(*nodes[0]->state, *nodes[0]->log, *clients);
    std::thread thread([&election] { election.run(); });

    {
        std::lock_guard lock(nodes[0]->state->state_mutex);
        nodes[0]->state->shutting_down = true;
    }
    nodes[0]->state->election_cv.notify_all();

    thread.join();
    SUCCEED();
}

} // namespace
