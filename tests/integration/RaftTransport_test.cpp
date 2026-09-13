#include <gtest/gtest.h>

#include <KeyCodec.h>
#include <Raft/RaftHardStateStore.h>
#include <Raft/RaftLog.h>
#include <Raft/RaftPeerClients.h>
#include <Raft/RaftProtoCodec.h>
#include <Raft/RaftServiceImpl.h>
#include <Raft/RaftState.h>
#include <ValueCodec.h>
#include <storage/Index.h>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <chrono>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <system_error>
#include <vector>

// Two real nodes in one process, talking over a real gRPC channel. The handler
// tests in RaftService_test.cpp call the methods directly; these exercise the
// transport itself - serialization, the service registration, the size limits
// and the deadline.
namespace {

namespace raftpb = stoneleaf::raft;

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

// Binds a throwaway socket to port 0, reads back the port the kernel picked, and
// releases it. The cluster config has to name every raft port before either
// RaftState exists, and a gRPC server only reports its port after it is already
// running, so the ports cannot come from the servers themselves. The window
// between release and rebind is theoretically racy; nothing else in the suite
// listens, so in practice it is not.
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

    // Mirrors the server's shutdown order: the RPC server stops first, because
    // Shutdown() returns only once every in-flight handler has returned, and a
    // handler writes to the log we are about to destroy.
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

class RaftTransportTest : public ::testing::Test {
protected:
    void SetUp() override {
        a_raft = {"127.0.0.1", reserve_port()};
        b_raft = {"127.0.0.1", reserve_port()};
        c_raft = {"127.0.0.1", reserve_port()};
        ASSERT_NE(a_raft.port, 0);
        ASSERT_NE(b_raft.port, 0);
        ASSERT_NE(c_raft.port, 0);

        // Three members so a second candidate has a legitimate identity to use.
        // Only a and b get servers; c exists in the config and never answers,
        // which is what the unreachable-peer case needs.
        cluster = {
            {.raft = a_raft, .database_server = NodeAddress{"127.0.0.1", 16001}},
            {.raft = b_raft, .database_server = NodeAddress{"127.0.0.1", 16002}},
            {.raft = c_raft, .database_server = NodeAddress{"127.0.0.1", 16003}},
        };

        start(node_a, cluster[0].database_server, a_raft, "a");
        start(node_b, cluster[1].database_server, b_raft, "b");

        peers_of_a = std::make_unique<RaftPeerClients>(std::vector<NodeAddress>{b_raft, c_raft});
    }

    void TearDown() override {
        peers_of_a.reset();
        node_a.stop();
        node_b.stop();
    }

    void start(Node& node, const NodeAddress& self_client,
               const NodeAddress& raft, const std::string& tag) {
        const std::string suffix = std::to_string(
            std::chrono::steady_clock::now().time_since_epoch().count());
        node.directory = std::filesystem::temp_directory_path() /
                         ("stoneleafdb_raft_transport_" + tag + "_" + suffix);
        std::filesystem::create_directories(node.directory);

        node.log = std::make_unique<RaftLog>(raft_config());
        node.log->open((node.directory / "raft").string());

        node.hard_state = std::make_unique<RaftHardStateStore>();
        node.hard_state->open((node.directory / "hardstate").string());

        node.state = std::make_unique<RaftState>(cluster, self_client, *node.hard_state, 0);
        node.service = std::make_unique<RaftServiceImpl>(*node.state, *node.log);
        node.server = RaftRpc::start_server(raft.port, *node.service);
        ASSERT_NE(node.server, nullptr) << "failed to bind raft port " << raft.port;
    }

    NodeAddress a_raft;
    NodeAddress b_raft;
    NodeAddress c_raft;
    std::vector<ClusterMember> cluster;
    Node node_a;
    Node node_b;
    std::unique_ptr<RaftPeerClients> peers_of_a;
};

TEST_F(RaftTransportTest, ARequestVoteCrossesTheWireAndIsGranted) {
    raftpb::RequestVoteRequest request;
    request.set_term(1);
    request.set_candidate(a_raft.to_string());
    request.set_last_log_idx(0);
    request.set_last_log_term(0);

    grpc::ClientContext context;
    RaftRpc::apply_deadline(context);
    raftpb::RequestVoteResponse response;
    const grpc::Status status =
        peers_of_a->stub(b_raft).RequestVote(&context, request, &response);

    ASSERT_TRUE(status.ok()) << status.error_message();
    EXPECT_TRUE(response.vote_granted());
    EXPECT_EQ(response.term(), 1u);

    // The vote is recorded on the responder, not merely asserted in its reply.
    std::lock_guard lock(node_b.state->state_mutex);
    EXPECT_EQ(node_b.state->current_term(), 1u);
    EXPECT_EQ(node_b.state->voted_for(), std::optional<NodeAddress>{a_raft});
}

TEST_F(RaftTransportTest, ASecondCandidateInTheSameTermIsDeniedOverTheWire) {
    raftpb::RequestVoteRequest first;
    first.set_term(1);
    first.set_candidate(a_raft.to_string());
    grpc::ClientContext first_context;
    RaftRpc::apply_deadline(first_context);
    raftpb::RequestVoteResponse first_response;
    ASSERT_TRUE(peers_of_a->stub(b_raft)
                    .RequestVote(&first_context, first, &first_response)
                    .ok());
    ASSERT_TRUE(first_response.vote_granted());

    // Same term, different candidate. A fresh ClientContext per call: they are
    // single-use, and reusing the first would abort the second.
    raftpb::RequestVoteRequest second;
    second.set_term(1);
    second.set_candidate(c_raft.to_string());
    grpc::ClientContext second_context;
    RaftRpc::apply_deadline(second_context);
    raftpb::RequestVoteResponse second_response;
    ASSERT_TRUE(peers_of_a->stub(b_raft)
                    .RequestVote(&second_context, second, &second_response)
                    .ok());

    EXPECT_FALSE(second_response.vote_granted());
}

TEST_F(RaftTransportTest, AnAppendEntriesCrossesTheWireAndLandsInTheFollowersLog) {
    raftpb::AppendEntriesRequest request;
    request.set_term(1);
    request.set_leader(a_raft.to_string());
    request.set_prev_log_idx(0);
    request.set_prev_log_term(0);
    request.set_leader_commit_idx(1);
    const RaftMutationEntry entry{
        .term = 1, .idx = 1, .operations = {put_op(1, "one"), put_op(2, "two")}};
    RaftProtoCodec::to_proto(entry, request.add_entries());

    grpc::ClientContext context;
    RaftRpc::apply_deadline(context);
    raftpb::AppendEntriesResponse response;
    const grpc::Status status =
        peers_of_a->stub(b_raft).AppendEntries(&context, request, &response);

    ASSERT_TRUE(status.ok()) << status.error_message();
    EXPECT_TRUE(response.success());
    EXPECT_EQ(response.term(), 1u);

    // The whole round trip: encoded here, decoded there, appended, synced.
    EXPECT_EQ(node_b.log->last_index(), 1u);
    EXPECT_EQ(node_b.log->durable_index(), 1u);
    const RaftMutationEntry stored = node_b.log->read(1);
    EXPECT_EQ(stored.term, 1u);
    EXPECT_EQ(stored.idx, 1u);
    EXPECT_EQ(stored.operations.size(), 2u);

    std::lock_guard lock(node_b.state->state_mutex);
    EXPECT_EQ(node_b.state->commit_index(), 1u);
    EXPECT_EQ(node_b.state->leader_raft_address(), std::optional<NodeAddress>{a_raft});
}

TEST_F(RaftTransportTest, AMalformedEntryIsRejectedAcrossTheWire) {
    raftpb::AppendEntriesRequest request;
    request.set_term(1);
    request.set_leader(a_raft.to_string());
    request.set_prev_log_idx(0);
    request.set_prev_log_term(0);

    raftpb::RaftEntry* bad = request.add_entries();
    bad->set_term(1);
    bad->set_idx(1);
    raftpb::Mutation* mutation = bad->add_operations();
    mutation->set_type(raftpb::MUTATION_UNSPECIFIED);
    RaftProtoCodec::to_proto(key_for(1), mutation->mutable_key());

    grpc::ClientContext context;
    RaftRpc::apply_deadline(context);
    raftpb::AppendEntriesResponse response;
    const grpc::Status status =
        peers_of_a->stub(b_raft).AppendEntries(&context, request, &response);

    // The codec's exception becomes a status, not a crashed peer.
    EXPECT_EQ(status.error_code(), grpc::StatusCode::INVALID_ARGUMENT);
    EXPECT_EQ(node_b.log->last_index(), 0u);
}

TEST_F(RaftTransportTest, AnUnreachablePeerFailsWellInsideTheElectionTimeout) {
    // c is in the config and has no server. A replication thread must be
    // released before the silence could cost an election, which is the whole
    // reason RPC_DEADLINE sits below ELECTION_TIMEOUT_MIN.
    raftpb::RequestVoteRequest request;
    request.set_term(1);
    request.set_candidate(a_raft.to_string());

    const auto started = std::chrono::steady_clock::now();
    grpc::ClientContext context;
    RaftRpc::apply_deadline(context);
    raftpb::RequestVoteResponse response;
    const grpc::Status status =
        peers_of_a->stub(c_raft).RequestVote(&context, request, &response);
    const auto elapsed = std::chrono::steady_clock::now() - started;

    EXPECT_FALSE(status.ok());
    EXPECT_LT(elapsed, RaftState::ELECTION_TIMEOUT_MIN);
}

TEST_F(RaftTransportTest, AnUnknownPeerHasNoStub) {
    EXPECT_THROW(peers_of_a->stub(NodeAddress{"127.0.0.1", 1}), std::out_of_range);
}

} // namespace
