#include <Raft/RaftPeerClients.h>

#include <grpcpp/grpcpp.h>

#include <stdexcept>
#include <string>
#include <utility>

namespace {

grpc::ChannelArguments peer_channel_arguments() {
    grpc::ChannelArguments arguments;
    arguments.SetMaxReceiveMessageSize(RaftRpc::MAX_MESSAGE_BYTES);
    arguments.SetMaxSendMessageSize(RaftRpc::MAX_MESSAGE_BYTES);
    arguments.SetInt(GRPC_ARG_KEEPALIVE_TIME_MS, RaftRpc::KEEPALIVE_TIME_MS);
    arguments.SetInt(GRPC_ARG_KEEPALIVE_TIMEOUT_MS, RaftRpc::KEEPALIVE_TIMEOUT_MS);
    // Without this, keepalive only runs while a call is outstanding. A follower
    // between heartbeats has none, so its channel would never be probed and a
    // silently dead peer would stay "connected" until the next RPC.
    arguments.SetInt(GRPC_ARG_KEEPALIVE_PERMIT_WITHOUT_CALLS, 1);
    return arguments;
}

} // namespace

namespace RaftRpc {

void apply_deadline(grpc::ClientContext& context) {
    // system_clock, not steady_clock: gRPC deadlines are absolute wall-clock
    // timestamps.
    context.set_deadline(std::chrono::system_clock::now() + RPC_DEADLINE);
}

std::unique_ptr<grpc::Server> start_server(
    std::uint16_t port, stoneleaf::raft::RaftService::Service& service) {
    grpc::ServerBuilder builder;
    builder.SetMaxReceiveMessageSize(MAX_MESSAGE_BYTES);
    builder.SetMaxSendMessageSize(MAX_MESSAGE_BYTES);
    builder.AddListeningPort("0.0.0.0:" + std::to_string(port),
                             grpc::InsecureServerCredentials());
    builder.RegisterService(&service);
    // BuildAndStart() returns a running server, or nullptr if the port could not
    // be bound. It does NOT block - Wait() does, and the database server has its
    // own accept loop to run, so it never calls it.
    return builder.BuildAndStart();
}

} // namespace RaftRpc

RaftPeerClients::RaftPeerClients(const std::vector<NodeAddress>& peers) {
    const grpc::ChannelArguments arguments = peer_channel_arguments();
    for (const NodeAddress& peer : peers) {
        // Lazy: no connection is attempted here, so a peer that is down cannot
        // fail our startup.
        std::shared_ptr<grpc::Channel> channel = grpc::CreateCustomChannel(
            peer.to_string(), grpc::InsecureChannelCredentials(), arguments);
        stubs_[peer] = stoneleaf::raft::RaftService::NewStub(channel);
        channels_[peer] = std::move(channel);
    }
}

stoneleaf::raft::RaftService::StubInterface& RaftPeerClients::stub(
    const NodeAddress& peer) const {
    const auto found = stubs_.find(peer);
    if (found == stubs_.end()) {
        throw std::out_of_range("No Raft stub for peer " + peer.to_string());
    }
    return *found->second;
}

void RaftPeerClients::set_stub_for_testing(
    const NodeAddress& peer,
    std::unique_ptr<stoneleaf::raft::RaftService::StubInterface> stub) {
    stubs_[peer] = std::move(stub);
}
