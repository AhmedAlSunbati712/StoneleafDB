#pragma once

#include <Raft/NodeAddress.h>

#include <grpcpp/client_context.h>
#include <grpcpp/server.h>
#include <raft.grpc.pb.h>

#include <chrono>
#include <cstdint>
#include <memory>
#include <unordered_map>
#include <vector>

// The outbound half of the Raft plane, and the one place the RPC tuning lives so
// the server and the channels cannot drift apart. Like RaftProtoCodec.h this
// header includes generated code, so RaftState, RaftLog and the apply loop must
// not include it.
namespace RaftRpc {

// A single AppendEntries carries a whole batch of mutation entries, which can
// exceed gRPC's 4 MiB default. The limit is enforced independently at BOTH ends:
// raising it only on the sender turns an oversized message into a peer-side
// RESOURCE_EXHAUSTED rather than a larger message, so start_server() and the
// channels below both apply this.
constexpr int MAX_MESSAGE_BYTES = 64 * 1024 * 1024;

// HEARTBEAT_INTERVAL (50ms) < RPC_DEADLINE < ELECTION_TIMEOUT_MIN (150ms), and
// both halves of that ordering are load-bearing:
//   - above the heartbeat interval, so a merely slow heartbeat is not cancelled
//     and retried while the first one is still in flight;
//   - below the minimum election timeout, so a replication thread blocked on a
//     dead peer is always released before that peer's silence could cost us an
//     election we would otherwise have won.
constexpr auto RPC_DEADLINE = std::chrono::milliseconds(100);

// Keepalive, so a peer that disappears without sending a FIN - a partition, a
// hard kill - is discovered by the transport instead of only ever by RPC
// deadlines.
constexpr int KEEPALIVE_TIME_MS = 10000;
constexpr int KEEPALIVE_TIMEOUT_MS = 2000;

// Applied per call. A ClientContext is single-use and must never be reused, so
// there is nowhere to cache this.
void apply_deadline(grpc::ClientContext& context);

// Starts a Raft RPC server on 0.0.0.0:<port> with the message-size limits above.
// Binding the wildcard address rather than the configured host is deliberate:
// peers on other hosts have to be able to reach us, while the address we are
// KNOWN by stays the config spelling, which is what is_peer() matches against.
// Returns nullptr when the port cannot be bound - BuildAndStart()'s way of
// reporting failure - so callers must check.
std::unique_ptr<grpc::Server> start_server(
    std::uint16_t port, stoneleaf::raft::RaftService::Service& service);

} // namespace RaftRpc

// One channel and one stub per peer, built once at startup. Channels connect
// lazily and reconnect themselves, so construction succeeds even when every peer
// is unreachable - which is the normal case while a cluster is still coming up,
// and the reason startup does not depend on peer availability.
class RaftPeerClients {
public:
    // Builds a stub for each address given. Self must not be among them.
    explicit RaftPeerClients(const std::vector<NodeAddress>& peers);

    RaftPeerClients(const RaftPeerClients&) = delete;
    RaftPeerClients& operator=(const RaftPeerClients&) = delete;

    // Throws std::out_of_range for an address that was never in the peer list.
    // An unknown peer means this node and its caller disagree about the cluster,
    // which must surface rather than be papered over with a null stub.
    stoneleaf::raft::RaftService::StubInterface& stub(const NodeAddress& peer) const;

    // Substitutes a fake, so the replication and election paths can be tested
    // without standing up a server for every peer.
    void set_stub_for_testing(
        const NodeAddress& peer,
        std::unique_ptr<stoneleaf::raft::RaftService::StubInterface> stub);

private:
    // Channels are held alongside the stubs purely to keep them alive: a stub
    // shares ownership, but holding the channel makes the lifetime explicit.
    std::unordered_map<NodeAddress, std::shared_ptr<grpc::Channel>> channels_;
    std::unordered_map<NodeAddress,
                       std::unique_ptr<stoneleaf::raft::RaftService::StubInterface>> stubs_;
};
