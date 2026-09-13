#pragma once

#include <Raft/RaftState.h>

#include <grpcpp/server_context.h>
#include <grpcpp/support/status.h>
#include <raft.grpc.pb.h>

// Only a reference is held, so the log's definition is not needed here. Keeping
// it out means including this header does not drag in RaftSegment and the WAL
// config along with it.
class RaftLog;

// The inbound half of the Raft plane: the two RPCs every node serves on its
// raft address. Both handlers take state_mutex for the whole call and reply
// only from state that is already durable, so a peer can never observe a term
// or a vote this node would forget across a crash.
//
// One instance is registered with the gRPC server at startup; the sync server's
// thread pool calls into it from many threads at once, which is why every
// handler body locks rather than assuming a single caller. state and log must
// both outlive the gRPC server, so they are torn down after it stops.
class RaftServiceImpl final : public stoneleaf::raft::RaftService::Service {
    public:
        RaftServiceImpl(RaftState& state, RaftLog& log);

        // override on both, deliberately: the signatures must match the
        // generated Service base exactly. Without it, any drift (a stray
        // const, a renamed message) would define a new non-virtual member
        // instead, leaving the base's default body in place - the build stays
        // green and every peer RPC returns UNIMPLEMENTED at runtime, so the
        // cluster never elects a leader and nothing points at why.
        grpc::Status RequestVote(grpc::ServerContext* context,
                                 const stoneleaf::raft::RequestVoteRequest* request,
                                 stoneleaf::raft::RequestVoteResponse* response) override;

        grpc::Status AppendEntries(grpc::ServerContext* context,
                                   const stoneleaf::raft::AppendEntriesRequest* request,
                                   stoneleaf::raft::AppendEntriesResponse* response) override;

    private:
        RaftState& state_;
        RaftLog& log_;
};
