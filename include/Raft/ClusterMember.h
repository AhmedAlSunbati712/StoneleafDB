#pragma once
#include <Raft/NodeAddress.h>

// One row of the cluster config.
struct ClusterMember {
    NodeAddress raft;             // gRPC address; the node's identity
    NodeAddress database_server;  // where database clients connect; only ever handed to clients
};
