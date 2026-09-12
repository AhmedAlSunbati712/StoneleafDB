#pragma once

#include <Raft/ClusterMember.h>

#include <string>
#include <vector>

// Parses the cluster configuration file: one row per node, two whitespace
// separated columns, "<raft_addr> <database_server_addr>". Blank lines are
// skipped, and everything from a '#' to end of line is a comment.
//
// The config is the only source of address spellings and is required to be
// byte-identical on every node, so anything ambiguous is a startup failure
// rather than a guess. Throws std::runtime_error when the file cannot be read,
// a row does not have exactly two columns, an address is not host:port, an
// address appears twice in either column, or the file contains no rows.
std::vector<ClusterMember> parse_cluster_config(const std::string& path);
