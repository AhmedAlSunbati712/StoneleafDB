#include <Raft/ClusterConfig.h>

#include <fstream>
#include <sstream>
#include <stdexcept>
#include <unordered_set>

namespace {

std::runtime_error malformed(const std::string& path, std::size_t line_number, const std::string& why) {
    return std::runtime_error(
        "Cluster config " + path + " line " + std::to_string(line_number) + ": " + why);
}

} // namespace

std::vector<ClusterMember> parse_cluster_config(const std::string& path) {
    std::ifstream file(path);
    if (!file) throw std::runtime_error("Cannot read cluster config " + path);

    std::vector<ClusterMember> cluster;
    // Duplicates are compared on the verbatim spelling, which is what every
    // other comparison in the system uses.
    std::unordered_set<std::string> raft_addresses;
    std::unordered_set<std::string> client_addresses;

    std::string line;
    for (std::size_t line_number = 1; std::getline(file, line); ++line_number) {
        const std::size_t comment = line.find('#');
        if (comment != std::string::npos) line.erase(comment);

        std::istringstream fields(line);
        std::string raft_text;
        std::string client_text;
        if (!(fields >> raft_text)) continue;   // blank or comment-only line
        if (!(fields >> client_text)) {
            throw malformed(path, line_number, "expected '<raft_addr> <database_server_addr>'");
        }

        std::string extra;
        if (fields >> extra) {
            throw malformed(path, line_number, "expected exactly two addresses");
        }

        ClusterMember member;
        try {
            member.raft = NodeAddress::from_string(raft_text);
            member.database_server = NodeAddress::from_string(client_text);
        } catch (const std::exception& error) {
            throw malformed(path, line_number, error.what());
        }

        if (!raft_addresses.insert(raft_text).second) {
            throw malformed(path, line_number, "duplicate raft address " + raft_text);
        }
        if (!client_addresses.insert(client_text).second) {
            throw malformed(path, line_number, "duplicate database server address " + client_text);
        }

        cluster.push_back(std::move(member));
    }

    if (cluster.empty()) throw std::runtime_error("Cluster config " + path + " has no nodes");
    return cluster;
}
