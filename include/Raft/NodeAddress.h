#pragma once
#include <compare>
#include <cstdint>
#include <functional>
#include <string>
#include <string_view>

// Nodes are identified by address rather than by a separate id, so this is
// also the type persisted in voted_for and sent on the wire.
struct NodeAddress {
    std::string host;     // exactly as written in the cluster config
    std::uint16_t port;

    bool operator==(const NodeAddress&) const = default;
    auto operator<=>(const NodeAddress&) const = default;

    // "host:port". Also the gRPC target, and the form RaftHardStateStore
    // persists voted_for in.
    std::string to_string() const;

    // Inverse of to_string(); splits on the last ':'. Used to read the cluster
    // config and the persisted vote. Must round-trip exactly -
    // from_string(s).to_string() == s - or config spellings drift.
    static NodeAddress from_string(std::string_view text);
};

// Required to key an unordered_map on NodeAddress.
template <>
struct std::hash<NodeAddress> {
    std::size_t operator()(const NodeAddress& a) const noexcept {
        std::size_t h = std::hash<std::string>{}(a.host);
        h ^= std::hash<std::uint16_t>{}(a.port) + 0x9e3779b9 + (h << 6) + (h >> 2);
        return h;
    }
};
