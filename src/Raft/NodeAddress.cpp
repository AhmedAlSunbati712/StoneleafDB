#include <Raft/NodeAddress.h>
#include <string_view>
#include <charconv>
#include <stdexcept>
std::string NodeAddress::to_string() const {
    return host + ":" + std::to_string(port);
}

NodeAddress NodeAddress::from_string(std::string_view text) {
    // First check that the separator exists and that it's
    // somewhere in the middle of the string
    std::size_t separator = text.rfind(':');

    if (separator == 0 || separator == std::string_view::npos || separator + 1 == text.size()) {
        throw std::invalid_argument("Error: Expected host:port!");
    }

    const std::string host(text.substr(0, separator));
    const std::string_view port_text = text.substr(separator + 1);

    unsigned int port = 0;
    const auto [end, error] = std::from_chars(
        port_text.data(),
        port_text.data() + port_text.size(),
        port
    );

    if (error != std::errc{} || end != port_text.data() + port_text.size() || port > 65535) {
        throw std::invalid_argument("Error: Invalid port string");
    }

    NodeAddress address = NodeAddress{
        host,
        static_cast<std::uint16_t>(port)
    };

    return address;
}
