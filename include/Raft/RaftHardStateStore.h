#pragma once

#include <cstdint>
#include <mutex>
#include <optional>
#include <string>

struct RaftHardState {
    std::uint64_t term = 0;
    std::optional<std::string> voted_for;
};

class RaftHardStateStore {
public:
    void open(const std::string& directory);
    void close() noexcept;
    bool is_open() const noexcept;

    RaftHardState load() const;
    void persist(std::uint64_t term, std::optional<std::string> voted_for);

private:
    std::string directory_;
    RaftHardState state_;
    mutable std::mutex mutex_;
};
