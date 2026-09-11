#include <Raft/RaftHardStateStore.h>

#include <DiskIO.h>
#include <Endian.h>

#include <array>
#include <exception>
#include <filesystem>
#include <fcntl.h>
#include <limits>
#include <span>
#include <stdexcept>
#include <utility>
#include <vector>

namespace {

constexpr std::size_t TERM_SIZE = sizeof(std::uint64_t);
constexpr std::size_t HAS_VOTE_SIZE = sizeof(std::uint8_t);
constexpr std::size_t VOTE_LENGTH_SIZE = sizeof(std::uint32_t);
constexpr std::size_t HEADER_SIZE = TERM_SIZE + HAS_VOTE_SIZE + VOTE_LENGTH_SIZE;

RaftHardState decode_state(std::span<const char> encoded) {
    if (encoded.size() < HEADER_SIZE) {
        throw std::runtime_error("Raft hard state is truncated");
    }
    const std::uint64_t term = get_u64_be(encoded.data());
    const std::uint8_t has_vote = static_cast<std::uint8_t>(encoded[TERM_SIZE]);
    if (has_vote > 1) throw std::runtime_error("Raft hard state has an invalid vote flag");
    const std::uint32_t vote_length = get_u32_be(encoded.data() + TERM_SIZE + HAS_VOTE_SIZE);
    if (encoded.size() != HEADER_SIZE + vote_length) {
        throw std::runtime_error("Raft hard-state vote length disagrees with file size");
    }
    if (has_vote == 0 && vote_length != 0) {
        throw std::runtime_error("Raft hard state has vote bytes without a vote");
    }
    if (has_vote == 1 && vote_length == 0) {
        throw std::runtime_error("Raft hard state has an empty voted-for identity");
    }
    if (term == 0 && has_vote == 1) {
        throw std::runtime_error("Raft term zero cannot carry a vote");
    }

    RaftHardState result{.term = term};
    if (has_vote == 1) {
        result.voted_for = std::string(
            encoded.begin() + static_cast<std::ptrdiff_t>(HEADER_SIZE), encoded.end());
    }
    return result;
}

std::vector<char> encode_state(
    std::uint64_t term,
    const std::optional<std::string>& voted_for) {
    if (term == 0 && voted_for.has_value()) {
        throw std::invalid_argument("Raft term zero cannot carry a vote");
    }
    if (voted_for.has_value() && voted_for->empty()) {
        throw std::invalid_argument("Raft voted-for identity must not be empty");
    }
    const std::size_t vote_size = voted_for.has_value() ? voted_for->size() : 0;
    if (vote_size > std::numeric_limits<std::uint32_t>::max()) {
        throw std::invalid_argument("Raft voted-for identity is too large");
    }

    std::vector<char> encoded(HEADER_SIZE + vote_size, 0);
    put_u64_be(encoded.data(), term);
    encoded[TERM_SIZE] = voted_for.has_value() ? '\1' : '\0';
    put_u32_be(
        encoded.data() + TERM_SIZE + HAS_VOTE_SIZE,
        static_cast<std::uint32_t>(vote_size));
    if (voted_for.has_value()) {
        std::copy(voted_for->begin(), voted_for->end(), encoded.begin() + HEADER_SIZE);
    }
    return encoded;
}

} // namespace

void RaftHardStateStore::open(const std::string& directory) {
    if (directory.empty()) throw std::invalid_argument("Raft directory must not be empty");
    std::lock_guard lock(mutex_);
    if (!directory_.empty()) throw std::runtime_error("Raft hard-state store is already open");

    std::filesystem::create_directories(directory);
    const auto path = std::filesystem::path(directory) / "state";
    RaftHardState loaded;
    if (std::filesystem::exists(path)) {
        const int fd = disk::open_file(path.string(), O_RDONLY);
        try {
            const std::size_t size = disk::file_size(fd);
            std::vector<char> encoded(size);
            disk::read_exact_at(fd, encoded, 0);
            disk::close_file(fd);
            loaded = decode_state(encoded);
        } catch (...) {
            std::exception_ptr failure = std::current_exception();
            try { disk::close_file(fd); } catch (...) {}
            std::rethrow_exception(failure);
        }
    }
    directory_ = directory;
    state_ = std::move(loaded);
}

void RaftHardStateStore::close() noexcept {
    std::lock_guard lock(mutex_);
    directory_.clear();
    state_ = {};
}

bool RaftHardStateStore::is_open() const noexcept {
    std::lock_guard lock(mutex_);
    return !directory_.empty();
}

RaftHardState RaftHardStateStore::load() const {
    std::lock_guard lock(mutex_);
    if (directory_.empty()) throw std::runtime_error("Raft hard-state store is not open");
    return state_;
}

void RaftHardStateStore::persist(
    std::uint64_t term,
    std::optional<std::string> voted_for) {
    std::lock_guard lock(mutex_);
    if (directory_.empty()) throw std::runtime_error("Raft hard-state store is not open");
    if (term < state_.term) throw std::invalid_argument("Raft term must not decrease");
    if (term == state_.term && state_.voted_for.has_value() &&
        state_.voted_for != voted_for) {
        throw std::invalid_argument("Raft vote cannot change within one term");
    }

    const std::vector<char> encoded = encode_state(term, voted_for);
    const auto directory = std::filesystem::path(directory_);
    const auto temporary = directory / "state.tmp";
    const auto destination = directory / "state";
    const int fd = disk::open_file(
        temporary.string(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
    try {
        disk::write_exact_at(fd, encoded, 0);
        disk::sync_file_to_disk_fd(fd);
        disk::close_file(fd);
    } catch (...) {
        std::exception_ptr failure = std::current_exception();
        try { disk::close_file(fd); } catch (...) {}
        std::rethrow_exception(failure);
    }

    std::filesystem::rename(temporary, destination);
    state_ = {.term = term, .voted_for = std::move(voted_for)};
    disk::sync_directory(directory_);
}
