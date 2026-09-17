#include <Raft/RaftLog.h>

#include <DiskIO.h>
#include <Raft/RaftEntryCodec.h>

#include <algorithm>
#include <filesystem>
#include <fcntl.h>
#include <iomanip>
#include <map>
#include <mutex>
#include <sstream>
#include <stdexcept>

namespace {

struct SegmentFiles {
    bool store = false;
    bool index = false;
};

std::string segment_stem(std::uint64_t base_index) {
    std::ostringstream name;
    name << "segment-" << std::setw(20) << std::setfill('0') << base_index;
    return name.str();
}

std::pair<std::uint64_t, bool> parse_segment_name(const std::string& name) {
    constexpr std::size_t prefix_size = 8;
    constexpr std::size_t digits_size = 20;
    const bool store = name.size() == prefix_size + digits_size + 6 &&
        name.ends_with(".store");
    const bool index = name.size() == prefix_size + digits_size + 6 &&
        name.ends_with(".index");
    if (!name.starts_with("segment-")) return {0, false};
    if (!store && !index) throw std::runtime_error("Malformed Raft segment filename");
    const std::string digits = name.substr(prefix_size, digits_size);
    if (digits.find_first_not_of("0123456789") != std::string::npos) {
        throw std::runtime_error("Malformed Raft segment filename");
    }
    try {
        return {std::stoull(digits), store};
    } catch (...) {
        throw std::runtime_error("Malformed Raft segment filename");
    }
}

} // namespace

RaftLog::RaftLog(Config config) : config_(config) {
    config_.validate();
    if (config_.initial_lsn != 1) {
        throw std::invalid_argument("Raft log indexes must begin at one");
    }
}

RaftLog::~RaftLog() noexcept {
    std::unique_lock lock(mutex_);
    wait_for_sync_to_finish(lock);
    segments_.clear();
}

void RaftLog::open(const std::string& directory) {
    if (directory.empty()) throw std::invalid_argument("Raft directory must not be empty");
    std::unique_lock lock(mutex_);
    if (!segments_.empty()) throw std::runtime_error("Raft log is already open");

    try {
        std::filesystem::create_directories(directory);
        directory_ = directory;
        std::map<std::uint64_t, SegmentFiles> discovered;
        for (const auto& item : std::filesystem::directory_iterator(directory_)) {
            const std::string name = item.path().filename().string();
            if (!name.starts_with("segment-")) continue;
            const auto [base, is_store] = parse_segment_name(name);
            if (base == 0) throw std::runtime_error("Raft segment base index zero is invalid");
            auto& files = discovered[base];
            (is_store ? files.store : files.index) = true;
        }

        bool removed_orphan = false;
        for (auto it = discovered.begin(); it != discovered.end();) {
            if (!it->second.store) {
                std::filesystem::remove(
                    std::filesystem::path(directory_) / (segment_stem(it->first) + ".index"));
                it = discovered.erase(it);
                removed_orphan = true;
            } else {
                ++it;
            }
        }
        if (removed_orphan) disk::sync_directory(directory_);

        if (discovered.empty()) {
            create_segment(1);
        } else {
            std::uint64_t expected_base = 1;
            for (const auto& [base, files] : discovered) {
                if (base != expected_base) {
                    throw std::runtime_error("Raft segments do not form a continuous index sequence");
                }
                const auto stem = std::filesystem::path(directory_) / segment_stem(base);
                const int store_fd = disk::open_file(stem.string() + ".store", O_RDWR);
                int index_fd = -1;
                try {
                    index_fd = disk::open_file(
                        stem.string() + ".index", O_RDWR | O_CREAT, 0644);
                    segments_.push_back(std::make_unique<RaftSegment>(
                        base, store_fd, index_fd, config_));
                    if (!files.index) disk::sync_directory(directory_);
                } catch (...) {
                    if (index_fd == -1) disk::close_file(store_fd);
                    throw;
                }
                expected_base = segments_.back()->next_index();
            }
        }
        next_index_ = segments_.back()->next_index();
        if (next_index_ > 1) {
            last_term_ = 0;
            for (auto it = segments_.rbegin(); it != segments_.rend(); ++it) {
                if ((*it)->next_index() > (*it)->base_index()) {
                    last_term_ = (*it)->term_at((*it)->next_index() - 1);
                    break;
                }
            }
        } else {
            last_term_ = 0;
        }
        durable_index_ = next_index_ - 1;
        recovery_required_ = false;
        directory_dirty_ = false;
    } catch (...) {
        segments_.clear();
        directory_.clear();
        next_index_ = last_term_ = durable_index_ = 0;
        recovery_required_ = directory_dirty_ = false;
        throw;
    }
}

void RaftLog::close() {
    std::unique_lock lock(mutex_);
    wait_for_sync_to_finish(lock);
    if (segments_.empty()) return;
    if (recovery_required_) {
        throw std::runtime_error("Raft log must be reopened before close");
    }
    for (auto& segment : segments_) segment->sync();
    if (directory_dirty_) disk::sync_directory(directory_);
    segments_.clear();
    directory_.clear();
    next_index_ = last_term_ = durable_index_ = 0;
    directory_dirty_ = false;
}

bool RaftLog::is_open() const noexcept {
    std::shared_lock lock(mutex_);
    return !segments_.empty();
}

std::uint64_t RaftLog::append(
    std::uint64_t term,
    std::vector<MutationOp> operations) {
    std::unique_lock lock(mutex_);
    if (segments_.empty()) throw std::runtime_error("Raft log is not open");
    if (recovery_required_) throw std::runtime_error("Raft log must be reopened before appending");

    RaftMutationEntry entry{
        .term = term,
        .idx = next_index_,
        .operations = std::move(operations),
    };
    (void)RaftEntryCodec::encode(entry);
    if (segments_.back()->is_maxed()) create_segment(next_index_);
    try {
        segments_.back()->append(entry);
    } catch (...) {
        if (segments_.back()->recovery_required()) recovery_required_ = true;
        throw;
    }
    next_index_ += 1;
    last_term_ = entry.term;
    return entry.idx;
}

void RaftLog::append_from_leader(
    std::uint64_t first_index,
    std::span<const RaftMutationEntry> entries) {
    std::unique_lock lock(mutex_);
    if (segments_.empty()) throw std::runtime_error("Raft log is not open");
    if (recovery_required_) throw std::runtime_error("Raft log must be reopened before appending");
    if (first_index != next_index_) {
        throw std::invalid_argument("Leader batch must begin at the next Raft index");
    }

    for (std::size_t i = 0; i < entries.size(); ++i) {
        if (entries[i].idx != first_index + i) {
            throw std::invalid_argument("Leader batch indexes must be dense and match first_index");
        }
        (void)RaftEntryCodec::encode(entries[i]);
    }
    for (const RaftMutationEntry& entry : entries) {
        if (segments_.back()->is_maxed()) create_segment(next_index_);
        try {
            segments_.back()->append(entry);
        } catch (...) {
            if (segments_.back()->recovery_required()) recovery_required_ = true;
            throw;
        }
        next_index_ += 1;
        last_term_ = entry.term;
    }
}

RaftMutationEntry RaftLog::read(std::uint64_t index) const {
    std::shared_lock lock(mutex_);
    if (segments_.empty()) throw std::runtime_error("Raft log is not open");
    if (index == 0 || index >= next_index_) {
        throw std::out_of_range("Raft index is not present in log");
    }
    for (const auto& segment : segments_) {
        if (index >= segment->base_index() && index < segment->next_index()) {
            return segment->read(index);
        }
    }
    throw std::out_of_range("Raft index is not present in log");
}

std::vector<RaftMutationEntry> RaftLog::scan_from(std::uint64_t index) const {
    std::shared_lock lock(mutex_);
    if (segments_.empty()) throw std::runtime_error("Raft log is not open");
    if (index == 0 || index > next_index_) {
        throw std::out_of_range("Raft scan index is outside the log");
    }
    std::vector<RaftMutationEntry> entries;
    for (const auto& segment : segments_) {
        if (segment->next_index() <= index) continue;
        auto part = segment->scan();
        const auto first = part.begin() + static_cast<std::ptrdiff_t>(
            std::max(index, segment->base_index()) - segment->base_index());
        entries.insert(entries.end(),
            std::make_move_iterator(first), std::make_move_iterator(part.end()));
    }
    return entries;
}

std::uint64_t RaftLog::last_index() const noexcept {
    std::shared_lock lock(mutex_);
    return next_index_ == 0 ? 0 : next_index_ - 1;
}

std::uint64_t RaftLog::last_term() const noexcept {
    std::shared_lock lock(mutex_);
    return last_term_;
}

std::uint64_t RaftLog::term_at(std::uint64_t index) const {
    std::shared_lock lock(mutex_);
    if (segments_.empty()) throw std::runtime_error("Raft log is not open");
    if (index == 0 || index >= next_index_) return 0;
    for (const auto& segment : segments_) {
        if (index >= segment->base_index() && index < segment->next_index()) {
            return segment->term_at(index);
        }
    }
    return 0;
}

void RaftLog::truncate_suffix(std::uint64_t from_index) {
    std::unique_lock lock(mutex_);
    // An in-flight sync holds segment pointers this may erase, and would
    // otherwise mark the replaced suffix durable when it finishes.
    wait_for_sync_to_finish(lock);
    if (segments_.empty()) throw std::runtime_error("Raft log is not open");
    if (recovery_required_) throw std::runtime_error("Raft log must be reopened before truncating");
    if (from_index == 0 || from_index > next_index_) {
        throw std::out_of_range("Raft truncation boundary is outside the log");
    }
    if (from_index == next_index_) return;

    std::uint64_t retained_last_term = 0;
    if (from_index > 1) {
        const std::uint64_t retained_index = from_index - 1;
        for (const auto& segment : segments_) {
            if (retained_index >= segment->base_index() &&
                retained_index < segment->next_index()) {
                retained_last_term = segment->term_at(retained_index);
                break;
            }
        }
    }

    std::size_t target = segments_.size();
    for (std::size_t i = 0; i < segments_.size(); ++i) {
        if (from_index >= segments_[i]->base_index() &&
            from_index <= segments_[i]->next_index()) {
            target = i;
            break;
        }
    }
    if (target == segments_.size()) {
        throw std::runtime_error("Raft truncation boundary has no containing segment");
    }

    try {
        segments_[target]->truncate_suffix(from_index);
        std::vector<std::uint64_t> removed_bases;
        for (std::size_t i = target + 1; i < segments_.size(); ++i) {
            removed_bases.push_back(segments_[i]->base_index());
        }
        segments_.erase(segments_.begin() + static_cast<std::ptrdiff_t>(target + 1),
                        segments_.end());
        for (std::uint64_t base : removed_bases) {
            const auto stem = std::filesystem::path(directory_) / segment_stem(base);
            std::filesystem::remove(stem.string() + ".store");
            std::filesystem::remove(stem.string() + ".index");
            directory_dirty_ = true;
        }
    } catch (...) {
        recovery_required_ = true;
        throw;
    }
    next_index_ = from_index;
    last_term_ = retained_last_term;
    durable_index_ = std::min(durable_index_, from_index - 1);
}

void RaftLog::sync_through(std::uint64_t index) {
    std::unique_lock lock(mutex_);
    if (segments_.empty()) throw std::runtime_error("Raft log is not open");
    if (recovery_required_) throw std::runtime_error("Raft log must be reopened before synchronization");
    if (index == 0 || index >= next_index_) {
        throw std::out_of_range("Raft durability target is not present in log");
    }

    // Group commit: see Log::sync_through. A waiter re-validates after waking,
    // because a truncation may have run between the in-flight sync and now.
    while (true) {
        if (index <= durable_index_ && !directory_dirty_) return;
        if (!sync_in_progress_) break;
        sync_done_.wait(lock);
        if (segments_.empty()) throw std::runtime_error("Raft log was closed during synchronization");
        if (index >= next_index_) {
            throw std::out_of_range("Raft durability target was truncated during synchronization");
        }
    }

    const std::uint64_t sync_goal = next_index_ - 1;
    std::vector<RaftSegment*> to_sync;
    for (auto& segment : segments_) {
        if (segment->next_index() == segment->base_index()) continue;
        if (segment->next_index() - 1 <= durable_index_) continue;
        to_sync.push_back(segment.get());
    }
    const bool sync_directory = directory_dirty_;
    const std::string directory = directory_;
    directory_dirty_ = false;
    sync_in_progress_ = true;
    lock.unlock();

    try {
        for (RaftSegment* segment : to_sync) segment->sync();
        if (sync_directory) disk::sync_directory(directory);
    } catch (...) {
        lock.lock();
        if (sync_directory) directory_dirty_ = true;
        sync_in_progress_ = false;
        sync_done_.notify_all();
        throw;
    }

    lock.lock();
    // No truncation ran meanwhile - it waits for this sync - so every index up
    // to sync_goal is still the entry that was just made durable.
    durable_index_ = std::max(durable_index_, sync_goal);
    sync_in_progress_ = false;
    sync_done_.notify_all();
}

void RaftLog::wait_for_sync_to_finish(std::unique_lock<std::shared_mutex>& lock) {
    sync_done_.wait(lock, [this] { return !sync_in_progress_; });
}

std::uint64_t RaftLog::durable_index() const noexcept {
    std::shared_lock lock(mutex_);
    return durable_index_;
}

void RaftLog::create_segment(std::uint64_t base_index) {
    const auto stem = std::filesystem::path(directory_) / segment_stem(base_index);
    const int store_fd = disk::open_file(
        stem.string() + ".store", O_RDWR | O_CREAT | O_EXCL, 0644);
    int index_fd = -1;
    try {
        index_fd = disk::open_file(
            stem.string() + ".index", O_RDWR | O_CREAT | O_EXCL, 0644);
        disk::sync_directory(directory_);
        segments_.push_back(std::make_unique<RaftSegment>(
            base_index, store_fd, index_fd, config_));
    } catch (...) {
        if (index_fd == -1) disk::close_file(store_fd);
        throw;
    }
}
