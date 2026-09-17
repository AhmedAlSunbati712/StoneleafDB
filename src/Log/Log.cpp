#include <Log/Log.h>

#include <DiskIO.h>
#include <Log/WalRecordCodec.h>

#include <fcntl.h>
#include <algorithm>
#include <filesystem>
#include <iomanip>
#include <map>
#include <mutex>
#include <sstream>
#include <stdexcept>

namespace {

struct SegmentFiles { bool store = false; bool index = false; };

std::string segment_stem(Lsn base_lsn) {
    std::ostringstream name;
    name << "segment-" << std::setw(20) << std::setfill('0') << base_lsn;
    return name.str();
}

std::pair<Lsn, bool> parse_segment_name(const std::string& name) {
    constexpr std::size_t prefix_size = 8;
    constexpr std::size_t digits_size = 20;
    const bool store = name.size() == prefix_size + digits_size + 6 && name.ends_with(".store");
    const bool index = name.size() == prefix_size + digits_size + 6 && name.ends_with(".index");
    if (!name.starts_with("segment-")) return {0, false};
    if (!store && !index) throw std::runtime_error("Malformed WAL segment filename");
    const auto digits = name.substr(prefix_size, digits_size);
    if (digits.find_first_not_of("0123456789") != std::string::npos) throw std::runtime_error("Malformed WAL segment filename");
    try { return {std::stoull(digits), store}; }
    catch (...) { throw std::runtime_error("Malformed WAL segment filename"); }
}

} // namespace

Log::Log(Config config) : config_(config) { config_.validate(); }

Log::~Log() noexcept {
    std::unique_lock lock(mutex_);
    wait_for_sync_to_finish(lock);
    segments_.clear();
}

void Log::open(const std::string& directory) {
    if (directory.empty()) throw std::invalid_argument("WAL directory must not be empty");
    std::unique_lock lock(mutex_);
    if (!segments_.empty()) throw std::runtime_error("Log is already open");

    try {
        std::filesystem::create_directories(directory);
        directory_ = directory;
        std::map<Lsn, SegmentFiles> discovered;
        for (const auto& entry : std::filesystem::directory_iterator(directory_)) {
            const auto name = entry.path().filename().string();
            if (!name.starts_with("segment-")) continue;
            const auto [base, is_store] = parse_segment_name(name);
            if (base == 0) throw std::runtime_error("Segment base LSN zero is invalid");
            auto& files = discovered[base];
            (is_store ? files.store : files.index) = true;
        }

        if (discovered.empty()) {
            create_segment(config_.initial_lsn);
        } else {
            Lsn expected_base = config_.initial_lsn;
            for (const auto& [base, files] : discovered) {
                if (base != expected_base) throw std::runtime_error("WAL segments do not form a continuous LSN sequence");
                if (!files.store) throw std::runtime_error("WAL Index exists without its authoritative Store");
                const auto stem = std::filesystem::path(directory_) / segment_stem(base);
                const int store_fd = disk::open_file(stem.string() + ".store", O_RDWR);
                int index_fd = -1;
                try {
                    index_fd = disk::open_file(stem.string() + ".index", O_RDWR | O_CREAT, 0644);
                    segments_.push_back(std::make_unique<Segment>(base, store_fd, index_fd, config_));
                    if (!files.index) disk::sync_directory(directory_);
                } catch (...) {
                    if (index_fd == -1) disk::close_file(store_fd);
                    throw;
                }
                expected_base = segments_.back()->next_lsn();
            }
        }
        next_lsn_ = segments_.back()->next_lsn();
        durable_lsn_ = next_lsn_ - 1;
        recovery_required_ = false;
    } catch (...) {
        segments_.clear(); directory_.clear(); next_lsn_ = durable_lsn_ = 0;
        throw;
    }
}

void Log::close() {
    std::unique_lock lock(mutex_);
    // An in-flight sync holds raw pointers into segments_.
    wait_for_sync_to_finish(lock);
    if (segments_.empty()) return;
    if (recovery_required_) throw std::runtime_error("Log must be reopened and recovered before close can synchronize");
    for (auto& segment : segments_) segment->sync();
    durable_lsn_ = next_lsn_ - 1;
    segments_.clear(); directory_.clear(); next_lsn_ = durable_lsn_ = 0;
}

bool Log::is_open() const noexcept { std::shared_lock lock(mutex_); return !segments_.empty(); }

Lsn Log::append(PendingWalRecord pending) {
    std::unique_lock lock(mutex_);
    if (segments_.empty()) throw std::runtime_error("Log is not open");
    if (recovery_required_) throw std::runtime_error("Log must be reopened and recovered before appending");

    WalRecord record{.lsn = next_lsn_, .type = pending.type,
                     .transaction_id = pending.transaction_id,
                     .prev_lsn = pending.prev_lsn, .data = std::move(pending.data)};

    // Validate before rollover so rejected caller input cannot create an empty
    // segment or otherwise mutate the logger.
    (void)WalRecordCodec::encode(record);

    // A complete record that crosses a limit remains in its original segment.
    // Rollover happens before the following append.
    if (segments_.back()->is_maxed()) create_segment(next_lsn_);
    try {
        segments_.back()->append(record);
    } catch (...) {
        // Only a failure after physical append begins is ambiguous. Invalid
        // input leaves the Segment writable and must not poison the Log.
        if (segments_.back()->recovery_required()) recovery_required_ = true;
        throw;
    }
    next_lsn_ += 1;
    return record.lsn;
}

WalRecord Log::read(Lsn lsn) const {
    std::shared_lock lock(mutex_);
    if (segments_.empty()) throw std::runtime_error("Log is not open");
    if (lsn < config_.initial_lsn || lsn >= next_lsn_) throw std::out_of_range("LSN is not present in Log");
    for (const auto& segment : segments_) {
        if (lsn >= segment->base_lsn() && lsn < segment->next_lsn()) return segment->read(lsn);
    }
    throw std::out_of_range("LSN is not present in Log");
}

std::vector<WalRecord> Log::scan() const {
    std::shared_lock lock(mutex_);
    if (segments_.empty()) throw std::runtime_error("Log is not open");
    std::vector<WalRecord> records;
    for (const auto& segment : segments_) {
        auto part = segment->scan();
        records.insert(records.end(), std::make_move_iterator(part.begin()), std::make_move_iterator(part.end()));
    }
    return records;
}
void Log::sync_through(Lsn target_lsn) {
    std::unique_lock lock(mutex_);
    if (segments_.empty()) throw std::runtime_error("Log is not open");
    if (recovery_required_) throw std::runtime_error("Log must be reopened and recovered before synchronization");
    if (target_lsn < config_.initial_lsn || target_lsn >= next_lsn_) throw std::out_of_range("Target LSN is not present in Log");

    // Group commit. Only one thread fsyncs at a time. Everyone else waits for
    // it and then re-checks: a sync that started after their record was
    // appended covers it, so under concurrency most callers never fsync.
    while (true) {
        if (target_lsn <= durable_lsn_) return;
        if (!sync_in_progress_) break;
        sync_done_.wait(lock);
        if (segments_.empty()) throw std::runtime_error("Log was closed during synchronization");
    }

    // Take everything appended so far, not just target_lsn: the extra records
    // cost nothing in the same fsync and spare their writers a sync of their own.
    const Lsn sync_goal = next_lsn_ - 1;
    std::vector<Segment*> to_sync;
    for (auto& segment : segments_) {
        if (segment->next_lsn() == segment->base_lsn()) continue;
        if (segment->next_lsn() - 1 <= durable_lsn_) continue;
        to_sync.push_back(segment.get());
    }
    sync_in_progress_ = true;
    lock.unlock();

    // Appends continue while the disk works. Segments are only destroyed by
    // close(), which waits for sync_in_progress_ to clear, so these pointers
    // stay valid. Older segments are synced first.
    try {
        // Store only: a synced record's Index entry is rebuilt by recovery if
        // it is lost, so the Index costs a full sync here for nothing.
        for (Segment* segment : to_sync) segment->sync_store();
    } catch (...) {
        lock.lock();
        sync_in_progress_ = false;
        sync_done_.notify_all();
        throw;
    }

    lock.lock();
    durable_lsn_ = std::max(durable_lsn_, sync_goal);
    sync_in_progress_ = false;
    sync_done_.notify_all();
}

void Log::wait_for_sync_to_finish(std::unique_lock<std::shared_mutex>& lock) {
    sync_done_.wait(lock, [this] { return !sync_in_progress_; });
}
Lsn Log::next_lsn() const noexcept { std::shared_lock lock(mutex_); return next_lsn_; }
Lsn Log::durable_lsn() const noexcept { std::shared_lock lock(mutex_); return durable_lsn_; }
Lsn Log::base_segment_offest() const noexcept { return static_cast<Lsn>(config_.initial_lsn); }
void Log::set_initial_lsn(Lsn initial_lsn) noexcept { config_.initial_lsn = initial_lsn; }
bool Log::recovery_required() const noexcept { std::shared_lock lock(mutex_); return recovery_required_; }

void Log::create_segment(Lsn base_lsn) {
    const auto stem = std::filesystem::path(directory_) / segment_stem(base_lsn);
    const int store_fd = disk::open_file(stem.string() + ".store", O_RDWR | O_CREAT | O_EXCL, 0644);
    int index_fd = -1;
    try {
        index_fd = disk::open_file(stem.string() + ".index", O_RDWR | O_CREAT | O_EXCL, 0644);
        disk::sync_directory(directory_);
        segments_.push_back(std::make_unique<Segment>(base_lsn, store_fd, index_fd, config_));
    } catch (...) {
        if (index_fd == -1) disk::close_file(store_fd);
        throw;
    }
}
