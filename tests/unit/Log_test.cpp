#include <gtest/gtest.h>

#include <atomic>

#include <Log/Log.h>

#include <filesystem>
#include <fstream>
#include <thread>

namespace {
class TempDir {
public:
    TempDir() { path = std::filesystem::temp_directory_path() / std::filesystem::path("stoneleaf-log-XXXXXX");
        std::string value = path.string(); value.push_back('\0'); path = ::mkdtemp(value.data()); }
    ~TempDir() { std::filesystem::remove_all(path); }
    std::filesystem::path path;
};
Config config() { return {.max_index_bytes = 120, .max_store_bytes = 8192, .initial_lsn = 1}; }
PendingWalRecord system(std::vector<char> data = {}) {
    return {.type = WalRecordType::SystemAction, .data = std::move(data)};
}

TEST(LogTest, CreatesDirectoryAndInitialSegment) {
    TempDir parent; auto directory = parent.path / "wal";
    Log log(config()); log.open(directory.string());
    EXPECT_TRUE(log.is_open());
    EXPECT_TRUE(std::filesystem::exists(directory / "segment-00000000000000000001.store"));
    EXPECT_TRUE(std::filesystem::exists(directory / "segment-00000000000000000001.index"));
    log.close(); EXPECT_FALSE(log.is_open()); EXPECT_NO_THROW(log.close());
}

TEST(LogTest, RebuildsMissingIndexAndRejectsMissingStore) {
    TempDir dir;
    { std::ofstream(dir.path / "segment-00000000000000000001.store"); }
    Log recovered(config()); EXPECT_NO_THROW(recovered.open(dir.path.string())); recovered.close();
    std::filesystem::remove(dir.path / "segment-00000000000000000001.store");
    Log invalid(config()); EXPECT_THROW(invalid.open(dir.path.string()), std::runtime_error);
}

TEST(LogTest, RejectsMalformedNamesAndSegmentGaps) {
    TempDir malformed; { std::ofstream(malformed.path / "segment-nope.store"); }
    Log first(config()); EXPECT_THROW(first.open(malformed.path.string()), std::runtime_error);
    TempDir gap;
    { std::ofstream(gap.path / "segment-00000000000000000001.store"); std::ofstream(gap.path / "segment-00000000000000000001.index");
      std::ofstream(gap.path / "segment-00000000000000000003.store"); std::ofstream(gap.path / "segment-00000000000000000003.index"); }
    Log second(config()); EXPECT_THROW(second.open(gap.path.string()), std::runtime_error);
}

TEST(LogTest, AssignsDenseLsnsAndSupportsReadScanAndReopen) {
    TempDir dir;
    { Log log(config()); log.open(dir.path.string());
      EXPECT_EQ(log.append(system({'a'})), 1u); EXPECT_EQ(log.append(system({'b'})), 2u);
      EXPECT_EQ(log.read(2).data, (std::vector<char>{'b'}));
      auto records = log.scan(); ASSERT_EQ(records.size(), 2u); EXPECT_EQ(records[0].lsn, 1u); log.close(); }
    Log reopened(config()); reopened.open(dir.path.string());
    EXPECT_EQ(reopened.next_lsn(), 3u); EXPECT_EQ(reopened.append(system({'c'})), 3u);
}

TEST(LogTest, RollsBeforeAppendAfterARecordCrossesLimit) {
    TempDir dir; Config small = config(); small.max_store_bytes = 44;
    Log log(small); log.open(dir.path.string());
    EXPECT_EQ(log.append(system({'a'})), 1u);
    EXPECT_EQ(log.append(system({'b'})), 2u);
    EXPECT_TRUE(std::filesystem::exists(dir.path / "segment-00000000000000000002.store"));
    auto records = log.scan(); ASSERT_EQ(records.size(), 2u); EXPECT_EQ(records[1].lsn, 2u);
}

TEST(LogTest, SyncThroughMakesEverythingAppendedDurableAcrossSegments) {
    // One record per segment. A sync for the first also takes the second:
    // it is already written, and the same round of fsyncs covers it.
    TempDir dir; Config small = config(); small.max_store_bytes = 44;
    Log log(small); log.open(dir.path.string());
    log.append(system({'a'})); log.append(system({'b'}));
    EXPECT_EQ(log.durable_lsn(), 0u);
    log.sync_through(1); EXPECT_EQ(log.durable_lsn(), 2u);
    EXPECT_NO_THROW(log.sync_through(2)); EXPECT_EQ(log.durable_lsn(), 2u);
    EXPECT_THROW(log.sync_through(3), std::out_of_range);
}

TEST(LogTest, SyncedRecordsSurviveLosingEveryUnsyncedIndexBlock) {
    // sync_through makes the Store durable, not the Index. Model the worst a
    // crash can do to the Index - every block read back as zeros - and check
    // reopening rebuilds it and loses no synced record.
    TempDir dir; Log log(config()); log.open(dir.path.string());
    for (char c = 'a'; c < 'a' + 20; ++c) log.append(system({c}));
    log.sync_through(20);
    const auto index_path = dir.path / "segment-00000000000000000001.index";
    const auto index_size = std::filesystem::file_size(index_path);
    // Abandon without close(), which would sync the Index too.
    { std::ofstream zeroed(index_path, std::ios::binary | std::ios::trunc);
      zeroed.write(std::string(index_size, '\0').data(), static_cast<std::streamsize>(index_size)); }

    Log reopened(config()); reopened.open(dir.path.string());
    ASSERT_EQ(reopened.next_lsn(), 21u);
    EXPECT_EQ(reopened.read(1).data, std::vector<char>{'a'});
    EXPECT_EQ(reopened.read(20).data, std::vector<char>{static_cast<char>('a' + 19)});
}

TEST(LogTest, ConcurrentSyncsEachReturnOnlyOnceTheirRecordIsDurable) {
    // Group commit: one thread fsyncs while the rest wait and then find their
    // record covered. None may return before its own LSN is durable.
    TempDir dir; Config small = config(); small.max_store_bytes = 512;
    Log log(small); log.open(dir.path.string());
    constexpr int thread_count = 8; constexpr int records_per_thread = 25;
    std::atomic<int> violations{0};
    std::vector<std::thread> threads;
    for (int i = 0; i < thread_count; ++i) {
        threads.emplace_back([&] {
            for (int j = 0; j < records_per_thread; ++j) {
                const Lsn lsn = log.append(system({'x'}));
                log.sync_through(lsn);
                if (log.durable_lsn() < lsn) violations++;
            }
        });
    }
    for (auto& thread : threads) thread.join();
    EXPECT_EQ(violations.load(), 0);
    EXPECT_EQ(log.durable_lsn(), static_cast<Lsn>(thread_count * records_per_thread));
    log.close();

    Log reopened(small); reopened.open(dir.path.string());
    EXPECT_EQ(reopened.scan().size(), static_cast<std::size_t>(thread_count * records_per_thread));
}

TEST(LogTest, ConcurrentAppendsRemainUniqueAndDense) {
    TempDir dir; Log log(config()); log.open(dir.path.string());
    constexpr int thread_count = 8; constexpr int records_per_thread = 50;
    std::vector<std::thread> threads;
    for (int i = 0; i < thread_count; ++i) {
        threads.emplace_back([&] { for (int j = 0; j < records_per_thread; ++j) log.append(system()); });
    }
    for (auto& thread : threads) thread.join();
    auto records = log.scan(); ASSERT_EQ(records.size(), thread_count * records_per_thread);
    for (std::size_t i = 0; i < records.size(); ++i) EXPECT_EQ(records[i].lsn, i + 1);
}

TEST(LogTest, InvalidAppendDoesNotRollOrConsumeLsn) {
    TempDir dir; Config small = config(); small.max_store_bytes = 40;
    Log log(small); log.open(dir.path.string());
    EXPECT_EQ(log.append(system()), 1u);
    EXPECT_THROW(log.append({.type = WalRecordType::TxnCommit, .transaction_id = 1}), std::invalid_argument);
    EXPECT_EQ(log.next_lsn(), 2u);
    EXPECT_FALSE(std::filesystem::exists(dir.path / "segment-00000000000000000002.store"));
}
} // namespace
