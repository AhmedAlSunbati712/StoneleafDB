#include <gtest/gtest.h>

#include <Raft/RaftLog.h>

#include <atomic>
#include <filesystem>
#include <fstream>
#include <span>
#include <thread>

namespace {

class TempDir {
public:
    TempDir() {
        path = std::filesystem::temp_directory_path() / "stoneleaf-raft-log-XXXXXX";
        std::string value = path.string();
        value.push_back('\0');
        path = ::mkdtemp(value.data());
    }
    ~TempDir() { std::filesystem::remove_all(path); }
    std::filesystem::path path;
};

Config config(std::uint64_t max_store_bytes = 8192) {
    return {.max_index_bytes = 120, .max_store_bytes = max_store_bytes, .initial_lsn = 1};
}

RaftMutationEntry entry(std::uint64_t term, std::uint64_t index) {
    return {.term = term, .idx = index, .operations = {}};
}

TEST(RaftLogTest, CreatesDirectoryAndAssignsDenseIndexes) {
    TempDir parent;
    const auto directory = parent.path / "database.raft";
    RaftLog log(config());
    log.open(directory.string());

    EXPECT_TRUE(log.is_open());
    EXPECT_EQ(log.last_index(), 0u);
    EXPECT_EQ(log.last_term(), 0u);
    EXPECT_EQ(log.term_at(0), 0u);
    EXPECT_EQ(log.append(2, {}), 1u);
    EXPECT_EQ(log.append(2, {}), 2u);
    EXPECT_EQ(log.last_index(), 2u);
    EXPECT_EQ(log.last_term(), 2u);
    EXPECT_EQ(log.term_at(3), 0u);
    EXPECT_EQ(log.durable_index(), 0u);
}

TEST(RaftLogTest, AppendsValidatedLeaderBatchAndScansFromIndex) {
    TempDir dir;
    RaftLog log(config());
    log.open(dir.path.string());
    const std::vector<RaftMutationEntry> batch{entry(3, 1), entry(3, 2), entry(4, 3)};
    log.append_from_leader(1, batch);

    const auto suffix = log.scan_from(2);
    ASSERT_EQ(suffix.size(), 2u);
    EXPECT_EQ(suffix[0].idx, 2u);
    EXPECT_EQ(suffix[1].term, 4u);
    EXPECT_TRUE(log.scan_from(4).empty());

    const std::vector<RaftMutationEntry> gap{entry(4, 4), entry(4, 6)};
    EXPECT_THROW(log.append_from_leader(4, gap), std::invalid_argument);
    EXPECT_EQ(log.last_index(), 3u);
}

TEST(RaftLogTest, ReadRangeReturnsAtMostMaxCountEntriesAcrossSegments) {
    // max_index_bytes = 120 fits few entries per segment, so a range of ten
    // crosses several segments.
    TempDir dir;
    RaftLog log(config());
    log.open(dir.path.string());
    for (std::uint64_t i = 1; i <= 30; ++i) log.append(i, {});

    const auto window = log.read_range(5, 10);
    ASSERT_EQ(window.size(), 10u);
    for (std::size_t k = 0; k < window.size(); ++k) {
        EXPECT_EQ(window[k].idx, 5 + k);
        EXPECT_EQ(window[k].term, 5 + k);
    }

    const auto tail = log.read_range(25, 100);
    ASSERT_EQ(tail.size(), 6u);
    EXPECT_EQ(tail.back().idx, 30u);

    EXPECT_TRUE(log.read_range(31, 10).empty());
    EXPECT_TRUE(log.read_range(1, 0).empty());
    EXPECT_THROW(log.read_range(0, 1), std::out_of_range);
    EXPECT_THROW(log.read_range(32, 1), std::out_of_range);
}

TEST(RaftLogTest, TruncatesWithinSegmentAndPersistsReplacement) {
    TempDir dir;
    {
        RaftLog log(config());
        log.open(dir.path.string());
        log.append_from_leader(1, std::vector<RaftMutationEntry>{
            entry(1, 1), entry(1, 2), entry(1, 3)});
        log.sync_through(3);
        log.truncate_suffix(2);
        EXPECT_EQ(log.durable_index(), 1u);
        EXPECT_EQ(log.last_term(), 1u);
        EXPECT_EQ(log.term_at(2), 0u);
        log.append_from_leader(2, std::vector<RaftMutationEntry>{entry(2, 2)});
        EXPECT_EQ(log.last_term(), 2u);
        log.sync_through(2);
        EXPECT_EQ(log.durable_index(), 2u);
        log.close();
    }

    RaftLog reopened(config());
    reopened.open(dir.path.string());
    EXPECT_EQ(reopened.last_index(), 2u);
    EXPECT_EQ(reopened.term_at(2), 2u);
}

TEST(RaftLogTest, TruncatesAcrossSegmentsWithoutResurrectingTail) {
    TempDir dir;
    {
        RaftLog log(config(1));
        log.open(dir.path.string());
        log.append(1, {});
        log.append(1, {});
        log.append(1, {});
        ASSERT_TRUE(std::filesystem::exists(
            dir.path / "segment-00000000000000000002.store"));
        log.sync_through(3);
        log.truncate_suffix(2);
        log.append(2, {});
        log.sync_through(2);
        log.close();
    }

    RaftLog reopened(config(1));
    reopened.open(dir.path.string());
    EXPECT_EQ(reopened.last_index(), 2u);
    EXPECT_EQ(reopened.term_at(1), 1u);
    EXPECT_EQ(reopened.term_at(2), 2u);
    EXPECT_EQ(reopened.scan_from(1).size(), 2u);
}

TEST(RaftLogTest, ReopensWithCrashCreatedEmptyTrailingSegment) {
    TempDir dir;
    {
        RaftLog log(config());
        log.open(dir.path.string());
        log.append(6, {});
        log.sync_through(1);
        log.close();
    }
    std::ofstream(dir.path / "segment-00000000000000000002.store");
    std::ofstream(dir.path / "segment-00000000000000000002.index");

    RaftLog reopened(config());
    reopened.open(dir.path.string());
    EXPECT_EQ(reopened.last_index(), 1u);
    EXPECT_EQ(reopened.last_term(), 6u);
    EXPECT_EQ(reopened.append(8, {}), 2u);
}

TEST(RaftLogTest, RemovesOrphanDerivedIndexDuringOpen) {
    TempDir dir;
    std::ofstream(dir.path / "segment-00000000000000000001.index");

    RaftLog log(config());
    EXPECT_NO_THROW(log.open(dir.path.string()));
    EXPECT_EQ(log.last_index(), 0u);
    EXPECT_TRUE(std::filesystem::exists(
        dir.path / "segment-00000000000000000001.store"));
}

TEST(RaftLogTest, ValidatesPublicBoundaries) {
    TempDir dir;
    Config wrong = config();
    wrong.initial_lsn = 2;
    EXPECT_THROW((void)RaftLog{wrong}, std::invalid_argument);

    RaftLog log(config());
    log.open(dir.path.string());
    EXPECT_THROW(log.read(0), std::out_of_range);
    EXPECT_THROW(log.scan_from(0), std::out_of_range);
    EXPECT_THROW(log.truncate_suffix(0), std::out_of_range);
    EXPECT_THROW(log.sync_through(1), std::out_of_range);
    EXPECT_NO_THROW(log.truncate_suffix(1));
}

TEST(RaftLogTest, ConcurrentAppendsRemainUniqueAndDense) {
    TempDir dir;
    RaftLog log(config());
    log.open(dir.path.string());
    constexpr int thread_count = 6;
    constexpr int entries_per_thread = 20;
    std::vector<std::thread> threads;
    for (int i = 0; i < thread_count; ++i) {
        threads.emplace_back([&log] {
            for (int j = 0; j < entries_per_thread; ++j) log.append(9, {});
        });
    }
    for (auto& thread : threads) thread.join();

    const auto entries = log.scan_from(1);
    ASSERT_EQ(entries.size(), thread_count * entries_per_thread);
    for (std::size_t i = 0; i < entries.size(); ++i) {
        EXPECT_EQ(entries[i].idx, i + 1);
        EXPECT_EQ(entries[i].term, 9u);
    }
}

TEST(RaftLogTest, ConcurrentSyncsEachReturnOnlyOnceTheirEntryIsDurable) {
    TempDir dir;
    RaftLog log(config(512));
    log.open(dir.path.string());
    constexpr int thread_count = 6;
    constexpr int entries_per_thread = 20;
    std::atomic<int> violations{0};
    std::vector<std::thread> threads;
    for (int i = 0; i < thread_count; ++i) {
        threads.emplace_back([&] {
            for (int j = 0; j < entries_per_thread; ++j) {
                const std::uint64_t index = log.append(9, {});
                log.sync_through(index);
                if (log.durable_index() < index) violations++;
            }
        });
    }
    for (auto& thread : threads) thread.join();
    EXPECT_EQ(violations.load(), 0);
    EXPECT_EQ(log.durable_index(), static_cast<std::uint64_t>(thread_count * entries_per_thread));
}

TEST(RaftLogTest, TruncationDuringSyncsNeverLeavesDurabilityPastTheLog) {
    // A follower truncates while syncs are in flight on another thread. Only
    // this thread truncates and appends, so right after each truncation
    // durable_index must not cover the removed suffix - an in-flight sync
    // finishing late must not restore it.
    TempDir dir;
    RaftLog log(config(512));
    log.open(dir.path.string());
    for (std::uint64_t i = 1; i <= 50; ++i) log.append(1, {});

    std::atomic<bool> stop{false};
    std::atomic<int> violations{0};
    std::thread syncer([&] {
        while (!stop.load()) {
            const std::uint64_t last = log.last_index();
            if (last == 0) continue;
            try {
                log.sync_through(last);
            } catch (const std::out_of_range&) {
                // Truncated away while waiting: expected.
            }
        }
    });

    for (int round = 0; round < 20; ++round) {
        log.truncate_suffix(20);
        if (log.durable_index() > 19) violations++;
        for (std::uint64_t i = 20; i <= 50; ++i) log.append(static_cast<std::uint64_t>(round + 2), {});
    }
    stop = true;
    syncer.join();

    EXPECT_EQ(violations.load(), 0);
    log.sync_through(log.last_index());
    EXPECT_EQ(log.durable_index(), log.last_index());
}

} // namespace
