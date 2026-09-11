#include <gtest/gtest.h>

#include <Raft/RaftLog.h>

#include <filesystem>
#include <fstream>
#include <span>

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

} // namespace
