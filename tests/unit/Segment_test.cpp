#include <gtest/gtest.h>

#include <DiskIO.h>
#include <Log/Segment.h>
#include <Log/Store.h>
#include <Log/WalRecordCodec.h>
#include <storage/Index.h>

#include <chrono>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <fcntl.h>
#include <span>
#include <string>
#include <utility>
#include <vector>

namespace {

class TempSegmentFiles {
    public:
        TempSegmentFiles() {
            const auto suffix = std::to_string(
                std::chrono::steady_clock::now().time_since_epoch().count());
            const std::filesystem::path temp = std::filesystem::temp_directory_path();
            store_path = temp / ("stoneleafdb_segment_test_" + suffix + ".store");
            index_path = temp / ("stoneleafdb_segment_test_" + suffix + ".index");
        }

        ~TempSegmentFiles() {
            std::error_code ec;
            std::filesystem::remove(store_path, ec);
            std::filesystem::remove(index_path, ec);
        }

        int open_store() const {
            return disk::open_file(store_path.string(), O_RDWR | O_CREAT, 0644);
        }

        int open_index() const {
            return disk::open_file(index_path.string(), O_RDWR | O_CREAT, 0644);
        }

        void append_store_bytes(std::span<const char> bytes) const {
            std::ofstream file(store_path, std::ios::binary | std::ios::app);
            file.write(bytes.data(), static_cast<std::streamsize>(bytes.size()));
        }

        void append_index_bytes(std::span<const char> bytes) const {
            std::ofstream file(index_path, std::ios::binary | std::ios::app);
            file.write(bytes.data(), static_cast<std::streamsize>(bytes.size()));
        }

        std::filesystem::path store_path;
        std::filesystem::path index_path;
};

Config config(
    std::uint64_t max_index_bytes = 120,
    std::uint64_t max_store_bytes = 4096) {
    return Config{
        .max_index_bytes = max_index_bytes,
        .max_store_bytes = max_store_bytes,
        .initial_lsn = 1,
    };
}

std::vector<std::uint64_t> write_store_records(
    const TempSegmentFiles &files,
    const std::vector<WalRecord> &records) {
    std::vector<std::uint64_t> offsets;
    Store store(files.open_store());
    for (const WalRecord &record : records) {
        offsets.push_back(store.append(WalRecordCodec::encode(record)));
    }
    return offsets;
}

void write_index_entries(
    const TempSegmentFiles &files,
    const std::vector<std::pair<std::uint32_t, std::uint64_t>> &entries) {
    Index index(files.open_index());
    for (const auto &[ordinal, store_offset] : entries) {
        index.append(ordinal, store_offset);
    }
}

TEST(SegmentTest, AppendReadAndScanUseAbsoluteLsns) {
    TempSegmentFiles files;
    Segment segment(100, files.open_store(), files.open_index(), config());
    const WalRecord first{.lsn = 100, .data = {'a'}};
    const WalRecord second{.lsn = 101, .data = {'b', 'c'}};

    segment.append(first);
    segment.append(second);

    EXPECT_EQ(segment.base_lsn(), 100u);
    EXPECT_EQ(segment.next_lsn(), 102u);
    EXPECT_EQ(segment.read(100).data, first.data);
    EXPECT_EQ(segment.read(101).data, second.data);
    const std::vector<WalRecord> records = segment.scan();
    ASSERT_EQ(records.size(), 2u);
    EXPECT_EQ(records[0].lsn, 100u);
    EXPECT_EQ(records[1].lsn, 101u);
}

TEST(SegmentTest, AppendRequiresTheNextDenseAbsoluteLsn) {
    TempSegmentFiles files;
    Segment segment(10, files.open_store(), files.open_index(), config());

    EXPECT_THROW(segment.append(WalRecord{.lsn = 11, .data = {}}), std::invalid_argument);
    EXPECT_NO_THROW(segment.append(WalRecord{.lsn = 10, .data = {}}));
    EXPECT_THROW(segment.append(WalRecord{.lsn = 10, .data = {}}), std::invalid_argument);
    EXPECT_THROW(segment.append(WalRecord{.lsn = 12, .data = {}}), std::invalid_argument);
}

TEST(SegmentTest, ReopenReconstructsNextLsnAndPreservesLookup) {
    TempSegmentFiles files;
    {
        Segment segment(50, files.open_store(), files.open_index(), config());
        segment.append(WalRecord{.lsn = 50, .data = {'x'}});
        segment.append(WalRecord{.lsn = 51, .data = {'y'}});
    }

    Segment reopened(50, files.open_store(), files.open_index(), config());

    EXPECT_EQ(reopened.next_lsn(), 52u);
    EXPECT_EQ(reopened.read(50).data, std::vector<char>({'x'}));
    EXPECT_EQ(reopened.read(51).data, std::vector<char>({'y'}));
}

TEST(SegmentTest, RecoveryAppendsOnlyMissingIndexSuffix) {
    TempSegmentFiles files;
    const std::vector<WalRecord> records{
        WalRecord{.lsn = 20, .data = {'a'}},
        WalRecord{.lsn = 21, .data = {'b', 'c'}},
    };
    const std::vector<std::uint64_t> offsets = write_store_records(files, records);
    write_index_entries(files, {{0, offsets[0]}});

    {
        Segment segment(20, files.open_store(), files.open_index(), config());
        EXPECT_EQ(segment.next_lsn(), 22u);
        EXPECT_EQ(segment.read(21).data, records[1].data);
    }

    Index recovered(files.open_index());
    EXPECT_EQ(recovered.scan().entry_count, 2u);
    EXPECT_EQ(recovered.read(0), offsets[0]);
    EXPECT_EQ(recovered.read(1), offsets[1]);
}

TEST(SegmentTest, RecoveryTruncatesPartialIndexEntryAndRepairsSuffix) {
    TempSegmentFiles files;
    const std::vector<WalRecord> records{
        WalRecord{.lsn = 30, .data = {'a'}},
        WalRecord{.lsn = 31, .data = {'b'}},
    };
    const std::vector<std::uint64_t> offsets = write_store_records(files, records);
    write_index_entries(files, {{0, offsets[0]}});
    const std::vector<char> partial{0x00, 0x00, 0x00};
    files.append_index_bytes(partial);

    Segment segment(30, files.open_store(), files.open_index(), config());

    EXPECT_EQ(segment.next_lsn(), 32u);
    EXPECT_EQ(segment.read(31).data, records[1].data);
}

TEST(SegmentTest, RecoveryRemovesIndexEntriesBeyondStore) {
    TempSegmentFiles files;
    const std::vector<std::uint64_t> offsets = write_store_records(
        files,
        {WalRecord{.lsn = 40, .data = {'a'}}});
    write_index_entries(files, {{0, offsets[0]}, {1, 999}});

    {
        Segment segment(40, files.open_store(), files.open_index(), config());
        EXPECT_EQ(segment.next_lsn(), 41u);
    }

    Index recovered(files.open_index());
    EXPECT_EQ(recovered.scan().entry_count, 1u);
    EXPECT_EQ(recovered.read(0), offsets[0]);
}

TEST(SegmentTest, RecoveryRebuildsIndexEntryWithWrongStoreOffset) {
    // The Index is never synced on the commit path, so after a crash any of it
    // may be stale. Store is synced and authoritative: recovery keeps only the
    // Index entries that match it and rebuilds the rest.
    TempSegmentFiles files;
    const std::vector<WalRecord> records{
        WalRecord{.lsn = 60, .data = {'a'}},
        WalRecord{.lsn = 61, .data = {'b'}},
    };
    const std::vector<std::uint64_t> offsets = write_store_records(files, records);
    write_index_entries(files, {{0, offsets[0]}, {1, offsets[0]}});

    {
        Segment segment(60, files.open_store(), files.open_index(), config());
        EXPECT_EQ(segment.next_lsn(), 62u);
        EXPECT_EQ(segment.read(61).data, records[1].data);
    }

    Index recovered(files.open_index());
    EXPECT_EQ(recovered.scan().entry_count, 2u);
    EXPECT_EQ(recovered.read(1), offsets[1]);
}

TEST(SegmentTest, RecoveryRebuildsIndexWhoseUnsyncedTailReadsAsZeros) {
    // A crash can persist the Index file's length but not its last blocks,
    // which then read back as zeros: complete entries with ordinal 0.
    TempSegmentFiles files;
    const std::vector<WalRecord> records{
        WalRecord{.lsn = 70, .data = {'a'}},
        WalRecord{.lsn = 71, .data = {'b'}},
        WalRecord{.lsn = 72, .data = {'c'}},
    };
    const std::vector<std::uint64_t> offsets = write_store_records(files, records);
    write_index_entries(files, {{0, offsets[0]}});
    const std::vector<char> zeros(2 * Index::ENTRY_SIZE, 0);
    files.append_index_bytes(zeros);

    {
        Segment segment(70, files.open_store(), files.open_index(), config());
        EXPECT_EQ(segment.next_lsn(), 73u);
        EXPECT_EQ(segment.read(71).data, records[1].data);
        EXPECT_EQ(segment.read(72).data, records[2].data);
    }

    Index recovered(files.open_index());
    EXPECT_EQ(recovered.scan().status, IndexScanStatus::Complete);
    EXPECT_EQ(recovered.scan().entry_count, 3u);
    EXPECT_EQ(recovered.read(2), offsets[2]);
}

TEST(SegmentTest, RecoveryTruncatesIncompleteStoreTailAndExtraIndex) {
    TempSegmentFiles files;
    const std::vector<std::uint64_t> offsets = write_store_records(
        files,
        {WalRecord{.lsn = 80, .data = {'a'}}});
    write_index_entries(files, {{0, offsets[0]}, {1, 500}});
    const std::vector<char> partial_store_prefix{0x00, 0x00};
    files.append_store_bytes(partial_store_prefix);

    {
        Segment segment(80, files.open_store(), files.open_index(), config());
        EXPECT_EQ(segment.next_lsn(), 81u);
    }

    Store recovered_store(files.open_store());
    Index recovered_index(files.open_index());
    EXPECT_EQ(recovered_store.scan().status, StoreScanStatus::Complete);
    EXPECT_EQ(recovered_store.scan().record_count, 1u);
    EXPECT_EQ(recovered_index.scan().entry_count, 1u);
}

TEST(SegmentTest, RecoveryRejectsNondenseStoreLsnSequence) {
    TempSegmentFiles files;
    write_store_records(
        files,
        {
            WalRecord{.lsn = 90, .data = {'a'}},
            WalRecord{.lsn = 92, .data = {'b'}},
        });

    EXPECT_THROW(
        Segment(90, files.open_store(), files.open_index(), config()),
        std::runtime_error);
}

TEST(SegmentTest, IsMaxedUsesStrictPostAppendIndexLimit) {
    TempSegmentFiles files;
    Segment segment(
        100,
        files.open_store(),
        files.open_index(),
        config(Index::ENTRY_SIZE, 4096));

    segment.append(WalRecord{.lsn = 100, .data = {}});
    EXPECT_FALSE(segment.is_maxed());

    segment.append(WalRecord{.lsn = 101, .data = {}});
    EXPECT_TRUE(segment.is_maxed());
}

TEST(SegmentTest, IsMaxedUsesStrictPostAppendStoreLimit) {
    TempSegmentFiles files;
    constexpr std::uint64_t FIRST_FRAME_SIZE =
        Store::RECORD_LENGTH_SIZE + WalRecordCodec::HEADER_SIZE + 1;
    Segment segment(
        110,
        files.open_store(),
        files.open_index(),
        config(120, FIRST_FRAME_SIZE));

    segment.append(WalRecord{.lsn = 110, .data = {'a'}});
    EXPECT_FALSE(segment.is_maxed());

    segment.append(WalRecord{.lsn = 111, .data = {'b'}});
    EXPECT_TRUE(segment.is_maxed());
}

TEST(SegmentTest, SyncPersistsAUsableStoreAndIndexPair) {
    TempSegmentFiles files;
    {
        Segment segment(120, files.open_store(), files.open_index(), config());
        segment.append(WalRecord{.lsn = 120, .data = {'x'}});
        EXPECT_NO_THROW(segment.sync());
    }

    Segment reopened(120, files.open_store(), files.open_index(), config());
    EXPECT_EQ(reopened.read(120).data, std::vector<char>({'x'}));
}

TEST(SegmentTest, ConstructorValidatesBaseLsnAndStoreLimit) {
    TempSegmentFiles zero_base_files;
    EXPECT_THROW(
        Segment(0, zero_base_files.open_store(), zero_base_files.open_index(), config()),
        std::invalid_argument);

    TempSegmentFiles zero_store_limit_files;
    EXPECT_THROW(
        Segment(
            1,
            zero_store_limit_files.open_store(),
            zero_store_limit_files.open_index(),
            config(120, 0)),
        std::invalid_argument);
}

} // namespace
