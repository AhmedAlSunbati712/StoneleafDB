#include <gtest/gtest.h>

#include <DiskIO.h>
#include <Log/Store.h>
#include <Raft/RaftEntryCodec.h>
#include <Raft/RaftSegment.h>
#include <storage/Index.h>

#include <chrono>
#include <filesystem>
#include <span>
#include <vector>
#include <fcntl.h>

namespace {

class TempSegmentFiles {
public:
    TempSegmentFiles() {
        const auto suffix = std::to_string(
            std::chrono::steady_clock::now().time_since_epoch().count());
        const auto temp = std::filesystem::temp_directory_path();
        store_path = temp / ("stoneleaf_raft_segment_" + suffix + ".store");
        index_path = temp / ("stoneleaf_raft_segment_" + suffix + ".index");
    }
    ~TempSegmentFiles() {
        std::error_code error;
        std::filesystem::remove(store_path, error);
        std::filesystem::remove(index_path, error);
    }
    int open_store() const { return disk::open_file(store_path.string(), O_RDWR | O_CREAT, 0644); }
    int open_index() const { return disk::open_file(index_path.string(), O_RDWR | O_CREAT, 0644); }

    std::filesystem::path store_path;
    std::filesystem::path index_path;
};

Config config() {
    return {.max_index_bytes = 120, .max_store_bytes = 4096, .initial_lsn = 1};
}

RaftMutationEntry entry(std::uint64_t term, std::uint64_t index) {
    return {.term = term, .idx = index, .operations = {}};
}

TEST(RaftSegmentTest, AppendsReadsScansAndLocatesTerms) {
    TempSegmentFiles files;
    RaftSegment segment(10, files.open_store(), files.open_index(), config());
    segment.append(entry(2, 10));
    segment.append(entry(3, 11));

    EXPECT_EQ(segment.base_index(), 10u);
    EXPECT_EQ(segment.next_index(), 12u);
    EXPECT_EQ(segment.read(10).term, 2u);
    EXPECT_EQ(segment.term_at(11), 3u);
    ASSERT_EQ(segment.scan().size(), 2u);
    EXPECT_THROW(segment.append(entry(4, 13)), std::invalid_argument);
}

TEST(RaftSegmentTest, TruncatesStoreAndIndexThenAcceptsReplacement) {
    TempSegmentFiles files;
    {
        RaftSegment segment(1, files.open_store(), files.open_index(), config());
        segment.append(entry(1, 1));
        segment.append(entry(1, 2));
        segment.append(entry(1, 3));
        segment.truncate_suffix(2);
        segment.append(entry(2, 2));
        segment.sync();
    }

    RaftSegment reopened(1, files.open_store(), files.open_index(), config());
    ASSERT_EQ(reopened.scan().size(), 2u);
    EXPECT_EQ(reopened.term_at(1), 1u);
    EXPECT_EQ(reopened.term_at(2), 2u);
}

TEST(RaftSegmentTest, RecoveryRebuildsIndexFromAuthoritativeStore) {
    TempSegmentFiles files;
    {
        Store store(files.open_store());
        store.append(RaftEntryCodec::encode(entry(5, 20)));
        store.append(RaftEntryCodec::encode(entry(5, 21)));
    }
    {
        Index index(files.open_index());
        index.append(0, 0);
    }

    RaftSegment recovered(20, files.open_store(), files.open_index(), config());
    EXPECT_EQ(recovered.next_index(), 22u);
    EXPECT_EQ(recovered.read(21).term, 5u);
    Index rebuilt(files.open_index());
    EXPECT_EQ(rebuilt.scan().entry_count, 2u);
}

TEST(RaftSegmentTest, RecoveryRebuildsIndexWhoseUnsyncedTailReadsAsZeros) {
    TempSegmentFiles files;
    std::uint64_t first_offset = 0;
    {
        Store store(files.open_store());
        first_offset = store.append(RaftEntryCodec::encode(entry(2, 10)));
        store.append(RaftEntryCodec::encode(entry(2, 11)));
        store.append(RaftEntryCodec::encode(entry(3, 12)));
    }
    {
        Index index(files.open_index());
        index.append(0, first_offset);
    }
    {
        // Two complete entries' worth of zeros: blocks the crash never wrote.
        const int fd = files.open_index();
        const std::vector<char> zeros(2 * Index::ENTRY_SIZE, 0);
        disk::write_exact_at(fd, std::span<const char>(zeros),
                             static_cast<std::streamoff>(Index::ENTRY_SIZE));
        disk::close_file(fd);
    }

    RaftSegment recovered(10, files.open_store(), files.open_index(), config());
    EXPECT_EQ(recovered.next_index(), 13u);
    EXPECT_EQ(recovered.read(12).term, 3u);
    EXPECT_EQ(recovered.term_at(11), 2u);
}

TEST(RaftSegmentTest, StoreFirstCrashShapeDoesNotResurrectSuffix) {
    TempSegmentFiles files;
    std::uint64_t second_offset = 0;
    {
        Store store(files.open_store());
        store.append(RaftEntryCodec::encode(entry(1, 1)));
        second_offset = store.append(RaftEntryCodec::encode(entry(1, 2)));
        store.truncate_to(1);
        store.sync();
    }
    {
        Index index(files.open_index());
        index.append(0, 0);
        index.append(1, second_offset);
    }

    RaftSegment recovered(1, files.open_store(), files.open_index(), config());
    EXPECT_EQ(recovered.next_index(), 2u);
    EXPECT_EQ(recovered.scan().size(), 1u);
}

} // namespace
