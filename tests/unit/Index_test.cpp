#include <gtest/gtest.h>

#include <DiskIO.h>
#include <Endian.h>
#include <Log/Config.h>
#include <storage/Index.h>

#include <cerrno>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <fcntl.h>
#include <span>
#include <stdexcept>
#include <string>
#include <unistd.h>
#include <vector>

namespace {

class TempIndexFile {
    public:
        TempIndexFile() {
            const auto suffix = std::to_string(
                std::chrono::steady_clock::now().time_since_epoch().count());
            path = std::filesystem::temp_directory_path() /
                ("stoneleafdb_index_test_" + suffix + ".idx");
        }

        ~TempIndexFile() {
            std::error_code ec;
            std::filesystem::remove(path, ec);
        }

        int open() const {
            return disk::open_file(path.string(), O_RDWR | O_CREAT, 0644);
        }

        void write_bytes(std::span<const char> bytes) const {
            std::ofstream file(path, std::ios::binary | std::ios::trunc);
            file.write(bytes.data(), static_cast<std::streamsize>(bytes.size()));
        }

        std::vector<char> read_bytes() const {
            std::ifstream file(path, std::ios::binary);
            return std::vector<char>(
                std::istreambuf_iterator<char>(file),
                std::istreambuf_iterator<char>());
        }

        std::filesystem::path path;
};

std::vector<char> entry_bytes(std::uint32_t ordinal, std::uint64_t store_offset) {
    std::vector<char> entry(Index::ENTRY_SIZE);
    put_u32_be(entry.data(), ordinal);
    put_u64_be(entry.data() + Index::ORDINAL_SIZE, store_offset);
    return entry;
}

TEST(IndexTest, AppendWritesExactBigEndianEntries) {
    TempIndexFile file;
    Index index(file.open());

    index.append(0, 0);
    index.append(1, 0x0102030405060708ULL);

    std::vector<char> expected = entry_bytes(0, 0);
    const std::vector<char> second = entry_bytes(1, 0x0102030405060708ULL);
    expected.insert(expected.end(), second.begin(), second.end());
    EXPECT_EQ(file.read_bytes(), expected);
    EXPECT_EQ(index.size(), 2 * Index::ENTRY_SIZE);
}

TEST(IndexTest, ReadLooksUpStoreOffsetsByRelativeLsn) {
    TempIndexFile file;
    Index index(file.open());
    index.append(0, 0);
    index.append(1, 37);
    index.append(2, 4096);

    EXPECT_EQ(index.read(0), 0u);
    EXPECT_EQ(index.read(1), 37u);
    EXPECT_EQ(index.read(2), 4096u);
}

TEST(IndexTest, ReopenPreservesEntriesAndAppendsNextRelativeLsn) {
    TempIndexFile file;

    {
        Index index(file.open());
        index.append(0, 11);
    }

    {
        Index index(file.open());
        EXPECT_EQ(index.scan().entry_count, 1u);
        EXPECT_EQ(index.read(0), 11u);
        EXPECT_NO_THROW(index.append(1, 29));
        EXPECT_EQ(index.read(1), 29u);
    }
}

TEST(IndexTest, EmptyIndexHasCompleteZeroLengthScan) {
    TempIndexFile file;
    Index index(file.open());

    const IndexScanResult result = index.scan();

    EXPECT_EQ(result.status, IndexScanStatus::Complete);
    EXPECT_EQ(result.physical_size, 0u);
    EXPECT_EQ(result.valid_size, 0u);
    EXPECT_EQ(result.entry_count, 0u);
}

TEST(IndexTest, AppendEncodesTheCallerProvidedRelativeLsn) {
    TempIndexFile file;
    Index index(file.open());

    EXPECT_NO_THROW(index.append(7, 19));
    EXPECT_EQ(file.read_bytes(), entry_bytes(7, 19));
}

TEST(IndexTest, ScanReportsIncompleteFinalEntryAndBlocksAppend) {
    TempIndexFile file;
    std::vector<char> bytes = entry_bytes(0, 0);
    bytes.push_back(0x00);
    bytes.push_back(0x00);
    bytes.push_back(0x00);
    file.write_bytes(bytes);
    Index index(file.open());

    const IndexScanResult result = index.scan();

    EXPECT_EQ(result.status, IndexScanStatus::IncompleteTail);
    EXPECT_EQ(result.physical_size, Index::ENTRY_SIZE + 3);
    EXPECT_EQ(result.valid_size, Index::ENTRY_SIZE);
    EXPECT_EQ(result.entry_count, 1u);
    EXPECT_EQ(index.read(0), 0u);
    EXPECT_THROW(index.append(1, 12), std::runtime_error);
}

TEST(IndexTest, RepairTailTruncatesToEntryBoundaryAndAllowsAppend) {
    TempIndexFile file;
    std::vector<char> bytes = entry_bytes(0, 0);
    bytes.push_back(0x00);
    file.write_bytes(bytes);
    Index index(file.open());

    index.repair_tail();
    const IndexScanResult repaired = index.scan();

    EXPECT_EQ(repaired.status, IndexScanStatus::Complete);
    EXPECT_EQ(repaired.physical_size, Index::ENTRY_SIZE);
    EXPECT_EQ(repaired.valid_size, Index::ENTRY_SIZE);
    EXPECT_EQ(repaired.entry_count, 1u);
    EXPECT_NO_THROW(index.append(1, 20));
    EXPECT_EQ(index.read(1), 20u);
}

TEST(IndexTest, ScanRejectsCompleteEntryWithUnexpectedRelativeLsn) {
    TempIndexFile file;
    file.write_bytes(entry_bytes(1, 0));
    Index index(file.open());

    const IndexScanResult result = index.scan();

    EXPECT_EQ(result.status, IndexScanStatus::Corrupt);
    EXPECT_EQ(result.valid_size, 0u);
    EXPECT_EQ(result.entry_count, 0u);
    EXPECT_THROW(index.repair_tail(), std::runtime_error);
    EXPECT_THROW(index.append(0, 0), std::runtime_error);
}

TEST(IndexTest, ReadRejectsRelativeLsnOutsideValidPrefix) {
    TempIndexFile file;
    Index index(file.open());
    index.append(0, 0);

    EXPECT_THROW(index.read(1), std::out_of_range);
    EXPECT_THROW(index.read(100), std::out_of_range);
}

TEST(IndexTest, SyncSucceedsForOpenIndex) {
    TempIndexFile file;
    Index index(file.open());
    index.append(0, 0);

    EXPECT_NO_THROW(index.sync());
}

TEST(IndexTest, DestructorClosesOwnedDescriptor) {
    TempIndexFile file;
    const int fd = file.open();

    {
        Index index(fd);
    }

    errno = 0;
    EXPECT_EQ(::fcntl(fd, F_GETFD), -1);
    EXPECT_EQ(errno, EBADF);
}

TEST(IndexTest, ConstructorRejectsInvalidDescriptor) {
    EXPECT_THROW(Index(-1), std::invalid_argument);
}

TEST(IndexTest, TruncateToRetainsOnlyTheRequestedValidPrefix) {
    TempIndexFile file;
    Index index(file.open());
    index.append(0, 0);
    index.append(1, 20);
    index.append(2, 40);

    index.truncate_to(1);

    EXPECT_EQ(index.size(), Index::ENTRY_SIZE);
    EXPECT_EQ(index.scan().status, IndexScanStatus::Complete);
    EXPECT_EQ(index.scan().entry_count, 1u);
    EXPECT_EQ(index.read(0), 0u);
    EXPECT_THROW(index.read(1), std::out_of_range);
    EXPECT_NO_THROW(index.append(1, 20));
    EXPECT_THROW(index.truncate_to(3), std::out_of_range);
}

TEST(IndexTest, ConfigRequiresIndexLimitAlignedToEntryWidth) {
    Config valid{
        .max_index_bytes = 10 * Index::ENTRY_SIZE,
        .max_store_bytes = 4096,
        .initial_lsn = 1,
    };
    EXPECT_NO_THROW(valid.validate());

    Config zero{
        .max_index_bytes = 0,
        .max_store_bytes = 4096,
        .initial_lsn = 1,
    };
    EXPECT_THROW(zero.validate(), std::invalid_argument);

    Config misaligned{
        .max_index_bytes = 10 * Index::ENTRY_SIZE + 1,
        .max_store_bytes = 4096,
        .initial_lsn = 1,
    };
    EXPECT_THROW(misaligned.validate(), std::invalid_argument);
}

} // namespace
