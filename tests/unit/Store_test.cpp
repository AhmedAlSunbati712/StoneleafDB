#include <gtest/gtest.h>

#include <DiskIO.h>
#include <Log/Store.h>

#include <array>
#include <cerrno>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <fcntl.h>
#include <span>
#include <string>
#include <system_error>
#include <unistd.h>
#include <vector>

namespace {

class TempStoreFile {
    public:
        TempStoreFile() {
            const auto suffix = std::to_string(
                std::chrono::steady_clock::now().time_since_epoch().count());
            path = std::filesystem::temp_directory_path() /
                ("stoneleafdb_store_test_" + suffix + ".wal");
        }

        ~TempStoreFile() {
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

std::vector<char> as_bytes(const std::string &value) {
    return std::vector<char>(value.begin(), value.end());
}

TEST(StoreTest, AppendUsesBigEndianLengthPrefixesAndReturnsOffsets) {
    TempStoreFile file;
    Store store(file.open());
    const std::vector<char> first = as_bytes("abc");
    const std::vector<char> second = as_bytes("de");

    EXPECT_EQ(store.append(first), 0u);
    EXPECT_EQ(store.append(second), 7u);
    EXPECT_EQ(store.size(), 13u);

    const std::vector<char> expected{
        0x00, 0x00, 0x00, 0x03, 'a', 'b', 'c',
        0x00, 0x00, 0x00, 0x02, 'd', 'e',
    };
    EXPECT_EQ(file.read_bytes(), expected);
}

TEST(StoreTest, ReadRoundTripsBinaryAndEmptyRecords) {
    TempStoreFile file;
    Store store(file.open());
    const std::vector<char> binary{
        static_cast<char>(0x00),
        static_cast<char>(0xFF),
        static_cast<char>(0x7F),
    };
    const std::vector<char> empty;

    const std::uint64_t binary_offset = store.append(binary);
    const std::uint64_t empty_offset = store.append(empty);

    EXPECT_EQ(store.read(binary_offset), binary);
    EXPECT_TRUE(store.read(empty_offset).empty());
}

TEST(StoreTest, ReadPrefixReturnsOnlyRequestedPayloadBytes) {
    TempStoreFile file;
    Store store(file.open());
    const std::uint64_t offset = store.append(as_bytes("abcdef"));

    EXPECT_EQ(store.read_prefix(offset, 3), as_bytes("abc"));
    EXPECT_TRUE(store.read_prefix(offset, 0).empty());
    EXPECT_THROW(store.read_prefix(offset, 7), std::out_of_range);
}

TEST(StoreTest, ReopenPreservesExistingRecordsAndAppendsAtEnd) {
    TempStoreFile file;
    const std::vector<char> first = as_bytes("first");
    const std::vector<char> second = as_bytes("second");

    {
        Store store(file.open());
        EXPECT_EQ(store.append(first), 0u);
    }

    {
        Store store(file.open());
        EXPECT_EQ(store.scan().record_count, 1u);
        EXPECT_EQ(store.append(second), 9u);
        EXPECT_EQ(store.read(0), first);
        EXPECT_EQ(store.read(9), second);
    }
}

TEST(StoreTest, ScanReportsCleanFileMetadata) {
    TempStoreFile file;
    Store store(file.open());
    store.append(as_bytes("one"));
    store.append(as_bytes("two"));

    const StoreScanResult result = store.scan();

    EXPECT_EQ(result.status, StoreScanStatus::Complete);
    EXPECT_EQ(result.physical_size, 14u);
    EXPECT_EQ(result.valid_size, 14u);
    EXPECT_EQ(result.record_count, 2u);
}

TEST(StoreTest, ScanReportsPartialLengthPrefixAfterLastCompleteRecord) {
    TempStoreFile file;
    const std::vector<char> bytes{
        0x00, 0x00, 0x00, 0x01, 'a',
        0x00, 0x00,
    };
    file.write_bytes(bytes);
    Store store(file.open());

    const StoreScanResult result = store.scan();

    EXPECT_EQ(result.status, StoreScanStatus::IncompleteTail);
    EXPECT_EQ(result.physical_size, 7u);
    EXPECT_EQ(result.valid_size, 5u);
    EXPECT_EQ(result.record_count, 1u);
    EXPECT_EQ(store.read(0), as_bytes("a"));
    EXPECT_THROW(store.append(as_bytes("blocked")), std::runtime_error);
}

TEST(StoreTest, ScanReportsPartialPayloadAfterLastCompleteRecord) {
    TempStoreFile file;
    const std::vector<char> bytes{
        0x00, 0x00, 0x00, 0x01, 'a',
        0x00, 0x00, 0x00, 0x04, 'b', 'c',
    };
    file.write_bytes(bytes);
    Store store(file.open());

    const StoreScanResult result = store.scan();

    EXPECT_EQ(result.status, StoreScanStatus::IncompleteTail);
    EXPECT_EQ(result.physical_size, 11u);
    EXPECT_EQ(result.valid_size, 5u);
    EXPECT_EQ(result.record_count, 1u);
}

TEST(StoreTest, RepairTailTruncatesToValidBoundaryAndAllowsAppend) {
    TempStoreFile file;
    const std::vector<char> bytes{
        0x00, 0x00, 0x00, 0x01, 'a',
        0x00, 0x00, 0x00, 0x04, 'b',
    };
    file.write_bytes(bytes);
    Store store(file.open());

    store.repair_tail();
    const StoreScanResult repaired = store.scan();

    EXPECT_EQ(repaired.status, StoreScanStatus::Complete);
    EXPECT_EQ(repaired.physical_size, 5u);
    EXPECT_EQ(repaired.valid_size, 5u);
    EXPECT_EQ(repaired.record_count, 1u);
    EXPECT_EQ(store.append(as_bytes("z")), 5u);
    EXPECT_EQ(store.read(5), as_bytes("z"));
}

TEST(StoreTest, TruncateToRetainsCompletePrefixAndAllowsAppend) {
    TempStoreFile file;
    Store store(file.open());
    store.append(as_bytes("one"));
    store.append(as_bytes("two"));
    store.append(as_bytes("three"));

    store.truncate_to(1);

    EXPECT_EQ(store.scan().record_count, 1u);
    EXPECT_EQ(store.read(0), as_bytes("one"));
    EXPECT_EQ(store.append(as_bytes("replacement")), 7u);
    EXPECT_THROW(store.truncate_to(3), std::out_of_range);
}

TEST(StoreTest, ReadRejectsOffsetsOutsideCompleteRecordRegion) {
    TempStoreFile file;
    Store store(file.open());
    store.append(as_bytes("abc"));

    EXPECT_THROW(store.read(7), std::out_of_range);
    EXPECT_THROW(store.read(100), std::out_of_range);
}

TEST(StoreTest, SyncSucceedsForOpenStore) {
    TempStoreFile file;
    Store store(file.open());
    store.append(as_bytes("durable"));

    EXPECT_NO_THROW(store.sync());
}

TEST(StoreTest, DestructorClosesOwnedDescriptor) {
    TempStoreFile file;
    const int fd = file.open();

    {
        Store store(fd);
    }

    errno = 0;
    EXPECT_EQ(::fcntl(fd, F_GETFD), -1);
    EXPECT_EQ(errno, EBADF);
}

TEST(StoreTest, ConstructorRejectsInvalidDescriptor) {
    EXPECT_THROW(Store(-1), std::invalid_argument);
}

} // namespace
