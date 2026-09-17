#pragma once

#include <cstddef>
#include <cstdint>
#include <shared_mutex>
#include <span>
#include <vector>

/// Result of inspecting the length-prefixed records in a WAL store file.
enum class StoreScanStatus : std::uint8_t {
    Complete = 0,
    IncompleteTail,
};

/// Describes the framing-valid portion of a WAL store file.
///
/// `physical_size` includes every byte currently present in the file, while
/// `valid_size` stops immediately after the final complete record. The two
/// sizes differ only when `status` is `IncompleteTail`.
///
/// This scan validates record framing only. Segment and WalRecordCodec validate
/// the absolute LSN stored in each payload. The current minimal record format
/// does not yet include a record type, magic value, or checksum.
struct StoreScanResult {
    StoreScanStatus status = StoreScanStatus::Complete;
    std::uint64_t physical_size = 0;
    std::uint64_t valid_size = 0;
    std::uint64_t record_count = 0;
};

/// Owns the append-only store file for one WAL segment.
///
/// Records use the following framing:
///
/// ```text
/// +---------------------------+----------------------+
/// | payload length (4 bytes)  | opaque payload       |
/// | unsigned, big-endian      | payload length bytes |
/// +---------------------------+----------------------+
/// ```
///
/// The constructor takes ownership of `fd` and inspects all existing frames.
/// If the final frame is incomplete, reads of earlier complete records remain
/// available, but append is rejected until `repair_tail()` is called. The
/// caller must not access or mutate the file through another descriptor while
/// the Store exists.
///
/// Reads may run concurrently. Append and tail repair are serialized.
/// Synchronization runs alongside them: it makes durable at least every append
/// that returned before it was called, so it never needs to stall a writer.
/// The owner must not close the Store while a sync is in flight. Successful
/// append makes bytes visible to reads but does not
/// make them crash-durable; callers use `sync()` at the WAL durability
/// boundary.
class Store {
    public:
        static constexpr std::size_t RECORD_LENGTH_SIZE = sizeof(std::uint32_t);

        /// Takes ownership of an open, writable file descriptor.
        ///
        /// Throws `std::invalid_argument` for a negative descriptor and
        /// propagates filesystem errors encountered while inspecting the file.
        explicit Store(int fd);
        ~Store() noexcept;

        Store(const Store &) = delete;
        Store &operator=(const Store &) = delete;
        Store(Store &&) = delete;
        Store &operator=(Store &&) = delete;

        /// Appends one opaque record and returns its length-prefix offset.
        ///
        /// Empty records are valid. Throws if the record cannot fit in the
        /// four-byte length field, the file has an incomplete tail, or an I/O
        /// operation fails.
        std::uint64_t append(std::span<const char> record);

        /// Reads the record whose length prefix starts at `offset`.
        ///
        /// `offset` must be zero or an offset previously returned by `append`
        /// or discovered while walking complete frames. Throws when the offset
        /// is outside the framing-valid region or the read fails.
        std::vector<char> read(std::uint64_t offset) const;

        /// Reads the first `count` bytes of a complete record payload.
        ///
        /// This avoids decoding or copying a variable-length payload when a
        /// caller needs fixed-position metadata such as a Raft entry term.
        std::vector<char> read_prefix(std::uint64_t offset, std::size_t count) const;

        /// Returns the current framing inspection result.
        StoreScanResult scan() const;

        /// Removes an incomplete final frame, if one exists.
        ///
        /// The file is truncated to `scan().valid_size`. The truncation is not
        /// synchronized automatically; call `sync()` if recovery requires the
        /// repair to be durable before proceeding.
        void repair_tail();

        /// Retains exactly `record_count` complete records from the front.
        ///
        /// The requested count must not exceed the framing-valid prefix. The
        /// truncation is not synchronized automatically.
        void truncate_to(std::uint64_t record_count);

        /// Synchronizes all current store bytes to the required storage layer.
        void sync();

        /// Returns the current physical file size in bytes.
        std::uint64_t size() const;

    private:
        int fd_ = -1;
        std::uint64_t size_ = 0;
        StoreScanResult scan_result_{};
        mutable std::shared_mutex mutex_;

        StoreScanResult inspect_file() const;
};
