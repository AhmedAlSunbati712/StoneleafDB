#pragma once

#include <cstddef>
#include <cstdint>
#include <shared_mutex>

/// Result of inspecting the fixed-width entries in a record index file.
enum class IndexScanStatus : std::uint8_t {
    Complete = 0,
    IncompleteTail,
    Corrupt,
};

/// Describes the valid prefix of a record index file.
///
/// `valid_size` stops before an incomplete final entry or the first entry whose
/// encoded ordinal does not match its positional entry. The Store remains
/// authoritative; Segment later validates that indexed offsets identify real
/// Store records and rebuilds an invalid Index from those records.
struct IndexScanResult {
    IndexScanStatus status = IndexScanStatus::Complete;
    std::uint64_t physical_size = 0;
    std::uint64_t valid_size = 0;
    std::uint64_t entry_count = 0;
};

/// Owns a fixed-width index file for a segmented record store.
///
/// Entries are dense and begin at ordinal zero:
///
/// ```text
/// +--------------------------+--------------------------+
/// | ordinal (4 bytes)        | Store offset (8 bytes)   |
/// | unsigned, big-endian     | unsigned, big-endian     |
/// +--------------------------+--------------------------+
/// ```
///
/// Because entries have a fixed width, lookup is a positional read at
/// `ordinal * ENTRY_SIZE`. The constructor takes ownership of `fd` and
/// inspects existing entries. A partial final entry blocks append until
/// `repair_tail()` truncates it. A corrupt complete entry requires a later
/// rebuild from the authoritative Store and cannot be repaired here.
///
/// Reads may run concurrently. Append and repair are serialized; synchronization
/// runs alongside them and covers every append that returned before it was
/// called. The owner must not close the Index while a sync is in flight.
/// Segment owns the dense-sequence invariant; this lower-level
/// file layer encodes the ordinal supplied by its caller. Append does not
/// make an entry crash-durable; the caller uses `sync()` at the appropriate
/// caller's durability boundary.
class Index {
    public:
        static constexpr std::size_t ORDINAL_SIZE = sizeof(std::uint32_t);
        static constexpr std::size_t STORE_OFFSET_SIZE = sizeof(std::uint64_t);
        static constexpr std::size_t ENTRY_SIZE = ORDINAL_SIZE + STORE_OFFSET_SIZE;

        /// Takes ownership of an open, writable index file descriptor.
        explicit Index(int fd);
        ~Index() noexcept;

        Index(const Index &) = delete;
        Index &operator=(const Index &) = delete;
        Index(Index &&) = delete;
        Index &operator=(Index &&) = delete;

        /// Appends an ordinal-to-Store-offset mapping.
        ///
        /// Segment must supply the current entry ordinal.
        /// Throws when the Index has an incomplete or corrupt tail or an I/O
        /// operation fails.
        void append(std::uint32_t ordinal, std::uint64_t store_offset);

        /// Returns the Store offset mapped by `ordinal`.
        ///
        /// Throws when the ordinal is outside the valid entry prefix, the
        /// stored entry does not match the requested ordinal, or an I/O fails.
        std::uint64_t read(std::uint32_t ordinal) const;

        /// Returns the current fixed-width entry inspection result.
        IndexScanResult scan() const;

        /// Truncates an incomplete final entry, if present.
        ///
        /// Corrupt complete entries are not repairable by Index. Truncation is
        /// not synchronized automatically; call `sync()` when required.
        void repair_tail();

        /// Truncates the Index to exactly `entry_count` complete entries.
        ///
        /// The requested count must not exceed the current valid prefix. This
        /// is used by Segment to discard only an invalid derived suffix before
        /// rebuilding it from the authoritative Store.
        void truncate_to(std::uint64_t entry_count);

        /// Synchronizes all current index bytes to the required storage layer.
        void sync();

        /// Returns the current physical index-file size in bytes.
        std::uint64_t size() const;

    private:
        int fd_ = -1;
        std::uint64_t size_ = 0;
        IndexScanResult scan_result_{};
        mutable std::shared_mutex mutex_;

        IndexScanResult inspect_file() const;
};
