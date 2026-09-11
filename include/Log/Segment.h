#pragma once

#include <Log/Config.h>
#include <Log/Store.h>
#include <Log/WalRecord.h>
#include <storage/Index.h>

#include <cstdint>
#include <shared_mutex>
#include <vector>

/// Coordinates one authoritative Store with its derived fixed-width Index.
///
/// Segment receives its absolute `base_lsn` and already-open file descriptors
/// from the future Log layer. Construction performs tail-oriented recovery:
/// incomplete Store framing is removed, the last matching Store/Index boundary
/// is located, and only the inconsistent Index suffix is reconstructed.
///
/// Segment never creates the next segment. After every successful append, its
/// caller checks `is_maxed()` and rolls before appending another record.
class Segment {
    public:
        /// Takes ownership of both file descriptors through Store and Index.
        Segment(
            std::uint64_t base_lsn,
            int store_fd,
            int index_fd,
            Config config);

        Segment(const Segment &) = delete;
        Segment &operator=(const Segment &) = delete;
        Segment(Segment &&) = delete;
        Segment &operator=(Segment &&) = delete;

        /// Appends the record to Store and then appends its derived Index entry.
        ///
        /// The record LSN must equal `next_lsn()`. A failed physical append
        /// makes the Segment non-writable until it is reopened and recovered.
        void append(const WalRecord &record);

        /// Looks up and decodes one record by absolute LSN.
        WalRecord read(std::uint64_t lsn) const;

        /// Decodes all Store records in ascending absolute-LSN order.
        std::vector<WalRecord> scan() const;

        /// Synchronizes authoritative Store bytes before derived Index bytes.
        void sync();

        /// Reports whether either configured limit was strictly exceeded.
        bool is_maxed() const;

        std::uint64_t base_lsn() const;
        std::uint64_t next_lsn() const;
        bool recovery_required() const;

    private:
        Config config_;
        std::uint64_t base_lsn_ = 0;
        std::uint64_t next_lsn_ = 0;
        bool recovery_required_ = false;
        Store store_;
        Index index_;
        mutable std::shared_mutex mutex_;

        void recover();
};
