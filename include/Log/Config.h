#pragma once

#include <storage/Index.h>

#include <cstdint>
#include <stdexcept>

/// Configuration shared by the standalone WAL logger components.
struct Config {
    std::uint64_t max_index_bytes = 0;
    std::uint64_t max_store_bytes = 0;
    std::uint64_t initial_lsn = 1;

    /// Validates configuration rules finalized by the current logger layers.
    ///
    /// The fixed-width Index must end only on an entry boundary. Other logger
    /// fields are validated when their owning components are implemented.
    void validate() const {
        if (max_index_bytes == 0 || max_index_bytes % Index::ENTRY_SIZE != 0) {
            throw std::invalid_argument(
                "max_index_bytes must be a nonzero multiple of Index::ENTRY_SIZE");
        }
        if (max_store_bytes == 0) {
            throw std::invalid_argument("max_store_bytes must be nonzero");
        }
        if (initial_lsn == 0) {
            throw std::invalid_argument("initial_lsn must be nonzero");
        }
    }
};
