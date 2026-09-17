#include <storage/Index.h>

#include <DiskIO.h>
#include <Endian.h>

#include <array>
#include <exception>
#include <mutex>
#include <stdexcept>

namespace {

constexpr std::size_t ORDINAL_OFFSET = 0;
constexpr std::size_t STORE_OFFSET_OFFSET = Index::ORDINAL_SIZE;

} // namespace

Index::Index(int fd) : fd_(fd) {
    if (fd_ < 0) {
        throw std::invalid_argument("Index requires a valid file descriptor");
    }

    try {
        scan_result_ = inspect_file();
        size_ = scan_result_.physical_size;
    } catch (...) {
        try {
            disk::close_file(fd_);
        } catch (const std::exception &) {
            // Preserve the inspection failure that prevented construction.
        }
        fd_ = -1;
        throw;
    }
}

Index::~Index() noexcept {
    if (fd_ == -1) return;

    try {
        disk::close_file(fd_);
    } catch (const std::exception &) {
        // Durability-sensitive callers must use sync() before destruction.
    }
    fd_ = -1;
}

void Index::append(std::uint32_t ordinal, std::uint64_t store_offset) {
    std::unique_lock lock(mutex_);

    if (scan_result_.status != IndexScanStatus::Complete) {
        throw std::runtime_error("Index must be repaired or rebuilt before appending");
    }

    const std::uint64_t entry_offset = size_;
    std::array<char, ENTRY_SIZE> entry{};
    put_u32_be(entry.data() + ORDINAL_OFFSET, ordinal);
    put_u64_be(entry.data() + STORE_OFFSET_OFFSET, store_offset);

    try {
        disk::write_exact_at(
            fd_,
            entry,
            static_cast<std::streamoff>(entry_offset));
    } catch (...) {
        std::exception_ptr append_failure = std::current_exception();
        try {
            disk::truncate_file(fd_, static_cast<std::streamoff>(entry_offset));
            size_ = entry_offset;
            scan_result_.status = IndexScanStatus::Complete;
            scan_result_.physical_size = size_;
            scan_result_.valid_size = size_;
        } catch (...) {
            try {
                scan_result_ = inspect_file();
                size_ = scan_result_.physical_size;
            } catch (...) {
                scan_result_.status = IndexScanStatus::Corrupt;
            }
        }
        std::rethrow_exception(append_failure);
    }

    size_ += ENTRY_SIZE;
    scan_result_.status = IndexScanStatus::Complete;
    scan_result_.physical_size = size_;
    scan_result_.valid_size = size_;
    scan_result_.entry_count += 1;
}

std::uint64_t Index::read(std::uint32_t ordinal) const {
    std::shared_lock lock(mutex_);

    if (ordinal >= scan_result_.entry_count) {
        throw std::out_of_range("Index ordinal is outside the valid entry range");
    }

    const std::uint64_t entry_offset = static_cast<std::uint64_t>(ordinal) * ENTRY_SIZE;
    std::array<char, ENTRY_SIZE> entry{};
    disk::read_exact_at(fd_, entry, static_cast<std::streamoff>(entry_offset));

    const std::uint32_t stored_ordinal = get_u32_be(entry.data() + ORDINAL_OFFSET);
    if (stored_ordinal != ordinal) {
        throw std::runtime_error("Index entry contains an unexpected ordinal");
    }

    return get_u64_be(entry.data() + STORE_OFFSET_OFFSET);
}

IndexScanResult Index::scan() const {
    std::shared_lock lock(mutex_);
    return scan_result_;
}

void Index::repair_tail() {
    std::unique_lock lock(mutex_);

    scan_result_ = inspect_file();
    size_ = scan_result_.physical_size;
    if (scan_result_.status == IndexScanStatus::Complete) return;
    if (scan_result_.status == IndexScanStatus::Corrupt) {
        throw std::runtime_error("A corrupt Index must be rebuilt from the Store");
    }

    disk::truncate_file(fd_, static_cast<std::streamoff>(scan_result_.valid_size));
    size_ = scan_result_.valid_size;
    scan_result_.status = IndexScanStatus::Complete;
    scan_result_.physical_size = size_;
}

void Index::truncate_to(std::uint64_t entry_count) {
    std::unique_lock lock(mutex_);

    if (entry_count > scan_result_.entry_count) {
        throw std::out_of_range("Cannot retain more Index entries than the valid prefix");
    }

    const std::uint64_t new_size = entry_count * ENTRY_SIZE;
    disk::truncate_file(fd_, static_cast<std::streamoff>(new_size));
    size_ = new_size;
    scan_result_.status = IndexScanStatus::Complete;
    scan_result_.physical_size = new_size;
    scan_result_.valid_size = new_size;
    scan_result_.entry_count = entry_count;
}

void Index::sync() {
    // As Store::sync: fsync outside the lock, so appends proceed meanwhile.
    int fd = -1;
    {
        std::shared_lock lock(mutex_);
        fd = fd_;
    }
    disk::sync_file_to_disk_fd(fd);
}

std::uint64_t Index::size() const {
    std::shared_lock lock(mutex_);
    return size_;
}

IndexScanResult Index::inspect_file() const {
    IndexScanResult result;
    result.physical_size = static_cast<std::uint64_t>(disk::file_size(fd_));

    const std::uint64_t complete_entries = result.physical_size / ENTRY_SIZE;
    for (std::uint64_t ordinal = 0; ordinal < complete_entries; ++ordinal) {
        std::array<char, ENTRY_SIZE> entry{};
        const std::uint64_t entry_offset = ordinal * ENTRY_SIZE;
        disk::read_exact_at(
            fd_,
            entry,
            static_cast<std::streamoff>(entry_offset));

        const std::uint32_t stored_ordinal =
            get_u32_be(entry.data() + ORDINAL_OFFSET);
        if (stored_ordinal != ordinal) {
            result.status = IndexScanStatus::Corrupt;
            result.valid_size = entry_offset;
            result.entry_count = ordinal;
            return result;
        }
    }

    result.entry_count = complete_entries;
    result.valid_size = complete_entries * ENTRY_SIZE;
    result.status = result.valid_size == result.physical_size
        ? IndexScanStatus::Complete
        : IndexScanStatus::IncompleteTail;
    return result;
}
