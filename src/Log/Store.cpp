#include <Log/Store.h>

#include <DiskIO.h>
#include <Endian.h>

#include <array>
#include <exception>
#include <limits>
#include <mutex>
#include <stdexcept>

Store::Store(int fd) : fd_(fd) {
    if (fd_ < 0) {
        throw std::invalid_argument("Store requires a valid file descriptor");
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

Store::~Store() noexcept {
    if (fd_ == -1) return;

    try {
        disk::close_file(fd_);
    } catch (const std::exception &) {
        // Destructors cannot report close failures. WAL callers must use
        // sync() before destruction when durability errors matter.
    }
    fd_ = -1;
}

std::uint64_t Store::append(std::span<const char> record) {
    if (record.size() > std::numeric_limits<std::uint32_t>::max()) {
        throw std::length_error("Store record exceeds the four-byte length field");
    }

    std::unique_lock lock(mutex_);
    if (scan_result_.status == StoreScanStatus::IncompleteTail) {
        throw std::runtime_error("Store has an incomplete tail; repair it before appending");
    }

    const std::uint64_t record_size = static_cast<std::uint64_t>(record.size());
    const std::uint64_t frame_size = RECORD_LENGTH_SIZE + record_size;
    const std::uint64_t record_offset = size_;
    std::array<char, RECORD_LENGTH_SIZE> length_bytes{};
    put_u32_be(length_bytes.data(), static_cast<std::uint32_t>(record_size));

    try {
        disk::write_exact_at(
            fd_,
            std::span<const char>(length_bytes),
            static_cast<std::streamoff>(record_offset));
        disk::write_exact_at(
            fd_,
            record,
            static_cast<std::streamoff>(record_offset + RECORD_LENGTH_SIZE));
    } catch (...) {
        std::exception_ptr append_failure = std::current_exception();
        try {
            disk::truncate_file(fd_, static_cast<std::streamoff>(record_offset));
            size_ = record_offset;
            scan_result_.status = StoreScanStatus::Complete;
            scan_result_.physical_size = size_;
            scan_result_.valid_size = size_;
        } catch (...) {
            // A failed rollback leaves the exact tail state unknown. Inspect it
            // before returning so a later append cannot extend damaged bytes.
            try {
                scan_result_ = inspect_file();
                size_ = scan_result_.physical_size;
            } catch (...) {
                scan_result_.status = StoreScanStatus::IncompleteTail;
            }
        }
        std::rethrow_exception(append_failure);
    }

    size_ = record_offset + frame_size;
    scan_result_.status = StoreScanStatus::Complete;
    scan_result_.physical_size = size_;
    scan_result_.valid_size = size_;
    scan_result_.record_count += 1;
    return record_offset;
}

std::vector<char> Store::read(std::uint64_t offset) const {
    std::shared_lock lock(mutex_);

    if (offset >= scan_result_.valid_size ||
        scan_result_.valid_size - offset < RECORD_LENGTH_SIZE) {
        throw std::out_of_range("Store record offset is outside the valid record region");
    }

    std::array<char, RECORD_LENGTH_SIZE> length_bytes{};
    disk::read_exact_at(fd_, length_bytes, static_cast<std::streamoff>(offset));
    const std::uint64_t record_size = get_u32_be(length_bytes.data());
    const std::uint64_t payload_offset = offset + RECORD_LENGTH_SIZE;

    if (record_size > scan_result_.valid_size - payload_offset) {
        throw std::runtime_error("Store record extends beyond the valid record region");
    }

    std::vector<char> record(static_cast<std::size_t>(record_size));
    disk::read_exact_at(fd_, record, static_cast<std::streamoff>(payload_offset));
    return record;
}

std::vector<char> Store::read_prefix(std::uint64_t offset, std::size_t count) const {
    std::shared_lock lock(mutex_);

    if (offset >= scan_result_.valid_size ||
        scan_result_.valid_size - offset < RECORD_LENGTH_SIZE) {
        throw std::out_of_range("Store record offset is outside the valid record region");
    }

    std::array<char, RECORD_LENGTH_SIZE> length_bytes{};
    disk::read_exact_at(fd_, length_bytes, static_cast<std::streamoff>(offset));
    const std::uint64_t record_size = get_u32_be(length_bytes.data());
    const std::uint64_t payload_offset = offset + RECORD_LENGTH_SIZE;
    if (record_size > scan_result_.valid_size - payload_offset) {
        throw std::runtime_error("Store record extends beyond the valid record region");
    }
    if (count > record_size) {
        throw std::out_of_range("Store prefix exceeds the record payload");
    }

    std::vector<char> prefix(count);
    disk::read_exact_at(fd_, prefix, static_cast<std::streamoff>(payload_offset));
    return prefix;
}

StoreScanResult Store::scan() const {
    std::shared_lock lock(mutex_);
    return scan_result_;
}

void Store::repair_tail() {
    std::unique_lock lock(mutex_);

    // Re-inspect while holding the exclusive lock so the truncation boundary
    // cannot be stale relative to an append through this Store.
    scan_result_ = inspect_file();
    size_ = scan_result_.physical_size;
    if (scan_result_.status == StoreScanStatus::Complete) return;

    disk::truncate_file(fd_, static_cast<std::streamoff>(scan_result_.valid_size));
    size_ = scan_result_.valid_size;
    scan_result_.status = StoreScanStatus::Complete;
    scan_result_.physical_size = size_;
}

void Store::truncate_to(std::uint64_t record_count) {
    std::unique_lock lock(mutex_);
    if (record_count > scan_result_.record_count) {
        throw std::out_of_range("Cannot retain more Store records than the valid prefix");
    }

    std::uint64_t offset = 0;
    for (std::uint64_t ordinal = 0; ordinal < record_count; ++ordinal) {
        std::array<char, RECORD_LENGTH_SIZE> length_bytes{};
        disk::read_exact_at(fd_, length_bytes, static_cast<std::streamoff>(offset));
        offset += RECORD_LENGTH_SIZE + get_u32_be(length_bytes.data());
    }

    disk::truncate_file(fd_, static_cast<std::streamoff>(offset));
    size_ = offset;
    scan_result_.status = StoreScanStatus::Complete;
    scan_result_.physical_size = offset;
    scan_result_.valid_size = offset;
    scan_result_.record_count = record_count;
}

void Store::sync() {
    // The fsync runs with the lock released so appends are not stalled behind
    // the disk. It still covers every byte whose append returned before this
    // call: those write()s completed, and fsync flushes all completed writes.
    // The descriptor outlives the call because only the owner that is waiting
    // for this sync to finish can close it.
    int fd = -1;
    {
        std::shared_lock lock(mutex_);
        fd = fd_;
    }
    disk::sync_file_to_disk_fd(fd);
}

std::uint64_t Store::size() const {
    std::shared_lock lock(mutex_);
    return size_;
}

StoreScanResult Store::inspect_file() const {
    StoreScanResult result;
    result.physical_size = static_cast<std::uint64_t>(disk::file_size(fd_));

    std::uint64_t offset = 0;
    while (offset < result.physical_size) {
        const std::uint64_t remaining = result.physical_size - offset;
        if (remaining < RECORD_LENGTH_SIZE) {
            result.status = StoreScanStatus::IncompleteTail;
            result.valid_size = offset;
            return result;
        }

        std::array<char, RECORD_LENGTH_SIZE> length_bytes{};
        disk::read_exact_at(fd_, length_bytes, static_cast<std::streamoff>(offset));
        const std::uint64_t record_size = get_u32_be(length_bytes.data());
        const std::uint64_t remaining_payload = remaining - RECORD_LENGTH_SIZE;
        if (record_size > remaining_payload) {
            result.status = StoreScanStatus::IncompleteTail;
            result.valid_size = offset;
            return result;
        }

        offset += RECORD_LENGTH_SIZE + record_size;
        result.record_count += 1;
    }

    result.status = StoreScanStatus::Complete;
    result.valid_size = result.physical_size;
    return result;
}
