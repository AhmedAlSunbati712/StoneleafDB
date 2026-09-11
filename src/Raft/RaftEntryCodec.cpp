#include <Raft/RaftEntryCodec.h>

#include <Endian.h>
#include <KeyCodec.h>
#include <ValueCodec.h>

#include <limits>
#include <stdexcept>
#include <type_traits>

namespace {

class Writer {
public:
    void u8(std::uint8_t value) { data_.push_back(static_cast<char>(value)); }
    void u32(std::uint32_t value) {
        const std::size_t offset = data_.size();
        data_.resize(offset + sizeof(value));
        put_u32_be(data_.data() + offset, value);
    }
    void u64(std::uint64_t value) {
        const std::size_t offset = data_.size();
        data_.resize(offset + sizeof(value));
        put_u64_be(data_.data() + offset, value);
    }
    void bytes(std::span<const char> value) {
        data_.insert(data_.end(), value.begin(), value.end());
    }
    std::vector<char> finish() { return std::move(data_); }

private:
    std::vector<char> data_;
};

class Reader {
public:
    explicit Reader(std::span<const char> data) : data_(data) {}

    std::uint8_t u8() {
        require(1);
        return static_cast<std::uint8_t>(data_[offset_++]);
    }
    std::uint32_t u32() {
        require(4);
        const std::uint32_t value = get_u32_be(data_.data() + offset_);
        offset_ += 4;
        return value;
    }
    std::uint64_t u64() {
        require(8);
        const std::uint64_t value = get_u64_be(data_.data() + offset_);
        offset_ += 8;
        return value;
    }
    std::span<const char> bytes(std::size_t count) {
        require(count);
        const auto value = data_.subspan(offset_, count);
        offset_ += count;
        return value;
    }
    std::size_t remaining() const { return data_.size() - offset_; }
    void finish() const {
        if (offset_ != data_.size()) {
            throw std::runtime_error("Raft entry has trailing bytes");
        }
    }

private:
    void require(std::size_t count) const {
        if (count > data_.size() - offset_) {
            throw std::runtime_error("Raft entry is truncated");
        }
    }

    std::span<const char> data_;
    std::size_t offset_ = 0;
};

void write_key(Writer& out, const Key& key) {
    if (!KeyCodec::validate_key(key)) {
        throw std::invalid_argument("Raft mutation contains an invalid key");
    }
    out.u8(static_cast<std::uint8_t>(key.type));
    out.u32(key.size);
    out.bytes(key.data);
}

Key read_key(Reader& in) {
    Key key{.type = static_cast<KeyType>(in.u8()), .size = in.u32()};
    const auto bytes = in.bytes(key.size);
    key.data.assign(bytes.begin(), bytes.end());
    if (!KeyCodec::validate_key(key)) {
        throw std::runtime_error("Raft entry contains an invalid key");
    }
    return key;
}

void write_value(Writer& out, const Value& value) {
    if (!ValueCodec::validate_value(value)) {
        throw std::invalid_argument("Raft put contains an invalid value");
    }
    out.u8(static_cast<std::uint8_t>(value.type));
    out.u32(value.size);
    out.bytes(value.data);
}

Value read_value(Reader& in) {
    Value value{.type = static_cast<ValueType>(in.u8()), .size = in.u32()};
    const auto bytes = in.bytes(value.size);
    value.data.assign(bytes.begin(), bytes.end());
    if (!ValueCodec::validate_value(value)) {
        throw std::runtime_error("Raft entry contains an invalid value");
    }
    return value;
}

void validate_header(const RaftMutationEntry& entry, bool decoding) {
    auto fail = [decoding](const char* message) {
        if (decoding) throw std::runtime_error(message);
        throw std::invalid_argument(message);
    };
    if (entry.term == 0) fail("Raft entry term zero is invalid");
    if (entry.idx == 0) fail("Raft entry index zero is reserved for none");
}

} // namespace

namespace RaftEntryCodec {

std::vector<char> encode(const RaftMutationEntry& entry) {
    validate_header(entry, false);
    if (entry.operations.size() > std::numeric_limits<std::uint32_t>::max()) {
        throw std::invalid_argument("Raft entry contains too many mutations");
    }

    Writer out;
    out.u64(entry.term);
    out.u64(entry.idx);
    out.u32(static_cast<std::uint32_t>(entry.operations.size()));
    for (const MutationOp& mutation : entry.operations) {
        out.u8(static_cast<std::uint8_t>(mutation.type));
        if (mutation.type == RaftMutationType::Put) {
            if (!std::holds_alternative<PutMutation>(mutation.operation)) {
                throw std::invalid_argument("Raft mutation type does not match Put payload");
            }
            const auto& put = std::get<PutMutation>(mutation.operation);
            write_key(out, put.key);
            write_value(out, put.value);
        } else if (mutation.type == RaftMutationType::Delete) {
            if (!std::holds_alternative<DeleteMutation>(mutation.operation)) {
                throw std::invalid_argument("Raft mutation type does not match Delete payload");
            }
            write_key(out, std::get<DeleteMutation>(mutation.operation).key);
        } else {
            throw std::invalid_argument("Raft mutation type is unknown");
        }
    }
    return out.finish();
}

RaftMutationEntry decode(std::span<const char> encoded) {
    Reader in(encoded);
    RaftMutationEntry entry{.term = in.u64(), .idx = in.u64()};
    validate_header(entry, true);

    const std::uint32_t operation_count = in.u32();
    constexpr std::size_t MINIMUM_OPERATION_SIZE = 1 + 1 + 4;
    if (operation_count > in.remaining() / MINIMUM_OPERATION_SIZE) {
        throw std::runtime_error("Raft mutation count exceeds entry size");
    }
    entry.operations.reserve(operation_count);
    for (std::uint32_t i = 0; i < operation_count; ++i) {
        const auto type = static_cast<RaftMutationType>(in.u8());
        if (type == RaftMutationType::Put) {
            Key key = read_key(in);
            Value value = read_value(in);
            entry.operations.push_back(MutationOp{
                .type = type,
                .operation = PutMutation{std::move(key), std::move(value)},
            });
        } else if (type == RaftMutationType::Delete) {
            entry.operations.push_back(MutationOp{
                .type = type,
                .operation = DeleteMutation{read_key(in)},
            });
        } else {
            throw std::runtime_error("Raft entry contains an unknown mutation type");
        }
    }
    in.finish();
    return entry;
}

} // namespace RaftEntryCodec
