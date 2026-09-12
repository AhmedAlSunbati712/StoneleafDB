#include <Log/WalPayloadCodec.h>

#include <Endian.h>
#include <KeyCodec.h>
#include <V2PageCodec.h>
#include <ValueCodec.h>

#include <algorithm>
#include <limits>
#include <stdexcept>
#include <type_traits>

namespace {

class Writer {
public:
    void u8(std::uint8_t value) { data_.push_back(static_cast<char>(value)); }
    void u32(std::uint32_t value) { const auto at = data_.size(); data_.resize(at + 4); put_u32_be(data_.data() + at, value); }
    void u64(std::uint64_t value) { const auto at = data_.size(); data_.resize(at + 8); put_u64_be(data_.data() + at, value); }
    void bytes(std::span<const char> value) { data_.insert(data_.end(), value.begin(), value.end()); }
    std::vector<char> finish() { return std::move(data_); }
private:
    std::vector<char> data_;
};

class Reader {
public:
    explicit Reader(std::span<const char> data) : data_(data) {}
    std::uint8_t u8() { require(1); return static_cast<unsigned char>(data_[offset_++]); }
    std::uint32_t u32() { require(4); const auto v = get_u32_be(data_.data() + offset_); offset_ += 4; return v; }
    std::uint64_t u64() { require(8); const auto v = get_u64_be(data_.data() + offset_); offset_ += 8; return v; }
    std::span<const char> bytes(std::size_t count) { require(count); auto value = data_.subspan(offset_, count); offset_ += count; return value; }
    std::size_t remaining() const { return data_.size() - offset_; }
    void finish() const { if (offset_ != data_.size()) throw std::runtime_error("WAL payload has trailing bytes"); }
private:
    void require(std::size_t count) const { if (count > data_.size() - offset_) throw std::runtime_error("WAL payload is truncated"); }
    std::span<const char> data_;
    std::size_t offset_ = 0;
};

template <typename Enum>
Enum checked_enum(std::uint8_t raw, std::uint8_t first, std::uint8_t last, const char* message) {
    if (raw < first || raw > last) throw std::runtime_error(message);
    return static_cast<Enum>(raw);
}

void write_key(Writer& out, const Key& key) {
    if (!KeyCodec::validate_key(key)) throw std::invalid_argument("B-tree undo contains an invalid key");
    out.u8(static_cast<std::uint8_t>(key.type)); out.u32(key.size); out.bytes(key.data);
}

Key read_key(Reader& in) {
    Key key{.type = static_cast<KeyType>(in.u8())};
    key.size = in.u32();
    auto bytes = in.bytes(key.size); key.data.assign(bytes.begin(), bytes.end());
    if (!KeyCodec::validate_key(key)) throw std::runtime_error("WAL payload contains an invalid key");
    return key;
}

void write_value(Writer& out, const Value& value) {
    if (!ValueCodec::validate_value(value)) throw std::invalid_argument("B-tree undo contains an invalid value");
    out.u8(static_cast<std::uint8_t>(value.type)); out.u32(value.size); out.bytes(value.data);
}

Value read_value(Reader& in) {
    Value value{.type = static_cast<ValueType>(in.u8())};
    value.size = in.u32();
    auto bytes = in.bytes(value.size); value.data.assign(bytes.begin(), bytes.end());
    if (!ValueCodec::validate_value(value)) throw std::runtime_error("WAL payload contains an invalid value");
    return value;
}

void write_effects(Writer& out, const std::vector<PageEffect>& effects) {
    if (effects.empty()) throw std::invalid_argument("WAL mutation payload requires a page effect");
    if (effects.size() > std::numeric_limits<std::uint32_t>::max()) throw std::invalid_argument("Too many page effects");
    out.u32(static_cast<std::uint32_t>(effects.size()));
    for (const auto& effect : effects) {
        if (effect.kind < PageEffectKind::Write || effect.kind > PageEffectKind::Free) throw std::invalid_argument("Unknown page effect kind");
        if (V2PageCodec::validate_structure(effect.after_image) != V2PageCodecResult::Success ||
            V2PageCodec::page_num(effect.after_image) != effect.page_num) {
            throw std::invalid_argument("Page effect image does not match its page number");
        }
        out.u8(static_cast<std::uint8_t>(effect.kind)); out.u32(effect.page_num); out.bytes(effect.after_image);
    }
}

std::vector<PageEffect> read_effects(Reader& in) {
    const auto count = in.u32();
    if (count == 0) throw std::runtime_error("WAL mutation payload has no page effects");
    if (count > in.remaining() / (1 + 4 + V2_PAGE_SIZE)) throw std::runtime_error("Page effect count exceeds payload size");
    std::vector<PageEffect> effects; effects.reserve(count);
    for (std::uint32_t i = 0; i < count; ++i) {
        PageEffect effect{.kind = checked_enum<PageEffectKind>(in.u8(), 1, 3, "Unknown page effect kind"), .page_num = in.u32()};
        auto image = in.bytes(V2_PAGE_SIZE); std::copy(image.begin(), image.end(), effect.after_image.begin());
        if (V2PageCodec::validate_structure(effect.after_image) != V2PageCodecResult::Success ||
            V2PageCodec::page_num(effect.after_image) != effect.page_num) throw std::runtime_error("Invalid page effect image");
        effects.push_back(std::move(effect));
    }
    return effects;
}

void require_payload_type(WalRecordType expected, const WalPayload& payload) {
    const bool matches = static_cast<std::size_t>(expected) - 1 == payload.index();
    if (!matches) throw std::invalid_argument("Payload variant does not match WAL record type");
}

} // namespace

BTreeActionKind action_kind(const UndoDescriptor& undo) {
    return std::visit([](const auto& value) {
        using T = std::decay_t<decltype(value)>;
        if constexpr (std::is_same_v<T, InsertUndo>) return BTreeActionKind::Insert;
        if constexpr (std::is_same_v<T, UpdateUndo>) return BTreeActionKind::Update;
        return BTreeActionKind::Delete;
    }, undo);
}

namespace WalPayloadCodec {

std::vector<char> encode(WalRecordType type, const WalPayload& payload) {
    require_payload_type(type, payload);
    Writer out;
    if (type == WalRecordType::TxnBegin || type == WalRecordType::TxnEnd) return out.finish();
    if (type == WalRecordType::TxnCommit) { out.u64(std::get<CommitPayload>(payload).raft_index); return out.finish(); }
    if (type == WalRecordType::TxnAbort) {
        const auto reason = std::get<AbortPayload>(payload).reason;
        if (reason < AbortReason::ClientRequest || reason > AbortReason::InternalError) throw std::invalid_argument("Unknown abort reason");
        out.u8(static_cast<std::uint8_t>(reason)); return out.finish();
    }
    if (type == WalRecordType::BTreeAction) {
        const auto& action = std::get<BTreeActionPayload>(payload);
        out.u8(static_cast<std::uint8_t>(action_kind(action.undo)));
        std::visit([&](const auto& undo) { write_key(out, undo.key); if constexpr (!std::is_same_v<std::decay_t<decltype(undo)>, InsertUndo>) write_value(out, undo.old_value); }, action.undo);
        write_effects(out, action.effects); return out.finish();
    }
    if (type == WalRecordType::Compensation) {
        const auto& clr = std::get<CompensationPayload>(payload); out.u64(clr.undo_of_lsn); out.u64(clr.undo_next_lsn); write_effects(out, clr.effects); return out.finish();
    }
    const auto& system = std::get<SystemActionPayload>(payload);
    if (system.kind < SystemActionKind::BTreeSplit || system.kind > SystemActionKind::PageMaintenance) throw std::invalid_argument("Unknown system action kind");
    out.u8(static_cast<std::uint8_t>(system.kind)); write_effects(out, system.effects); return out.finish();
}

WalPayload decode(WalRecordType type, std::span<const char> payload) {
    Reader in(payload); WalPayload result;
    switch (type) {
        case WalRecordType::TxnBegin: result = BeginPayload{}; break;
        case WalRecordType::TxnCommit: result = CommitPayload{in.u64()}; break;
        case WalRecordType::TxnEnd: result = EndPayload{}; break;
        case WalRecordType::TxnAbort: result = AbortPayload{checked_enum<AbortReason>(in.u8(), 1, 4, "Unknown abort reason")}; break;
        case WalRecordType::BTreeAction: {
            const auto kind = checked_enum<BTreeActionKind>(in.u8(), 1, 3, "Unknown B-tree action kind");
            Key key = read_key(in);
            UndoDescriptor undo = kind == BTreeActionKind::Insert ? UndoDescriptor{InsertUndo{std::move(key)}} :
                kind == BTreeActionKind::Update ? UndoDescriptor{UpdateUndo{std::move(key), read_value(in)}} : UndoDescriptor{DeleteUndo{std::move(key), read_value(in)}};
            result = BTreeActionPayload{std::move(undo), read_effects(in)}; break;
        }
        case WalRecordType::Compensation: { const auto undo_of = in.u64(); const auto undo_next = in.u64(); result = CompensationPayload{undo_of, undo_next, read_effects(in)}; break; }
        case WalRecordType::SystemAction: { auto kind = checked_enum<SystemActionKind>(in.u8(), 1, 3, "Unknown system action kind"); result = SystemActionPayload{kind, read_effects(in)}; break; }
        default: throw std::runtime_error("Unknown WAL record type");
    }
    in.finish(); return result;
}

} // namespace WalPayloadCodec
