#pragma once

#include <Key.h>
#include <Log/WalRecord.h>
#include <PageV2.h>
#include <Value.h>

#include <array>
#include <cstdint>
#include <variant>
#include <vector>

enum class AbortReason : std::uint8_t { ClientRequest = 1, DeadlockVictim, StatementFailure, InternalError };
enum class BTreeActionKind : std::uint8_t { Insert = 1, Update, Delete };
enum class PageEffectKind : std::uint8_t { Write = 1, Allocate, Free };
enum class SystemActionKind : std::uint8_t { BTreeSplit = 1, BTreeMerge, PageMaintenance };

struct BeginPayload {};
// raft_index is the Raft log index this transaction applied, or 0 when the
// transaction applied no Raft entry. Raft indexes start at 1, so 0 is
// unambiguous. Recovery takes the highest value over committed transactions
// as last_applied.
struct CommitPayload { std::uint64_t raft_index = 0; };
struct EndPayload {};
struct AbortPayload { AbortReason reason; };
struct InsertUndo { Key key; };
struct UpdateUndo { Key key; Value old_value; };
struct DeleteUndo { Key key; Value old_value; };
using UndoDescriptor = std::variant<InsertUndo, UpdateUndo, DeleteUndo>;

struct PageEffect {
    PageEffectKind kind;
    std::uint32_t page_num;
    std::array<char, V2_PAGE_SIZE> after_image;
};

struct BTreeActionPayload { UndoDescriptor undo; std::vector<PageEffect> effects; };
struct CompensationPayload { Lsn undo_of_lsn; Lsn undo_next_lsn; std::vector<PageEffect> effects; };
struct SystemActionPayload { SystemActionKind kind; std::vector<PageEffect> effects; };
using WalPayload = std::variant<BeginPayload, BTreeActionPayload, CompensationPayload,
                                SystemActionPayload, CommitPayload, AbortPayload, EndPayload>;

BTreeActionKind action_kind(const UndoDescriptor& undo);
