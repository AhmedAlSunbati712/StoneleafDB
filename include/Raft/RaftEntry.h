#pragma once

#include <Key.h>
#include <Value.h>

#include <cstdint>
#include <variant>
#include <vector>

enum class RaftMutationType : std::uint8_t {
    Put = 0,
    Delete,
};

struct PutMutation {
    Key key;
    Value value;
};

struct DeleteMutation {
    Key key;
};

struct MutationOp {
    RaftMutationType type;
    std::variant<PutMutation, DeleteMutation> operation;
};

struct RaftMutationEntry {
    std::uint64_t term;
    std::uint64_t idx;
    std::vector<MutationOp> operations;
};
