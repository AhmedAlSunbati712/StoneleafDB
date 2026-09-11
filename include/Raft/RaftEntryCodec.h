#pragma once

#include <Raft/RaftEntry.h>

#include <cstddef>
#include <span>
#include <vector>

namespace RaftEntryCodec {

inline constexpr std::size_t TERM_OFFSET = 0;
inline constexpr std::size_t INDEX_OFFSET = 8;
inline constexpr std::size_t OPERATION_COUNT_OFFSET = 16;
inline constexpr std::size_t HEADER_SIZE = 20;

std::vector<char> encode(const RaftMutationEntry& entry);
RaftMutationEntry decode(std::span<const char> encoded);

} // namespace RaftEntryCodec
