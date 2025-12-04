#ifndef SERVER_STATS_H
#define SERVER_STATS_H

#include <atomic>
#include <cstdint>

struct ServerStats {
  // Total counters (Lifetime)
  std::atomic<uint64_t> totalMessagesProcessed{0};
  std::atomic<uint64_t> totalBytesProcessed{0};

  // Interval counters (Reset every second)
  std::atomic<uint64_t> messagesThisInterval{0};
  std::atomic<uint64_t> bytesThisInterval{0};

  // State
  std::atomic<int> activeClients{0};
};

#endif  // SERVER_STATS_H
