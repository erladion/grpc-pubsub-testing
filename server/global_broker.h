#ifndef GLOBAL_BROKER_H
#define GLOBAL_BROKER_H

#include <algorithm>
#include <memory>
#include <mutex>
#include <set>
#include <shared_mutex>
#include <thread>
#include <vector>

#include "server_stats.h"

#include "broker.grpc.pb.h"

class CallData;
class GrpcWorker;

class GlobalBroker {
public:
  static GlobalBroker& instance() {
    static GlobalBroker inst;
    return inst;
  }

  void Register(std::shared_ptr<CallData> client);
  void Unregister(std::shared_ptr<CallData> client);
  void Broadcast(const broker::BrokerPayload& msg, CallData* sender, GrpcWorker* sourcePeer = nullptr);

  void setBrokerId(const std::string& id) { m_brokerId = id; }

  void connectToPeer(const std::string& address);
  void removePeer(GrpcWorker* peer);

  void injectRemoteMessage(const broker::BrokerPayload& msg, GrpcWorker* sourcePeer = nullptr);

private:
  GlobalBroker();
  ~GlobalBroker();

  void StatsLoop();

private:
  std::shared_mutex m_clientMutex;
  std::set<std::shared_ptr<CallData>> m_clients;

  std::mutex m_peerMutex;
  std::vector<GrpcWorker*> m_peers;

  ServerStats m_stats;
  std::atomic<bool> m_running;
  std::thread m_monitorThread;
  std::string m_brokerId;

  std::mutex m_historyMutex;
  std::unordered_set<std::string> m_seenMessageIds;
  std::deque<std::string> m_messageIdOrder;
};

#endif  // GLOBAL_BROKER_H
