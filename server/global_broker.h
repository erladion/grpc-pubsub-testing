#ifndef GLOBAL_BROKER_H
#define GLOBAL_BROKER_H

#include <mutex>
#include <set>
#include <thread>

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

  void Register(CallData* client);
  void Unregister(CallData* client);
  void Broadcast(const broker::BrokerPayload& msg, CallData* sender = nullptr);

  void setBrokerId(const std::string& id) { m_brokerId = id; }

  void connectToPeer(const std::string& address);

  void injectRemoteMessage(const broker::BrokerPayload& msg);

private:
  GlobalBroker();
  ~GlobalBroker();

  void StatsLoop();

private:
  std::mutex m_mutex;
  std::set<CallData*> m_clients;

  ServerStats m_stats;

  std::atomic<bool> m_running;
  std::thread m_monitorThread;

  std::string m_brokerId;

  GrpcWorker* m_bridgeClient;
};

#endif  // GLOBAL_BROKER_H
