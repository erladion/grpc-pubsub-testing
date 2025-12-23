#ifndef CALLDATA_H
#define CALLDATA_H

#include <grpcpp/grpcpp.h>

#include <deque>
#include <iostream>
#include <memory>
#include <mutex>
#include <set>
#include <unordered_set>

#include "global_broker.h"
#include "safe_logger.h"

#include "broker.grpc.pb.h"

using grpc::ServerAsyncReaderWriter;
using grpc::ServerCompletionQueue;
using grpc::ServerContext;

class CallData;

enum OpType { CONNECT, READ, WRITE };

struct Tag {
  std::shared_ptr<CallData> connection;
  OpType type;
};

class CallData : public std::enable_shared_from_this<CallData> {
  using Clock = std::chrono::steady_clock;

  const size_t MAX_QUEUE_BYTES = 50 * 1024 * 1024;
  const int MAX_MSGS_PER_SEC = 2000;

public:
  static void Create(broker::BrokerService::AsyncService* service, grpc::ServerCompletionQueue* cq) {
    auto client = std::shared_ptr<CallData>(new CallData(service, cq));
    client->Start();
  }

  bool IsSubscribed(const std::string& key);

  void Proceed(Tag* tag, bool ok);

  void AsyncSend(std::shared_ptr<broker::BrokerPayload> msg);

private:
  CallData(broker::BrokerService::AsyncService* service, ServerCompletionQueue* cq);

  bool CheckRateLimit();

  void Start();
  void HandleConnect(bool ok);
  void HandleRead(bool ok);
  void HandleWrite(bool ok);

  void WriteNextItem();

  void Stop();

private:
  broker::BrokerService::AsyncService* m_pService;
  ServerCompletionQueue* m_pCompletionQueue;
  ServerContext m_serverContext;

  broker::BrokerPayload m_incomingMessage;

  std::deque<std::shared_ptr<broker::BrokerPayload>> m_writeQueue;
  std::shared_ptr<broker::BrokerPayload> m_currentWriteMessagePtr;

  size_t m_currentQueueBytes;

  ServerAsyncReaderWriter<broker::BrokerPayload, broker::BrokerPayload> m_stream;

  std::mutex m_queueMutex;
  bool m_writeInProgress;

  Clock::time_point m_lastRateCheck;
  int m_msgCountInterval;

  std::unordered_set<std::string> m_subscriptions;
  std::mutex m_subscriptionMutex;

  std::string m_clientId;
  bool m_handshakeComplete;

  bool m_dying;
};

#endif  // CALLDATA_H
