#ifndef CALLDATA_H
#define CALLDATA_H

#include <grpcpp/grpcpp.h>

#include <deque>
#include <iostream>
#include <mutex>
#include <set>
#include <unordered_set>

#include "async_structs.h"
#include "global_broker.h"
#include "safe_logger.h"

#include "broker.grpc.pb.h"

using grpc::ServerAsyncReaderWriter;
using grpc::ServerCompletionQueue;
using grpc::ServerContext;

class CallData {
  using Clock = std::chrono::steady_clock;

  const size_t MAX_QUEUE_BYTES = 50 * 1024 * 1024;
  const int MAX_MSGS_PER_SEC = 2000;

public:
  CallData(broker::BrokerService::AsyncService* service, ServerCompletionQueue* cq)
      : m_pService(service), m_pCompletionQueue(cq), m_stream(&m_serverContext), m_status(CONNECT), m_writeInProgress(false), m_currentQueueBytes(0),
        m_msgCountInterval(0) {
    m_serverContext.set_compression_algorithm(GRPC_COMPRESS_GZIP);
    m_connectTag = {this, CONNECT};
    m_readTag = {this, READ};
    m_writeTag = {this, WRITE};

    m_pService->RequestMessageStream(&m_serverContext, &m_stream, m_pCompletionQueue, m_pCompletionQueue, &m_connectTag);
  }

  bool IsSubscribed(const std::string& key) {
    std::lock_guard<std::mutex> lock(m_subscriptionMutex);
    return m_subscriptions.find(key) != m_subscriptions.end();
  }

  void Proceed(Tag* tag, bool ok) {
    if (tag->type == CONNECT) {
      HandleConnect(ok);
    } else if (tag->type == READ) {
      HandleRead(ok);
    } else if (tag->type == WRITE) {
      HandleWrite(ok);
    }
  }

  void AsyncSend(std::shared_ptr<broker::BrokerPayload> msg) {
    std::lock_guard<std::mutex> lock(m_queueMutex);

    if (m_currentQueueBytes > MAX_QUEUE_BYTES) {
      Logger::LogError("Client" + m_clientId + " is too slow. Dropping message.");

      m_status = WRITE;
      delete this;
      return;
    }

    m_writeQueue.push_back(msg);
    m_currentQueueBytes += msg->ByteSizeLong();

    if (!m_writeInProgress) {
      WriteNextItem();
    }
  }

private:
  bool CheckRateLimit() {
    auto now = Clock::now();
    auto diff = std::chrono::duration_cast<std::chrono::milliseconds>(now - m_lastRateCheck).count();
    if (diff > 1000) {
      m_msgCountInterval = 0;
      m_lastRateCheck = now;
    }
    m_msgCountInterval++;
    return m_msgCountInterval <= MAX_MSGS_PER_SEC;
  }

  void HandleConnect(bool ok) {
    if (!ok) {
      delete this;
      return;
    }

    new CallData(m_pService, m_pCompletionQueue);
    GlobalBroker::instance().Register(this);

    Logger::Log("New Client Connection Established");

    m_status = READ;
    m_stream.Read(&m_incomingMessage, &m_readTag);
  }

  void HandleRead(bool ok) {
    if (!ok) {
      Cleanup();
      return;
    }

    if (!CheckRateLimit()) {
      Logger::LogError("Rate limit exceeded for " + m_clientId + ". Ignoring message.");
      m_stream.Read(&m_incomingMessage, &m_readTag);
      return;
    }

    std::string key = m_incomingMessage.handler_key();

    if (!m_handshakeComplete) {
      if (key == "__SUBSCRIBE__" && !m_incomingMessage.sender_id().empty()) {
        m_handshakeComplete = true;
        Logger::Log("Handshake successful for client: " + m_clientId);
      } else {
        Logger::LogError("Client attempted data transfer before handshake.");
        Cleanup();
        return;
      }
    }
    if (key == "__SUBSCRIBE__") {
      if (!m_incomingMessage.sender_id().empty()) {
        m_clientId = m_incomingMessage.sender_id();
      }

      std::string topic = m_incomingMessage.topic();

      if (topic.empty()) {
        Logger::LogError("Client " + m_clientId + " sent empty subscription topic");
      } else {
        std::lock_guard<std::mutex> lock(m_subscriptionMutex);
        m_subscriptions.insert(topic);
        Logger::Log("Client " + m_clientId + " subscribed to: " + topic);
      }

      m_stream.Read(&m_incomingMessage, &m_readTag);
      return;
    }

    if (m_clientId == "Unknow" && !m_incomingMessage.sender_id().empty()) {
      m_clientId = m_incomingMessage.sender_id();
    }

    GlobalBroker::instance().Broadcast(m_incomingMessage, this);

    m_stream.Read(&m_incomingMessage, &m_readTag);
  }

  void HandleWrite(bool ok) {
    std::lock_guard<std::mutex> lock(m_queueMutex);
    m_writeInProgress = false;
    if (!ok) {
      return;
    }

    if (!m_writeQueue.empty()) {
      WriteNextItem();
    }
  }

  void WriteNextItem() {
    if (m_writeQueue.empty()) {
      return;
    }

    m_currentWriteMessagePtr = m_writeQueue.front();
    m_writeQueue.pop_front();
    m_writeInProgress = true;

    size_t size = m_currentWriteMessagePtr->ByteSizeLong();
    if (m_currentQueueBytes >= size) {
      m_currentQueueBytes -= size;
    } else {
      m_currentQueueBytes = 0;
    }

    grpc::WriteOptions options;
    if (size <= 1024) {
      options.set_no_compression();
    }

    m_stream.Write(*m_currentWriteMessagePtr, &m_writeTag);
  }

  void Cleanup() {
    GlobalBroker::instance().Unregister(this);
    delete this;
  }

private:
  broker::BrokerService::AsyncService* m_pService;
  ServerCompletionQueue* m_pCompletionQueue;
  ServerContext m_serverContext;

  broker::BrokerPayload m_incomingMessage;

  std::deque<std::shared_ptr<broker::BrokerPayload>> m_writeQueue;
  std::shared_ptr<broker::BrokerPayload> m_currentWriteMessagePtr;

  size_t m_currentQueueBytes;

  ServerAsyncReaderWriter<broker::BrokerPayload, broker::BrokerPayload> m_stream;

  OpType m_status;
  std::string m_clientId;

  bool m_writeInProgress;
  std::mutex m_queueMutex;

  std::unordered_set<std::string> m_subscriptions;
  std::mutex m_subscriptionMutex;

  bool m_handshakeComplete;
  Clock::time_point m_lastRateCheck;
  int m_msgCountInterval;

  Tag m_connectTag;
  Tag m_readTag;
  Tag m_writeTag;
};

#endif  // CALLDATA_H
