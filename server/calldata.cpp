#include "calldata.h"

CallData::CallData(broker::BrokerService::AsyncService* service, ServerCompletionQueue* cq)
    : m_pService(service), m_pCompletionQueue(cq), m_stream(&m_serverContext), m_writeInProgress(false), m_currentQueueBytes(0),
      m_msgCountInterval(0), m_dying(false) {
  m_serverContext.set_compression_algorithm(GRPC_COMPRESS_GZIP);
  m_lastRateCheck = std::chrono::steady_clock::now();
}

void CallData::Start() {
  Tag* tag = new Tag{shared_from_this(), CONNECT};
  m_pService->RequestMessageStream(&m_serverContext, &m_stream, m_pCompletionQueue, m_pCompletionQueue, tag);
}

bool CallData::IsSubscribed(const std::string& key) {
  std::lock_guard<std::mutex> lock(m_subscriptionMutex);
  return m_subscriptions.find(key) != m_subscriptions.end();
}

void CallData::Proceed(Tag* tag, bool ok) {
  std::unique_ptr<Tag> tagGuard(tag);

  if (m_dying) {
    return;
  }

  switch (tag->type) {
    case CONNECT:
      HandleConnect(ok);
      break;
    case READ:
      HandleRead(ok);
      break;
    case WRITE:
      HandleWrite(ok);
      break;
  }
}

void CallData::AsyncSend(std::shared_ptr<broker::BrokerPayload> msg) {
  std::lock_guard<std::mutex> lock(m_queueMutex);

  if (m_dying) {
    return;
  }

  if (m_currentQueueBytes > MAX_QUEUE_BYTES) {
    Logger::Log(Logger::Type::Error, "Client" + m_clientId + " is too slow. Dropping message.");

    Stop();
    return;
  }

  m_writeQueue.push_back(msg);
  m_currentQueueBytes += msg->ByteSizeLong();

  if (!m_writeInProgress) {
    WriteNextItem();
  }
}

bool CallData::CheckRateLimit() {
  auto now = Clock::now();
  auto diff = std::chrono::duration_cast<std::chrono::milliseconds>(now - m_lastRateCheck).count();
  if (diff > 1000) {
    m_msgCountInterval = 0;
    m_lastRateCheck = now;
  }
  m_msgCountInterval++;
  return m_msgCountInterval <= MAX_MSGS_PER_SEC;
}

void CallData::HandleConnect(bool ok) {
  if (!ok) {
    return;
  }

  CallData::Create(m_pService, m_pCompletionQueue);
  GlobalBroker::instance().Register(shared_from_this());

  Logger::Log(Logger::Type::Info, "New Client Connection Established");

  Tag* tag = new Tag{shared_from_this(), READ};
  m_stream.Read(&m_incomingMessage, tag);
}

void CallData::HandleRead(bool ok) {
  if (!ok) {
    Stop();
    return;
  }

  if (!CheckRateLimit()) {
    Logger::Log(Logger::Type::Error, "Rate limit exceeded for " + m_clientId + ". Ignoring message.");
    Tag* tag = new Tag{shared_from_this(), READ};
    m_stream.Read(&m_incomingMessage, tag);
    return;
  }

  std::string key = m_incomingMessage.handler_key();

  if (!m_handshakeComplete) {
    if (!m_incomingMessage.sender_id().empty()) {
      m_handshakeComplete = true;
      m_clientId = m_incomingMessage.sender_id();
      Logger::Log(Logger::Type::Info, "Handshake successful for client: " + m_clientId);
    } else {
      Logger::Log(Logger::Type::Error, "Client attempted data transfer before handshake.");
      Stop();
      return;
    }
  }
  if (key == "__SUBSCRIBE__") {
    if (!m_incomingMessage.sender_id().empty()) {
      m_clientId = m_incomingMessage.sender_id();
    }

    std::string topic = m_incomingMessage.topic();

    if (topic.empty()) {
      Logger::Log(Logger::Type::Error, "Client " + m_clientId + " sent empty subscription topic");
    } else {
      std::lock_guard<std::mutex> lock(m_subscriptionMutex);
      m_subscriptions.insert(topic);
      Logger::Log(Logger::Type::Info, "Client " + m_clientId + " subscribed to: " + topic);
    }
  } else {
    if (m_clientId == "Unknown" && !m_incomingMessage.sender_id().empty()) {
      m_clientId = m_incomingMessage.sender_id();
    }

    GlobalBroker::instance().Broadcast(m_incomingMessage, this);
  }

  Tag* tag = new Tag{shared_from_this(), READ};
  m_stream.Read(&m_incomingMessage, tag);
}

void CallData::HandleWrite(bool ok) {
  std::lock_guard<std::mutex> lock(m_queueMutex);
  m_writeInProgress = false;

  if (!ok) {
    Stop();
    return;
  }

  if (!m_writeQueue.empty()) {
    WriteNextItem();
  }
}

void CallData::WriteNextItem() {
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

  Tag* tag = new Tag{shared_from_this(), WRITE};
  m_stream.Write(*m_currentWriteMessagePtr, tag);
}

void CallData::Stop() {
  if (m_dying) {
    return;
  }
  m_dying = true;

  GlobalBroker::instance().Unregister(shared_from_this());
  m_serverContext.TryCancel();
}
