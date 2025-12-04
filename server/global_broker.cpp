#include "global_broker.h"
#include "calldata.h"

#include "grpcworker.h"

#include <QObject>

void GlobalBroker::Register(CallData* client) {
  std::lock_guard<std::mutex> lock(m_mutex);
  m_clients.insert(client);

  m_stats.activeClients++;
}

void GlobalBroker::Unregister(CallData* client) {
  std::lock_guard<std::mutex> lock(m_mutex);
  m_clients.erase(client);

  m_stats.activeClients--;
}

void GlobalBroker::Broadcast(const broker::BrokerPayload& msg, CallData* sender) {
  m_stats.totalMessagesProcessed++;
  m_stats.messagesThisInterval++;

  const size_t msgSize = msg.handler_key().size() + msg.sender_id().size() + msg.topic().size() + msg.payload().ByteSizeLong();

  m_stats.totalBytesProcessed += msgSize;
  m_stats.bytesThisInterval += msgSize;

  broker::BrokerPayload forwardMsg = msg;
  bool shouldForwardToBridge = false;

  if (forwardMsg.origin_broker_id().empty()) {
    forwardMsg.set_origin_broker_id(m_brokerId);
    shouldForwardToBridge = true;
  } else if (forwardMsg.origin_broker_id() != m_brokerId) {
    shouldForwardToBridge = false;
  }

  {
    std::lock_guard<std::mutex> lock(m_mutex);
    for (auto* client : m_clients) {
      if (client == sender) {
        continue;
      }

      if (client->IsSubscribed(msg.handler_key())) {
        client->AsyncSend(msg);
      }
    }
  }

  if (m_bridgeClient && shouldForwardToBridge) {
    m_bridgeClient->writeMessage(forwardMsg);
  }
}

void GlobalBroker::connectToPeer(const std::string& address) {
  if (m_bridgeClient) {
    return;
  }

  QString qtAddress = QString::fromStdString(address);

  m_bridgeClient = new GrpcWorker(qtAddress);

  QObject::connect(m_bridgeClient, &GrpcWorker::envelopeReceived, [this](broker::BrokerPayload msg) { this->injectRemoteMessage(msg); });

  m_bridgeClient->start();
}

void GlobalBroker::injectRemoteMessage(const broker::BrokerPayload& msg) {
  Broadcast(msg, nullptr);
}

GlobalBroker::GlobalBroker() : m_running(true), m_monitorThread(std::thread(&GlobalBroker::StatsLoop, this)) {}

GlobalBroker::~GlobalBroker() {
  m_running = false;
  if (m_monitorThread.joinable()) {
    m_monitorThread.join();
  }
}

void GlobalBroker::StatsLoop() {
  while (m_running) {
    std::this_thread::sleep_for(std::chrono::seconds(1));

    const uint64_t messagePerSec = m_stats.messagesThisInterval.exchange(0);
    const uint64_t bytesPerSec = m_stats.bytesThisInterval.exchange(0);

    const int currentClients = m_stats.activeClients.load();

    const double kbSec = bytesPerSec / 1024.0;

    if (messagePerSec > 0 || currentClients > 0) {
      std::cout << "[STATS] Clients: " << currentClients << " | MPS: " << messagePerSec << " | Throughput: " << std::fixed << std::setprecision(2)
                << kbSec << "KB/s" << std::endl;
    }
  }
}
