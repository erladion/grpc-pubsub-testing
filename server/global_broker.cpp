#include "global_broker.h"
#include "calldata.h"

#include "grpcworker.h"

#include "safe_logger.h"

#include <QObject>

void GlobalBroker::Register(CallData* client) {
  std::unique_lock<std::shared_mutex> lock(m_clientMutex);
  m_clients.insert(client);
  m_stats.activeClients++;
}

void GlobalBroker::Unregister(CallData* client) {
  std::unique_lock<std::shared_mutex> lock(m_clientMutex);
  m_clients.erase(client);
  m_stats.activeClients--;
}

void GlobalBroker::Broadcast(const broker::BrokerPayload& msg, CallData* sender) {
  m_stats.totalMessagesProcessed++;
  m_stats.messagesThisInterval++;
  const size_t msgSize = msg.ByteSizeLong();
  m_stats.totalBytesProcessed += msgSize;
  m_stats.bytesThisInterval += msgSize;

  auto sharedMsg = std::make_shared<broker::BrokerPayload>(msg);

  bool shouldForwardToBridges = false;
  if (sharedMsg->origin_broker_id().empty()) {
    sharedMsg->set_origin_broker_id(m_brokerId);
    shouldForwardToBridges = true;
  } else if (sharedMsg->origin_broker_id() == m_brokerId) {
    shouldForwardToBridges = true;
  }

  {
    std::shared_lock<std::shared_mutex> lock(m_clientMutex);
    for (auto* client : m_clients) {
      if (client == sender) {
        continue;
      }

      if (client->IsSubscribed(sharedMsg->handler_key())) {
        client->AsyncSend(sharedMsg);
      }
    }
  }

  if (shouldForwardToBridges) {
    std::lock_guard<std::mutex> lock(m_peerMutex);
    for (GrpcWorker* peer : m_peers) {
      peer->writeMessage(*sharedMsg);
    }
  }
}

void GlobalBroker::connectToPeer(const std::string& address) {
  const QString qtAddress = QString::fromStdString(address);
  GrpcWorker* newPeer = new GrpcWorker(qtAddress);

  QObject::connect(newPeer, &GrpcWorker::envelopeReceived, [this](broker::BrokerPayload msg) { this->injectRemoteMessage(msg); });

  newPeer->start();

  {
    std::lock_guard<std::mutex> lock(m_peerMutex);
    m_peers.push_back(newPeer);
  }
}

void GlobalBroker::removePeer(GrpcWorker* peer) {
  std::lock_guard<std::mutex> lock(m_peerMutex);
  auto it = std::find(m_peers.begin(), m_peers.end(), peer);
  if (it != m_peers.end()) {
    m_peers.erase(it);
    peer->stop();
    peer->deleteLater();
    Logger::Log("Peer disconnected and removed");
  }
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

  std::lock_guard<std::mutex> lock(m_peerMutex);
  for (GrpcWorker* peer : m_peers) {
    peer->stop();
    peer->wait();
    delete peer;
  }
  m_peers.clear();
}

void GlobalBroker::StatsLoop() {
  while (m_running) {
    std::this_thread::sleep_for(std::chrono::seconds(1));

    const uint64_t messagePerSec = m_stats.messagesThisInterval.exchange(0);
    const uint64_t bytesPerSec = m_stats.bytesThisInterval.exchange(0);
    const int currentClients = m_stats.activeClients.load();

    const double kbSec = bytesPerSec / 1024.0;

    if (messagePerSec > 0 || currentClients > 0) {
      Logger::Log("[STATS] Clients: " + std::to_string(currentClients) + " | MPS: " + std::to_string(messagePerSec) +
                  " | Throughput: " + std::to_string(kbSec) + "KB/s");
    }
  }
}
