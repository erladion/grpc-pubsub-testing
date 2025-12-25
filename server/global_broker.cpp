#include "global_broker.h"
#include "calldata.h"
#include "grpcworker.h"
#include "safe_logger.h"

#include <random>
#include <sstream>

static std::string generateUUID() {
  static std::random_device rd;
  static std::mt19937 gen(rd());
  static std::uniform_int_distribution<> dis(0, 15);
  static std::uniform_int_distribution<> dis2(8, 11);

  std::stringstream ss;
  ss << std::hex;
  for (int i = 0; i < 8; i++)
    ss << dis(gen);
  ss << "-";
  for (int i = 0; i < 4; i++)
    ss << dis(gen);
  ss << "-4";  // UUID version 4
  for (int i = 0; i < 3; i++)
    ss << dis(gen);
  ss << "-";
  ss << dis2(gen);  // UUID variant
  for (int i = 0; i < 3; i++)
    ss << dis(gen);
  ss << "-";
  for (int i = 0; i < 12; i++)
    ss << dis(gen);
  return ss.str();
}

void GlobalBroker::Register(std::shared_ptr<CallData> client) {
  std::unique_lock<std::shared_mutex> lock(m_clientMutex);
  m_clients.insert(client);
  m_stats.activeClients++;
}

void GlobalBroker::Unregister(std::shared_ptr<CallData> client) {
  std::unique_lock<std::shared_mutex> lock(m_clientMutex);
  m_clients.erase(client);
  m_stats.activeClients--;
}

void GlobalBroker::Broadcast(const broker::BrokerPayload& msg, CallData* sender, GrpcWorker* sourcePeer) {
  broker::BrokerPayload forwardMsg = msg;

  std::string uniqueId = forwardMsg.message_uuid();
  if (uniqueId.empty()) {
    uniqueId = generateUUID();
    forwardMsg.set_message_uuid(uniqueId);
  }

  {
    std::lock_guard<std::mutex> lock(m_historyMutex);

    // If we have seen this ID, it's a loop or a duplicate. DROP IT.
    if (m_seenMessageIds.count(uniqueId)) {
      return;
    }

    // Mark as seen
    m_seenMessageIds.insert(uniqueId);
    m_messageIdOrder.push_back(uniqueId);

    // Cleanup: Keep last 10,000 messages to prevent RAM leak
    if (m_messageIdOrder.size() > 10000) {
      std::string oldId = m_messageIdOrder.front();
      m_messageIdOrder.pop_front();
      m_seenMessageIds.erase(oldId);
    }
  }

  m_stats.totalMessagesProcessed++;
  m_stats.messagesThisInterval++;
  size_t msgSize = forwardMsg.ByteSizeLong();
  m_stats.totalBytesProcessed += msgSize;
  m_stats.bytesThisInterval += msgSize;

  if (forwardMsg.origin_broker_id().empty()) {
    forwardMsg.set_origin_broker_id(m_brokerId);
  }

  auto sharedMsg = std::make_shared<broker::BrokerPayload>(std::move(forwardMsg));

  // Local Delivery
  std::vector<std::shared_ptr<CallData>> targets;
  {
    std::shared_lock<std::shared_mutex> lock(m_clientMutex);
    targets.reserve(m_clients.size());

    for (const auto& client : m_clients) {
      if (client.get() != sender) {
        targets.push_back(client);
      }
    }
  }

  for (auto& client : targets) {
    if (client->IsSubscribed(sharedMsg->topic())) {
      client->AsyncSend(sharedMsg);
    }
  }

  // Bridge Flooding
  {
    std::lock_guard<std::mutex> lock(m_peerMutex);
    for (GrpcWorker* peer : m_peers) {
      if (peer == sourcePeer) {
        continue;
      }
      peer->writeMessage(*sharedMsg);
    }
  }
}

void GlobalBroker::connectToPeer(const std::string& address) {
  WorkerConfig config;
  config.targetAddress = address;
  config.compressionAlgo = 2;  // GZIP
  config.keepAliveTime = 10000;
  config.keepAliveTimeout = 5000;

  GrpcWorker* newPeer = new GrpcWorker(config, nullptr, nullptr);
  newPeer->setMessageCallback([this, newPeer](const broker::BrokerPayload& msg) { this->injectRemoteMessage(msg, newPeer); });
  newPeer->start();

  {
    std::lock_guard<std::mutex> lock(m_peerMutex);
    m_peers.push_back(newPeer);
  }
  Logger::Log(Logger::Type::Info, "Connected to Peer: " + address);
}

void GlobalBroker::removePeer(GrpcWorker* peer) {
  std::lock_guard<std::mutex> lock(m_peerMutex);
  auto it = std::find(m_peers.begin(), m_peers.end(), peer);
  if (it != m_peers.end()) {
    m_peers.erase(it);
    peer->stop();
    delete peer;
    Logger::Log(Logger::Type::Info, "Peer disconnected and removed");
  }
}

void GlobalBroker::injectRemoteMessage(const broker::BrokerPayload& msg, GrpcWorker* sourcePeer) {
  Broadcast(msg, nullptr, sourcePeer);
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
      Logger::Log(Logger::Type::Info, "[STATS] Clients: " + std::to_string(currentClients) + " | Peers: " + std::to_string(m_peers.size()) +
                                          " | MPS: " + std::to_string(messagePerSec) + " | Throughput: " + std::to_string(kbSec) + " KB/s");
    }

    std::stringstream ss;
    ss << "{";
    ss << "\"type\":\"stats_update\",";
    ss << "\"broker_id\":\"" << m_brokerId << "\",";
    ss << "\"clients\":" << currentClients << ",";
    ss << "\"peers_count\":" << m_peers.size() << ",";
    ss << "\"msgs_per_sec\":" << messagePerSec << ",";
    ss << "\"kb_per_sec\":" << kbSec << ",";
    ss << "\"total_msgs\":" << m_stats.totalMessagesProcessed.load() << ",";
    ss << "\"uptime_sec\":0";
    ss << "}";

    broker::BrokerPayload msg;
    msg.set_topic("__SYS_STATS__");
    msg.set_handler_key("__SYS_STATS__");
    msg.set_sender_id("BROKER_SYSTEM");
    msg.set_origin_broker_id(m_brokerId);
    msg.set_raw_data(ss.str());

    Broadcast(msg, nullptr, nullptr);
  }
}
