#include "global_broker.h"
#include "calldata.h"
#include "grpcworker.h"
#include "safe_logger.h"

#include <random>
#include <sstream>

static std::string generateUUID() {
  static std::random_device rd;
  thread_local std::mt19937 gen(rd());
  thread_local std::uniform_int_distribution<> dis(0, 15);
  thread_local std::uniform_int_distribution<> dis2(8, 11);

  std::stringstream ss;
  ss << std::hex;
  for (int i = 0; i < 8; i++) {
    ss << dis(gen);
  }
  ss << "-";
  for (int i = 0; i < 4; i++) {
    ss << dis(gen);
  }
  ss << "-4";  // UUID version 4
  for (int i = 0; i < 3; i++) {
    ss << dis(gen);
  }
  ss << "-";
  ss << dis2(gen);  // UUID variant
  for (int i = 0; i < 3; i++) {
    ss << dis(gen);
  }
  ss << "-";
  for (int i = 0; i < 12; i++) {
    ss << dis(gen);
  }
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
    if (client->isSubscribed(sharedMsg->topic())) {
      client->asyncSend(sharedMsg);
    }
  }

  // Bridge Flooding
  {
    std::lock_guard<std::mutex> lock(m_peerMutex);
    for (const auto& peerPtr : m_peers) {
      GrpcWorker* peer = peerPtr.get();
      if (peer == sourcePeer) {
        continue;
      }
      peer->writeMessage(*sharedMsg);
    }
  }
}

void GlobalBroker::connectToPeer(const std::string& address) {
  ConnectionConfig config;
  config.address = address;
  config.clientId = "BrokerPeer";
  config.compressionAlgo = 2;  // GZIP
  config.keepAliveTime = 10000;
  config.keepAliveTimeout = 5000;

  auto newPeer = std::make_unique<GrpcWorker>(config, nullptr, nullptr);
  GrpcWorker* peerPtr = newPeer.get();
  newPeer->setMessageCallback([this, peerPtr](const broker::BrokerPayload& msg) { this->injectRemoteMessage(msg, peerPtr); });
  newPeer->start();

  {
    std::lock_guard<std::mutex> lock(m_peerMutex);
    m_peers.push_back(std::move(newPeer));
  }
  Logger::Log(Logger::Type::Info, "Connected to Peer: " + address);
}

void GlobalBroker::removePeer(GrpcWorker* peer) {
  std::lock_guard<std::mutex> lock(m_peerMutex);
  auto it = std::find_if(m_peers.begin(), m_peers.end(), [peer](const std::unique_ptr<GrpcWorker>& p) { return p.get() == peer; });
  if (it != m_peers.end()) {
    (*it)->stop();
    m_peers.erase(it);
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
  for (const auto& peerPtr : m_peers) {
    GrpcWorker* peer = peerPtr.get();
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

    ss << ", \"connected_clients\": [";

    {
      std::shared_lock<std::shared_mutex> lock(m_clientMutex);
      bool firstClient = true;
      for (const auto& client : m_clients) {
        if (!firstClient)
          ss << ",";
        firstClient = false;

        ss << "{";
        ss << "\"id\": \"" << client->clientId() << "\",";
        ss << "\"subscriptions\": [";

        std::vector<std::string> subs = client->getSubscriptions();
        bool firstSub = true;
        for (const auto& topic : subs) {
          if (!firstSub)
            ss << ",";
          firstSub = false;
          ss << "\"" << topic << "\"";
        }
        ss << "]";
        ss << "}";
      }
    }

    ss << "]";
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
