#include "grpcconnectionmanager.h"

#include <filesystem>
#include <fstream>
#include <iostream>
#include <random>

GrpcConnectionManager* GrpcConnectionManager::m_instance = nullptr;
std::mutex GrpcConnectionManager::m_initMutex;

std::vector<std::pair<std::string, MessageCallback>> GrpcConnectionManager::s_pendingMsgCallbacks;
std::vector<std::pair<std::string, FileCallback>> GrpcConnectionManager::s_pendingFileCallbacks;
std::vector<StatusCallback> GrpcConnectionManager::s_pendingStatusCallbacks;

static std::string generateUUID() {
  static std::random_device rd;
  static std::mt19937 gen(rd());
  static std::uniform_int_distribution<> dis(0, 15);
  static std::uniform_int_distribution<> dis2(8, 11);

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

void GrpcConnectionManager::init(const std::string& clientId, const std::string& address) {
  std::lock_guard<std::mutex> lock(m_initMutex);
  if (!m_instance) {
    m_instance = new GrpcConnectionManager(address, clientId);

    for (auto& p : s_pendingMsgCallbacks) {
      m_instance->registerInternal(p.first, p.second);
    }
    s_pendingMsgCallbacks.clear();

    for (auto& p : s_pendingFileCallbacks) {
      m_instance->registerFileInternal(p.first, p.second);
    }
    s_pendingFileCallbacks.clear();

    {
      std::lock_guard<std::mutex> lock(m_instance->m_mapMutex);
      for (auto& cb : s_pendingStatusCallbacks) {
        m_instance->m_statusHandlers.push_back(cb);
      }
    }
    s_pendingStatusCallbacks.clear();
  }
}

void GrpcConnectionManager::shutdown() {
  std::lock_guard<std::mutex> lock(m_initMutex);
  if (m_instance) {
    delete m_instance;
    m_instance = nullptr;
  }
}

GrpcConnectionManager& GrpcConnectionManager::instance() {
  return *m_instance;
}

bool GrpcConnectionManager::sendMessage(const std::string& key, const std::string& message) {
  return instance().sendDataInternal(key, message);
}

bool GrpcConnectionManager::sendData(const std::string& key, const std::string_view& data) {
  return instance().sendDataInternal(key, data);
}

bool GrpcConnectionManager::sendDataRaw(const std::string& key, const char* data, int len) {
  return instance().sendDataInternal(key, std::string(data, len));
}

bool GrpcConnectionManager::sendFile(const std::string& key, const std::string& filepath) {
  return instance().sendFileInternal(key, filepath);
}

void GrpcConnectionManager::registerCallback(const std::string& key, MessageCallback callback) {
  std::lock_guard<std::mutex> lock(m_initMutex);
  if (m_instance) {
    instance().registerInternal(key, callback);
  } else {
    s_pendingMsgCallbacks.push_back({key, callback});
  }
}

void GrpcConnectionManager::registerFileCallback(const std::string& key, FileCallback callback) {
  instance().registerFileInternal(key, callback);
}

void GrpcConnectionManager::registerStatusCallback(StatusCallback callback) {
  std::lock_guard<std::mutex> lock(m_initMutex);
  if (m_instance) {
    std::lock_guard<std::mutex> mapLock(instance().m_mapMutex);
    instance().m_statusHandlers.push_back(callback);
  } else {
    s_pendingStatusCallbacks.push_back(callback);
  }
}

GrpcConnectionManager::GrpcConnectionManager(const std::string& address, const std::string& clientId) : m_clientId(clientId), m_running(true) {
  WorkerConfig config;
  config.targetAddress = address;

  m_worker = new GrpcWorker(config, &m_queue, [this](bool connected) {
    std::lock_guard<std::mutex> lock(m_mapMutex);
    std::cout << "[Client] Connection Status: " << (connected ? "ONLINE" : "OFFLINE") << std::endl;

    fflush(stdout);

    for (auto& callback : m_statusHandlers) {
      callback(connected);
    }

    if (connected) {
      std::cout << "[Client] Re-sending subscriptions..." << std::endl;
      for (auto const& [topic, _] : m_msgHandlers) {
        broker::BrokerPayload sub;
        sub.set_handler_key("__SUBSCRIBE__");
        sub.set_sender_id(m_clientId);
        sub.set_topic(topic);
        m_worker->writeMessage(sub);
      }

      for (auto const& [topic, _] : m_fileHandlers) {
        broker::BrokerPayload sub;
        sub.set_handler_key("__SUBSCRIBE__");
        sub.set_sender_id(m_clientId);
        sub.set_topic(topic);
        m_worker->writeMessage(sub);
      }
    }
  });

  m_worker->start();
  m_processingThread = std::thread(&GrpcConnectionManager::processingLoop, this);
}

GrpcConnectionManager::~GrpcConnectionManager() {
  m_running = false;
  m_queue.stop();
  if (m_processingThread.joinable()) {
    m_processingThread.join();
  }
  delete m_worker;
}

bool GrpcConnectionManager::sendRawEnvelope(const broker::BrokerPayload& envelope) {
  return m_worker->writeMessage(envelope);
}

bool GrpcConnectionManager::sendDataInternal(const std::string& key, const std::string_view& data) {
  broker::BrokerPayload msg;
  msg.set_handler_key(key);
  msg.set_sender_id(m_clientId);
  msg.set_topic(key);
  msg.set_raw_data(data.data(), data.size());
  return sendRawEnvelope(msg);
}

void GrpcConnectionManager::registerInternal(const std::string& key, MessageCallback callback) {
  std::lock_guard<std::mutex> lock(m_mapMutex);
  m_msgHandlers[key].push_back(callback);

  broker::BrokerPayload sub;
  sub.set_handler_key("__SUBSCRIBE__");
  sub.set_sender_id(m_clientId);
  sub.set_topic(key);
  sendRawEnvelope(sub);
}

void GrpcConnectionManager::registerFileInternal(const std::string& key, FileCallback callback) {
  std::lock_guard<std::mutex> lock(m_mapMutex);
  m_fileHandlers[key].push_back(callback);

  broker::BrokerPayload sub;
  sub.set_handler_key("__SUBSCRIBE__");
  sub.set_sender_id(m_clientId);
  sub.set_topic(key);
  sendRawEnvelope(sub);
}

void GrpcConnectionManager::processingLoop() {
  broker::BrokerPayload msg;
  while (m_queue.pop(msg)) {
    if (!m_running) {
      break;
    }
    std::string key = msg.handler_key();

    if (key == "__CHUNK__" || key == "__FILE_META__" || key == "__FILE_FOOTER__") {
      handleFilePacket(msg);
    } else {
      handleMessage(msg);
    }
  }
}

void GrpcConnectionManager::handleMessage(const broker::BrokerPayload& msg) {
  std::string topic = msg.topic();
  std::string data;

  if (msg.has_payload()) {
    data = msg.payload().value();
  } else {
    data = msg.raw_data();
  }

  std::vector<MessageCallback> callbacks;
  {
    std::lock_guard<std::mutex> lock(m_mapMutex);
    if (m_msgHandlers.count(topic)) {
      callbacks = m_msgHandlers[topic];
    }
  }

  for (auto& callback : callbacks) {
    callback(data);
  }
}

bool GrpcConnectionManager::sendFileInternal(const std::string& key, const std::string& filePath) {
  std::filesystem::path path(filePath);
  if (!std::filesystem::exists(path)) {
    std::cerr << "[Manager] File not found: " << filePath << std::endl;
    return false;
  }

  std::string filename = path.filename().string();
  size_t fileSize = std::filesystem::file_size(path);
  std::string transferId = generateUUID();

  std::ifstream inputFile(filePath, std::ios::binary);
  if (!inputFile.is_open()) {
    return false;
  }

  std::stringstream metaJson;
  metaJson << "{\"filename\":\"" << filename << "\",\"size\":" << fileSize << "}";

  broker::BrokerPayload metaMsg;
  metaMsg.set_handler_key("__FILE_META__");
  metaMsg.set_topic(key);
  metaMsg.set_sender_id(m_clientId);
  metaMsg.set_transfer_id(transferId);
  metaMsg.set_raw_data(metaJson.str());

  if (!m_worker->writeMessage(metaMsg)) {
    return false;
  }

  const size_t CHUNK_SIZE = 64 * 1024;  // 64KB per chunk
  std::vector<char> buffer(CHUNK_SIZE);

  while (inputFile.read(buffer.data(), CHUNK_SIZE) || inputFile.gcount() > 0) {
    broker::BrokerPayload chunkMsg;
    chunkMsg.set_handler_key("__CHUNK__");
    chunkMsg.set_topic(key);
    chunkMsg.set_sender_id(m_clientId);
    chunkMsg.set_transfer_id(transferId);

    chunkMsg.set_raw_data(buffer.data(), inputFile.gcount());

    m_worker->writeMessage(chunkMsg);
  }

  broker::BrokerPayload footerMsg;
  footerMsg.set_handler_key("__FILE_FOOTER__");
  footerMsg.set_topic(key);
  footerMsg.set_sender_id(m_clientId);
  footerMsg.set_transfer_id(transferId);
  m_worker->writeMessage(footerMsg);

  std::cout << "[Manager] Sent file: " << filename << " (" << fileSize << " bytes)" << std::endl;
  return true;
}

void GrpcConnectionManager::handleFilePacket(const broker::BrokerPayload& msg) {
  std::string type = msg.handler_key();
  std::string id = msg.transfer_id();

  std::lock_guard<std::mutex> lock(m_mapMutex);

  if (type == "__FILE_META__") {
    auto state = std::make_shared<FileTransferState>();
    state->originalTopic = msg.topic();
    state->receivedSize = 0;

    std::string meta = msg.raw_data();

    size_t nameStart = meta.find("\"filename\":\"");
    if (nameStart != std::string::npos) {
      nameStart += 12;
      size_t nameEnd = meta.find("\"", nameStart);
      state->destFilename = meta.substr(nameStart, nameEnd - nameStart);
    } else {
      state->destFilename = "unknown_" + id + ".bin";
    }

    state->destFilename = std::filesystem::path(state->destFilename).filename().string();

    state->tempPath = (std::filesystem::temp_directory_path() / (id + ".part")).string();
    state->fileHandle.open(state->tempPath, std::ios::binary);

    if (!state->fileHandle.is_open()) {
      std::cerr << "[Manager] Failed to create temp file: " << state->tempPath << std::endl;
      return;
    }

    m_transfers[id] = state;
    std::cout << "[Manager] Starting download: " << state->destFilename << std::endl;
  }

  else if (type == "__CHUNK__") {
    if (m_transfers.find(id) == m_transfers.end()) {
      return;
    }

    auto& state = m_transfers[id];
    state->fileHandle.write(msg.raw_data().data(), msg.raw_data().size());
    state->receivedSize += msg.raw_data().size();
  }

  else if (type == "__FILE_FOOTER__") {
    if (m_transfers.find(id) == m_transfers.end()) {
      return;
    }

    auto state = m_transfers[id];
    state->fileHandle.close();

    std::filesystem::path downloadDir = std::filesystem::current_path() / "downloads";
    if (!std::filesystem::exists(downloadDir)) {
      std::filesystem::create_directory(downloadDir);
    }

    std::filesystem::path finalPath = downloadDir / state->destFilename;

    try {
      if (std::filesystem::exists(finalPath)) {
        std::filesystem::remove(finalPath);
      }
      std::filesystem::rename(state->tempPath, finalPath);

      std::cout << "[Manager] File saved to: " << finalPath << std::endl;

      if (m_fileHandlers.count(state->originalTopic)) {
        for (auto& callback : m_fileHandlers[state->originalTopic]) {
          callback(finalPath.string());
        }
      }

    } catch (const std::filesystem::filesystem_error& e) {
      std::cerr << "[Manager] File move failed: " << e.what() << std::endl;
    }

    m_transfers.erase(id);
  }
}
