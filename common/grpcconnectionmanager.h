#ifndef GRPCCONNECTIONMANAGER_H
#define GRPCCONNECTIONMANAGER_H

#include <fstream>
#include <functional>
#include <iostream>
#include <map>
#include <mutex>
#include <string>
#include <type_traits>
#include <vector>

#include <google/protobuf/any.pb.h>
#include <google/protobuf/message.h>

#include "grpcworker.h"
#include "safequeue.h"

using MessageCallback = std::function<void(const std::string&)>;
using FileCallback = std::function<void(const std::string&)>;
using StatusCallback = std::function<void(bool)>;

struct FileTransferState {
  std::ofstream fileHandle;
  std::string destFilename;
  std::string tempPath;
  std::string originalTopic;
  size_t totalSize;
  size_t receivedSize;
};

class GrpcConnectionManager {
public:
  static void init(const ConnectionConfig& config);
  static void shutdown();
  static GrpcConnectionManager& instance();

  static bool sendMessage(const std::string& key, const std::string& message);
  static bool sendData(const std::string& key, const std::string_view& data);
  static bool sendDataRaw(const std::string& key, const char* data, int len);
  static bool sendFile(const std::string& key, const std::string& filepath);

  template <typename T>
  static typename std::enable_if<std::is_base_of<google::protobuf::Message, T>::value, bool>::type sendMessage(const std::string& key,
                                                                                                               const T& protobufMessage) {
    return instance().sendMessageInternal(key, protobufMessage);
  }

  static void registerCallback(const std::string& key, MessageCallback callback);
  static void registerFileCallback(const std::string& key, FileCallback callback);
  static void registerStatusCallback(StatusCallback callback);

  template <typename T>
  static void registerCallback(const std::string& key, std::function<void(const T&)> callback) {
    registerCallback(key, [callback, key](const std::string& rawData) {
      T typedMsg;
      if (tryUnpack(rawData, typedMsg)) {
        callback(typedMsg);
      } else {
        std::cerr << "[Warning] Failed to unpack message for key: " << key << std::endl;
      }
    });
  }

  template <typename T>
  static bool tryUnpack(const std::string& raw, T& outMsg) {
    google::protobuf::Any any;
    if (any.ParseFromString(raw)) {
      if (any.Is<T>()) {
        return any.UnpackTo(&outMsg);
      }
      if (any.type_url().find('/') != std::string::npos) {
        // It is a mismatched Any.
        // We could log here: "Type Mismatch in Any Wrapper"
      }
    }

    outMsg.Clear();
    if (outMsg.ParseFromString(raw)) {
      return true;
    }
    return false;
  }

private:
  GrpcConnectionManager(const ConnectionConfig& config);
  ~GrpcConnectionManager();

  void registerInternal(const std::string& key, MessageCallback callback);
  void registerFileInternal(const std::string& key, FileCallback callback);

  bool sendDataInternal(const std::string& key, const std::string_view& data);
  bool sendFileInternal(const std::string& key, const std::string& filePath);
  bool sendRawEnvelope(const broker::BrokerPayload& envelope);

  template <typename T>
  bool sendMessageInternal(const std::string& key, const T& protobufMessage) {
    broker::BrokerPayload envelope;
    envelope.set_handler_key(key);
    envelope.set_sender_id(m_clientId);
    envelope.set_topic(key);

    envelope.mutable_payload()->PackFrom(protobufMessage);

    return sendRawEnvelope(envelope);
  }

  void processingLoop();
  void handleMessage(const broker::BrokerPayload& msg);
  void handleFilePacket(const broker::BrokerPayload& msg);

private:
  static GrpcConnectionManager* m_instance;
  static std::mutex m_initMutex;

  std::string m_clientId;
  GrpcWorker* m_worker;
  SafeQueue<broker::BrokerPayload> m_queue;

  std::thread m_processingThread;
  std::atomic<bool> m_running;

  std::mutex m_mapMutex;
  std::map<std::string, std::vector<MessageCallback>> m_msgHandlers;
  std::map<std::string, std::vector<FileCallback>> m_fileHandlers;
  std::vector<StatusCallback> m_statusHandlers;

  std::map<std::string, std::shared_ptr<FileTransferState>> m_transfers;

  static std::vector<std::pair<std::string, MessageCallback>> s_pendingMsgCallbacks;
  static std::vector<std::pair<std::string, FileCallback>> s_pendingFileCallbacks;
  static std::vector<StatusCallback> s_pendingStatusCallbacks;
};

#endif  // GRPCCONNECTIONMANAGER_H
