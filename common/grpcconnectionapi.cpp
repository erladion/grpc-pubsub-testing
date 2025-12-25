#include "grpcconnectionapi.h"

#include <cstring>

#include "grpcconnectionmanager.h"

int initConnection(const GrpcConfig* config) {
  if (!config || !config->address) {
    return GRPC_ERROR_INVALID_ARGS;
  }

  ConnectionConfig cfg;
  cfg.address = config->address;
  cfg.clientId = config->client_id ? config->client_id : "UnknownClient";
  cfg.keepAliveTime = config->keepalive_time_ms;
  cfg.keepAliveTimeout = config->keepalive_timeout_ms;
  cfg.compressionAlgo = config->compression_algorithm;

  GrpcConnectionManager::init(cfg);

  return GRPC_SUCCESS;
}

void shutdownConnection() {
  GrpcConnectionManager::shutdown();
}

int sendText(const char* topic, const char* text) {
  if (!topic || !text) {
    return GRPC_ERROR_INVALID_ARGS;
  }
  bool res = GrpcConnectionManager::sendMessage(topic, text);
  return res ? GRPC_SUCCESS : GRPC_ERROR_NO_CONNECTION;
}

int sendData(const char* topic, const char* data, int len) {
  if (!topic || !data) {
    return GRPC_ERROR_INVALID_ARGS;
  }
  bool res = GrpcConnectionManager::sendDataRaw(topic, data, len);
  return res ? GRPC_SUCCESS : GRPC_ERROR_NO_CONNECTION;
}

int sendFile(const char* topic, const char* filepath) {
  if (!topic || !filepath) {
    return GRPC_ERROR_INVALID_ARGS;
  }
  bool res = GrpcConnectionManager::sendFile(topic, filepath);
  return res ? GRPC_SUCCESS : GRPC_ERROR_NO_CONNECTION;
}

void registerCallback(const char* topic, GrpcMessageCallback callback, void* userData) {
  if (!topic || !callback) {
    return;
  }

  GrpcConnectionManager::registerCallback(topic, [callback, userData, t = std::string(topic)](const std::string& data) {
    callback(t.c_str(), data.c_str(), (int)data.size(), userData);
  });
}

void registerFileCallback(const char* topic, GrpcFileCallback callback, void* userData) {
  if (!topic || !callback) {
    return;
  }

  GrpcConnectionManager::registerFileCallback(
      topic, [callback, userData, t = std::string(topic)](const std::string& path) { callback(t.c_str(), path.c_str(), userData); });
}

void registerStatusCallback(GrpcStatusCallback callback, void* userData) {
  if (!callback) {
    return;
  }
  GrpcConnectionManager::registerStatusCallback(
      [callback, userData](bool connected) { callback(connected ? GRPC_STATUS_CONNECTED : GRPC_STATUS_DISCONNECTED, userData); });
}
