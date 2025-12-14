#include "grpcconnectionapi.h"
#include "grpcconnectionmanager.h"

#include <QByteArray>
#include <QCoreApplication>
#include <QString>

static QCoreApplication* g_app = nullptr;
static int g_argc = 1;
static char* g_argv[] = {(char*)"GrpcCWrapper", nullptr};

static void ensure_qt() {
  if (!QCoreApplication::instance()) {
    g_app = new QCoreApplication(g_argc, g_argv);
  }
}

int initConnection(const GrpcConfig* config) {
  if (!config || !config->address)
    return GRPC_ERROR_INVALID_ARGS;
  ensure_qt();

  if (config->client_id && config->client_id[0] != '\0') {
    QCoreApplication::setApplicationName(QString::fromUtf8(config->client_id));
  } else {
    QCoreApplication::setApplicationName("UnknownCClient");
  }

  GrpcConnectionManager::init(QString::fromUtf8(config->address));
  return GRPC_SUCCESS;
}

void shutdownConnection() {
  GrpcConnectionManager::shutdown();
}

void processEvents() {
  if (QCoreApplication::instance()) {
    QCoreApplication::processEvents();
  }
}

void registerStatusCallback(GrpcStatusCallback cb, void* user_data) {
  GrpcConnectionManager::registerStatusCallback([cb, user_data](bool connected) {
    if (cb) {
      cb(connected ? GRPC_STATUS_CONNECTED : GRPC_STATUS_DISCONNECTED, user_data);
    }
  });
}

int sendData(const char* topic, const char* data, int len) {
  if (!topic || !data)
    return GRPC_ERROR_INVALID_ARGS;
  QByteArray bytes(data, len);
  bool res = GrpcConnectionManager::sendData(QString::fromUtf8(topic), bytes);
  return res ? GRPC_SUCCESS : GRPC_ERROR_NO_CONNECTION;
}

int sendText(const char* topic, const char* text) {
  if (!topic || !text)
    return GRPC_ERROR_INVALID_ARGS;
  QByteArray bytes(text);
  bool res = GrpcConnectionManager::sendData(QString::fromUtf8(topic), bytes);
  return res ? GRPC_SUCCESS : GRPC_ERROR_NO_CONNECTION;
}

int sendFile(const char* topic, const char* filepath) {
  if (!topic || !filepath)
    return GRPC_ERROR_INVALID_ARGS;
  bool res = GrpcConnectionManager::sendFile(QString::fromUtf8(topic), QString::fromUtf8(filepath));
  return res ? GRPC_SUCCESS : GRPC_ERROR_NO_CONNECTION;
}

void registerCallback(const char* topic, GrpcMessageCallback cb, void* user_data) {
  QString qTopic = QString::fromUtf8(topic);
  GrpcConnectionManager::registerCallback(qTopic, [cb, user_data, qTopic](const QByteArray& data) {
    if (cb) {
      cb(qTopic.toUtf8().constData(), data.constData(), data.size(), user_data);
    }
  });
}

void registerFileCallback(const char* topic, GrpcFileCallback cb, void* user_data) {
  QString qTopic = QString::fromUtf8(topic);
  GrpcConnectionManager::registerFileCallback(qTopic, [cb, user_data, qTopic](const QString& path) {
    if (cb) {
      cb(qTopic.toUtf8().constData(), path.toUtf8().constData(), user_data);
    }
  });
}
