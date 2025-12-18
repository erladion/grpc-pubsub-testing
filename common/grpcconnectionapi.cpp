#include "grpcconnectionapi.h"
#include "grpcconnectionmanager.h"

#include <QByteArray>
#include <QCoreApplication>
#include <QString>
#include <QTimer>
#include <QVariant>
#include <atomic>
#include <condition_variable>
#include <mutex>
#include <thread>

#include <google/protobuf/any.pb.h>

static QCoreApplication* g_app = nullptr;
static std::thread* g_qtThread = nullptr;
static std::mutex g_initMutex;
static std::condition_variable g_initCv;
static bool g_isInitialized = false;

static void qt_thread_entry(int argc, char* argv[], const QString& address, const QString& clientId, int compressionAlgo) {
  if (!QCoreApplication::instance()) {
    g_app = new QCoreApplication(argc, argv);
    g_app->setProperty("owned_by_lib", true);
  } else {
    g_app = QCoreApplication::instance();
  }

  if (!clientId.isEmpty()) {
    QCoreApplication::setApplicationName(clientId);
  } else {
    QCoreApplication::setApplicationName("UnknownCClient");
  }

  GrpcConnectionManager::init(address, compressionAlgo);

  {
    std::lock_guard<std::mutex> lock(g_initMutex);
    g_isInitialized = true;
  }
  g_initCv.notify_one();

  g_app->exec();

  GrpcConnectionManager::shutdown();

  if (g_app && g_app->property("owned_by_lib").toBool()) {
    delete g_app;
    g_app = nullptr;
  }
}

int initConnection(const GrpcConfig* config) {
  if (!config || !config->address) {
    return GRPC_ERROR_INVALID_ARGS;
  }
  if (g_qtThread) {
    return GRPC_SUCCESS;
  }

  static int argc = 1;
  static char* argv[] = {(char*)"GrpcCWrapper", nullptr};

  QString addr = QString::fromUtf8(config->address);
  QString id = config->client_id ? QString::fromUtf8(config->client_id) : QString();

  int comp = (int)config->compression_algorithm;

  g_qtThread = new std::thread(qt_thread_entry, argc, argv, addr, id, comp);

  std::unique_lock<std::mutex> lock(g_initMutex);
  g_initCv.wait(lock, [] { return g_isInitialized; });

  return GRPC_SUCCESS;
}

void shutdownConnection() {
  if (g_app) {
    QMetaObject::invokeMethod(g_app, "quit", Qt::QueuedConnection);
  }
  if (g_qtThread) {
    if (g_qtThread->joinable()) {
      g_qtThread->join();
    }
    delete g_qtThread;
    g_qtThread = nullptr;
  }
  g_isInitialized = false;
}

void registerStatusCallback(GrpcStatusCallback cb, void* user_data) {
  GrpcConnectionManager::registerStatusCallback([cb, user_data](bool connected) {
    if (cb) {
      cb(connected ? GRPC_STATUS_CONNECTED : GRPC_STATUS_DISCONNECTED, user_data);
    }
  });
}

int sendData(const char* topic, const char* data, int len) {
  if (!topic || !data) {
    return GRPC_ERROR_INVALID_ARGS;
  }
  bool res = GrpcConnectionManager::sendDataRaw(QString::fromUtf8(topic), data, len);
  return res ? GRPC_SUCCESS : GRPC_ERROR_NO_CONNECTION;
}

int sendText(const char* topic, const char* text) {
  if (!topic || !text) {
    return GRPC_ERROR_INVALID_ARGS;
  }
  bool res = GrpcConnectionManager::sendDataRaw(QString::fromUtf8(topic), text, strlen(text));
  return res ? GRPC_SUCCESS : GRPC_ERROR_NO_CONNECTION;
}

int sendFile(const char* topic, const char* filepath) {
  if (!topic || !filepath) {
    return GRPC_ERROR_INVALID_ARGS;
  }
  bool res = GrpcConnectionManager::sendFile(QString::fromUtf8(topic), QString::fromUtf8(filepath));
  return res ? GRPC_SUCCESS : GRPC_ERROR_NO_CONNECTION;
}

void registerCallback(const char* topic, GrpcMessageCallback cb, void* user_data) {
  QString qTopic = QString::fromUtf8(topic);
  GrpcConnectionManager::registerCallback(qTopic, [cb, user_data, qTopic](const QByteArray& data) {
    if (cb) {
      google::protobuf::Any anyMsg;
      if (anyMsg.ParseFromArray(data.constData(), data.size()) && !anyMsg.type_url().empty() && anyMsg.type_url().find('/') != std::string::npos) {
        const std::string& innerVal = anyMsg.value();
        cb(qTopic.toUtf8().constData(), innerVal.data(), innerVal.size(), user_data);
      } else {
        cb(qTopic.toUtf8().constData(), data.constData(), data.size(), user_data);
      }
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
