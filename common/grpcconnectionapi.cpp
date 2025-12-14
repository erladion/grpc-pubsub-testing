#include "grpcconnectionapi.h"
#include "grpcconnectionmanager.h"

#include <QByteArray>
#include <QCoreApplication>
#include <QString>
#include <QTimer>
#include <atomic>
#include <condition_variable>
#include <mutex>
#include <thread>

static QCoreApplication* g_app = nullptr;
static std::thread* g_qtThread = nullptr;
static std::mutex g_initMutex;
static std::condition_variable g_initCv;
static bool g_isInitialized = false;

// The loop that runs inside the background thread
static void qt_thread_entry(int argc, char* argv[], const QString& address, const QString& clientId) {
  if (!QCoreApplication::instance()) {
    g_app = new QCoreApplication(argc, argv);
  } else {
    g_app = QCoreApplication::instance();
  }

  if (!clientId.isEmpty()) {
    QCoreApplication::setApplicationName(clientId);
  } else {
    QCoreApplication::setApplicationName("UnknownCClient");
  }

  // Initialize the Manager inside the Qt Thread so it has correct thread affinity
  GrpcConnectionManager::init(address);

  // Notify initConnection that we are ready
  {
    std::lock_guard<std::mutex> lock(g_initMutex);
    g_isInitialized = true;
  }
  g_initCv.notify_one();

  // Run Event Loop (Blocking)
  g_app->exec();

  // Cleanup after exec() returns (via quit)
  GrpcConnectionManager::shutdown();

  // Only delete if we created it
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

  // Spawn Background Thread
  g_qtThread = new std::thread(qt_thread_entry, argc, argv, addr, id);

  // Wait for initialization to complete
  std::unique_lock<std::mutex> lock(g_initMutex);
  g_initCv.wait(lock, [] { return g_isInitialized; });

  return GRPC_SUCCESS;
}

void shutdownConnection() {
  if (g_app) {
    // We use invokeMethod because g_app lives in another thread
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
  // Status callbacks will be fired from the Qt thread.
  // The C client must handle thread safety.
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
  QByteArray bytes(data, len);
  // sendData is thread-safe via internal mutexes in Manager
  bool res = GrpcConnectionManager::sendData(QString::fromUtf8(topic), bytes);
  return res ? GRPC_SUCCESS : GRPC_ERROR_NO_CONNECTION;
}

int sendText(const char* topic, const char* text) {
  if (!topic || !text) {
    return GRPC_ERROR_INVALID_ARGS;
  }
  QByteArray bytes(text);
  bool res = GrpcConnectionManager::sendData(QString::fromUtf8(topic), bytes);
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
