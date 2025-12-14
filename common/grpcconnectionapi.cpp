#include "grpcconnectionapi.h"
#include "grpcconnectionmanager.h"

#include <QByteArray>
#include <QCoreApplication>
#include <QString>
#include <QTimer>

#include <google/protobuf/any.pb.h>

// Internal Qt Application Instance
static QCoreApplication* g_app = nullptr;
static int g_argc = 1;
static char* g_argv[] = {(char*)"GrpcCWrapper", nullptr};

// Ensure Qt is running
static void ensure_qt() {
  if (!QCoreApplication::instance()) {
    g_app = new QCoreApplication(g_argc, g_argv);
  }
}

void initConnection(const char* address, const char* clientId) {
  if (clientId && *clientId != '\0') {
    // We need a static string storage to ensure the char* remains valid
    // for the lifetime of QCoreApplication
    static std::string clientName = clientId;
    g_argv[0] = const_cast<char*>(clientName.data());
  }

  ensure_qt();
  GrpcConnectionManager::init(QString::fromUtf8(address));
}

void processEvents() {
  if (QCoreApplication::instance()) {
    QCoreApplication::processEvents();
  }
}

void sendData(const char* topic, const char* data, int len) {
  QByteArray bytes(data, len);
  GrpcConnectionManager::sendData(QString::fromUtf8(topic), bytes);
}

void sendText(const char* topic, const char* text) {
  QByteArray bytes(text);
  GrpcConnectionManager::sendData(QString::fromUtf8(topic), bytes);
}

void sendFile(const char* topic, const char* filepath) {
  GrpcConnectionManager::sendFile(QString::fromUtf8(topic), QString::fromUtf8(filepath));
}

void registerCallback(const char* topic, GrpcMessageCallback callBack, void* userData) {
  QString qTopic = QString::fromUtf8(topic);

  GrpcConnectionManager::registerCallback(qTopic, [callBack, userData, qTopic](const QByteArray& data) {
    if (!callBack)
      return;

    google::protobuf::Any anyMsg;

    const bool parsed = anyMsg.ParseFromArray(data.constData(), data.size());
    if (parsed && !anyMsg.type_url().empty() && anyMsg.type_url().find('/') != std::string::npos) {
      std::string inner_payload = anyMsg.value();

      callBack(qTopic.toUtf8().constData(), inner_payload.data(), static_cast<int>(inner_payload.size()), userData);

    } else {
      callBack(qTopic.toUtf8().constData(), data.constData(), data.size(), userData);
    }
  });
}

void registerFileCallback(const char* topic, GrpcFileCallback callback, void* userData) {
  QString qTopic = QString::fromUtf8(topic);

  GrpcConnectionManager::registerFileCallback(qTopic, [callback, userData, qTopic](const QString& path) {
    if (callback) {
      callback(qTopic.toUtf8().constData(), path.toUtf8().constData(), userData);
    }
  });
}
