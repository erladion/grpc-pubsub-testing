#include "grpcconnectionapi.h"
#include "grpcconnectionmanager.h"

#include <QByteArray>
#include <QCoreApplication>
#include <QString>
#include <QTimer>

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

void grpc_api_init(const char* address) {
  ensure_qt();
  GrpcConnectionManager::init(QString::fromUtf8(address));
}

void grpc_process_events() {
  if (QCoreApplication::instance()) {
    QCoreApplication::processEvents();
  }
}

void grpc_send_data(const char* topic, const char* data, int len) {
  QByteArray bytes(data, len);
  GrpcConnectionManager::sendData(QString::fromUtf8(topic), bytes);
}

void grpc_send_text(const char* topic, const char* text) {
  QByteArray bytes(text);
  GrpcConnectionManager::sendData(QString::fromUtf8(topic), bytes);
}

void grpc_send_file(const char* topic, const char* filepath) {
  GrpcConnectionManager::sendFile(QString::fromUtf8(topic), QString::fromUtf8(filepath));
}

void grpc_register_callback(const char* topic, GrpcMessageCallback cb, void* user_data) {
  QString qTopic = QString::fromUtf8(topic);

  // We capture the C function pointer 'cb' and 'user_data' in the C++ lambda
  GrpcConnectionManager::registerCallback(qTopic, [cb, user_data, qTopic](const QByteArray& data) {
    if (cb) {
      cb(qTopic.toUtf8().constData(), data.constData(), data.size(), user_data);
    }
  });
}

void grpc_register_file_callback(const char* topic, GrpcFileCallback cb, void* user_data) {
  QString qTopic = QString::fromUtf8(topic);

  GrpcConnectionManager::registerFileCallback(qTopic, [cb, user_data, qTopic](const QString& path) {
    if (cb) {
      cb(qTopic.toUtf8().constData(), path.toUtf8().constData(), user_data);
    }
  });
}
