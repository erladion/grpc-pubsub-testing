#pragma once
#include <QByteArray>
#include <QCoreApplication>
#include <QDebug>
#include <QFile>
#include <QMap>
#include <QMutex>
#include <QObject>
#include <QTimer>

#include <functional>

#include "grpcworker.h"
#include "protobuf_forward.h"

using MessageCallback = std::function<void(const QByteArray&)>;

using FileCallback = std::function<void(const QString&)>;

struct IncomingTransfer {
  QString originalTopic;
  int totalChunks;
  int receivedChunks;

  QFile* tempFile = nullptr;
  QString tempFilePath;

  qint64 lastUpdateTimestamp;
};

class GrpcConnectionManager : public QObject {
  Q_OBJECT
public:
  static void init(const QString& address = "127.0.0.1:50051");

  template <typename T>
  static void sendMessage(const QString& key, const T& protobufMessage) {
    instance().sendMessageInternal(key, protobufMessage);
  }

  template <typename T>
  static void registerCallback(const QString& key, std::function<void(const T&)> callback) {
    registerCallback(key, [callback, key](const QByteArray& rawData) {
      T typedMsg;
      if (tryUnpack(rawData, typedMsg)) {
        callback(typedMsg);
      } else {
        qWarning() << "Failed to unpack message for key:" << key;
      }
    });
  }

  static void registerCallback(const QString& key, MessageCallback callback);

  static void registerFileCallback(const QString& key, FileCallback callback);

  static void sendData(const QString& key, const QByteArray& data);

  static void sendFile(const QString& key, const QString& filePath);

  template <typename T>
  static bool tryUnpack(const QByteArray& raw, T& outMsg) {
    google::protobuf::Any any;
    if (!any.ParseFromArray(raw.data(), raw.size())) {
      return false;
    }

    if (!any.Is<T>()) {
      qDebug() << "Type Mismatch. Expected:" << typeid(T).name();
      return false;
    }
    return any.UnpackTo(&outMsg);
  }

private:
  static GrpcConnectionManager& instance();

  void registerInternal(const QString& key, MessageCallback callback);
  void registerFileInternal(const QString& key, FileCallback callback);

  template <typename T>
  void sendMessageInternal(const QString& key, const T& protobufMessage) {
    broker::BrokerPayload envelope;
    envelope.set_handler_key(key.toStdString());
    envelope.set_sender_id(m_appName);
    envelope.mutable_payload()->PackFrom(protobufMessage);
    envelope.set_topic(key.toStdString());
    sendRawEnvelope(envelope);
  }

  void sendDataInternal(const QString& key, const QByteArray& data);
  void sendFileInternal(const QString& key, const QString& filePath);

  bool sendRawEnvelope(const broker::BrokerPayload& envelope);

  explicit GrpcConnectionManager(const QString& address);
  ~GrpcConnectionManager();

  GrpcConnectionManager(const GrpcConnectionManager&) = delete;
  GrpcConnectionManager& operator=(const GrpcConnectionManager&) = delete;

private slots:
  void onEnvelopeReceived(const broker::BrokerPayload& msg);

  void processPayload(const QString& key, const QByteArray& data);
  void processFilePayload(const QString& key, const QString& filePath);

  // void onPayloadReceived(const QString& key, const QByteArray& data);
  void onWorkerConnected();
  void onWorkerDisconnected();

  void onCleanupTimer();

private:
  GrpcWorker* m_pWorker;
  QHash<QString, MessageCallback> m_handlers;
  QHash<QString, FileCallback> m_fileHandlers;

  QHash<QString, IncomingTransfer> m_incomingTransfers;

  QMutex m_mapMutex;
  bool m_isConnected;
  std::string m_appName;
  static GrpcConnectionManager* m_pInstance;

  QTimer m_cleanupTimer;
};
