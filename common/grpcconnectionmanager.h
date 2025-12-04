#pragma once
#include <QByteArray>
#include <QCoreApplication>
#include <QDebug>
#include <QMap>
#include <QMutex>
#include <QObject>
#include <functional>
#include <random>

#include "grpcworker.h"
#include "protobuf_forward.h"

using MessageCallback = std::function<void(const QByteArray&)>;

class GrpcConnectionManager : public QObject {
  Q_OBJECT
public:
  static void init(const QString& address = "127.0.0.1:50051");

  template <typename T>
  static void sendMessage(const QString& key, const T& protobufMessage) {
    instance().sendInternal(key, protobufMessage);
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

  template <typename T>
  static bool tryUnpack(const QByteArray& raw, T& outMsg) {
    google::protobuf::Any any;
    if (!any.ParseFromArray(raw.data(), raw.size()))
      return false;

    // Optional safety check
    if (!any.Is<T>()) {
      qDebug() << "Type Mismatch. Expected:" << typeid(T).name();
      return false;
    }
    return any.UnpackTo(&outMsg);
  }

private:
  static GrpcConnectionManager& instance();

  void registerInternal(const QString& key, MessageCallback callback);

  template <typename T>
  void sendInternal(const QString& key, const T& protobufMessage) {
    broker::BrokerPayload envelope;
    envelope.set_handler_key(key.toStdString());
    envelope.set_sender_id(m_appName);
    envelope.mutable_payload()->PackFrom(protobufMessage);
    envelope.set_topic(key.toStdString());  // Ensure proto has 'string topic = 4;'

    sendRawEnvelope(envelope);
  }

  void sendRawEnvelope(const broker::BrokerPayload& envelope);

  explicit GrpcConnectionManager(const QString& address);
  ~GrpcConnectionManager();

  GrpcConnectionManager(const GrpcConnectionManager&) = delete;
  GrpcConnectionManager& operator=(const GrpcConnectionManager&) = delete;

private slots:
  void onPayloadReceived(const QString& key, const QByteArray& data);
  void onWorkerConnected();
  void onWorkerDisconnected();

private:
  GrpcWorker* m_pWorker;
  QMap<QString, MessageCallback> m_handlers;
  QMutex m_mapMutex;

  bool m_isConnected;

  std::string m_appName;

  static GrpcConnectionManager* m_pInstance;
};
