#include "grpcconnectionmanager.h"
#include <QCoreApplication>
#include <QDebug>
#include <random>

GrpcConnectionManager* GrpcConnectionManager::m_pInstance = nullptr;

void GrpcConnectionManager::init(const QString& address) {
  if (m_pInstance) {
    qWarning() << "GrpcConnectionManager already initialized!";
    return;
  }
  m_pInstance = new GrpcConnectionManager(address);
}

void GrpcConnectionManager::registerCallback(const QString& key, MessageCallback callback) {
  instance().registerInternal(key, callback);
}

GrpcConnectionManager& GrpcConnectionManager::instance() {
  if (!m_pInstance) {
    qFatal("GrpcConnectionManager::init() must be called before using static methods!");
  }
  return *m_pInstance;
}

GrpcConnectionManager::GrpcConnectionManager(const QString& address) : m_pWorker(nullptr), m_isConnected(false) {
  if (!QCoreApplication::instance()) {
    qFatal("QCoreApplication must exist before initializing GrpcConnectionManager");
  }

  m_appName = QCoreApplication::applicationName().toStdString();

  if (m_appName.empty()) {
    m_appName = "UnknownApp";
  }

  qDebug() << "Initialized GrpcManager for Client ID:" << m_appName;

  m_pWorker = new GrpcWorker(address, nullptr);
  connect(m_pWorker, &GrpcWorker::payloadReceived, this, &GrpcConnectionManager::onPayloadReceived, Qt::QueuedConnection);
  connect(m_pWorker, &GrpcWorker::connected, this, &GrpcConnectionManager::onWorkerConnected, Qt::QueuedConnection);
  connect(m_pWorker, &GrpcWorker::disconnected, this, &GrpcConnectionManager::onWorkerDisconnected, Qt::QueuedConnection);
  m_pWorker->start();
}

GrpcConnectionManager::~GrpcConnectionManager() {
  if (m_pWorker) {
    m_pWorker->stop();
    delete m_pWorker;
  }
}

void GrpcConnectionManager::registerInternal(const QString& key, MessageCallback callback) {
  QMutexLocker lock(&m_mapMutex);
  m_handlers.insert(key, callback);

  if (m_isConnected) {
    broker::BrokerPayload subMsg;
    subMsg.set_handler_key("__SUBSCRIBE__");
    subMsg.set_sender_id(m_appName);
    subMsg.set_topic(key.toStdString());

    sendRawEnvelope(subMsg);
    qDebug() << "Runtime Subscription sent for:" << key;
  }
}

void GrpcConnectionManager::sendRawEnvelope(const broker::BrokerPayload& envelope) {
  if (m_pWorker) {
    m_pWorker->writeMessage(envelope);
  }
}

void GrpcConnectionManager::onPayloadReceived(const QString& key, const QByteArray& data) {
  QMutexLocker lock(&m_mapMutex);
  if (m_handlers.contains(key)) {
    m_handlers[key](data);
  }
}

void GrpcConnectionManager::onWorkerConnected() {
  QMutexLocker lock(&m_mapMutex);
  m_isConnected = true;

  qDebug() << "Broker connection established. Resubscribing to" << m_handlers.size() << "topics.";

  for (auto it = m_handlers.begin(); it != m_handlers.end(); ++it) {
    QString topic = it.key();

    broker::BrokerPayload subMsg;
    subMsg.set_handler_key("__SUBSCRIBE__");
    subMsg.set_sender_id(m_appName);
    subMsg.set_topic(topic.toStdString());

    sendRawEnvelope(subMsg);
  }
}

void GrpcConnectionManager::onWorkerDisconnected() {
  QMutexLocker lock(&m_mapMutex);
  m_isConnected = false;
}
