#include "grpcconnectionmanager.h"

#include <QCoreApplication>
#include <QDateTime>
#include <QDebug>
#include <QDir>
#include <QFile>
#include <QFileInfo>
#include <QFuture>
#include <QStandardPaths>
#include <QUuid>
#include <QtConcurrent/QtConcurrent>

GrpcConnectionManager* GrpcConnectionManager::m_pInstance = nullptr;

static const int STREAM_CHUNK_SIZE = 64 * 1024;

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

void GrpcConnectionManager::registerFileCallback(const QString& key, FileCallback callback) {
  instance().registerFileInternal(key, callback);
}

void GrpcConnectionManager::sendData(const QString& key, const QByteArray& data) {
  instance().sendData(key, data);
}

void GrpcConnectionManager::sendFile(const QString& key, const QString& filePath) {
  QFuture future = QtConcurrent::run([key, filePath]() { instance().sendFileInternal(key, filePath); });
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

  connect(m_pWorker, &GrpcWorker::envelopeReceived, this, &GrpcConnectionManager::onEnvelopeReceived, Qt::QueuedConnection);

  connect(m_pWorker, &GrpcWorker::connected, this, &GrpcConnectionManager::onWorkerConnected, Qt::QueuedConnection);
  connect(m_pWorker, &GrpcWorker::disconnected, this, &GrpcConnectionManager::onWorkerDisconnected, Qt::QueuedConnection);
  m_pWorker->start();
}

void GrpcConnectionManager::onEnvelopeReceived(const broker::BrokerPayload& msg) {
  QString handlerKey = QString::fromStdString(msg.handler_key());

  if (handlerKey == "__CHUNK__") {
    QMutexLocker lock(&m_mapMutex);

    QString transferId = QString::fromStdString(msg.transfer_id());
    int seqNum = msg.sequence_number();
    int seqCount = msg.sequence_count();

    QString originalTopic = QString::fromStdString(msg.topic());

    if (!m_incomingTransfers.contains(transferId)) {
      IncomingTransfer newItem;
      newItem.totalChunks = seqCount;
      newItem.receivedChunks = 0;
      newItem.originalTopic = originalTopic;
      newItem.lastUpdateTimestamp = QDateTime::currentMSecsSinceEpoch();

      QString tempDir = QStandardPaths::writableLocation(QStandardPaths::TempLocation);
      QDir dir(tempDir);

      if (!dir.exists()) {
        dir.mkpath(".");
      }

      QString safeId = transferId;
      safeId.replace("{", "").replace("}", "").replace("-", "");
      newItem.tempFilePath = dir.filePath("grpc_" + safeId + ".dat");
      newItem.tempFile = new QFile(newItem.tempFilePath);
      if (!newItem.tempFile->open(QIODevice::ReadWrite)) {
        qCritical() << "Failed to create temp file:" << newItem.tempFilePath;
        delete newItem.tempFile;
        return;
      }

      m_incomingTransfers.insert(transferId, newItem);
    }

    IncomingTransfer& transfer = m_incomingTransfers[transferId];

    if (transfer.tempFile && transfer.tempFile->isOpen()) {
      QByteArray chunkData = QByteArray::fromStdString(msg.raw_data());
      qint64 offset = static_cast<qint64>(seqNum) * STREAM_CHUNK_SIZE;

      if (!transfer.tempFile->seek(offset)) {
        qWarning() << "Seek failed for chunk" << seqNum;
      }

      transfer.tempFile->write(chunkData);
      transfer.receivedChunks++;
    }

    if (transfer.receivedChunks >= transfer.totalChunks) {
      qDebug() << "Download complete:" << transfer.tempFilePath;

      transfer.tempFile->close();
      delete transfer.tempFile;
      transfer.tempFile = nullptr;

      QString finalPath = transfer.tempFilePath;
      QString topic = transfer.originalTopic;

      m_incomingTransfers.remove(transferId);
      lock.unlock();

      processFilePayload(topic, finalPath);
    }
    return;
  }

  QByteArray finalData;
  if (msg.has_payload()) {
    std::string serialized;
    msg.payload().SerializeToString(&serialized);
    finalData = QByteArray::fromStdString(serialized);
  } else {
    finalData = QByteArray::fromStdString(msg.raw_data());
  }
  processPayload(handlerKey, finalData);
}

void GrpcConnectionManager::processPayload(const QString& key, const QByteArray& data) {
  QMutexLocker lock(&m_mapMutex);
  if (m_handlers.contains(key)) {
    m_handlers[key](data);
  }
}

void GrpcConnectionManager::processFilePayload(const QString& key, const QString& filePath) {
  QMutexLocker lock(&m_mapMutex);

  if (m_fileHandlers.contains(key)) {
    m_fileHandlers[key](filePath);
    // Note: We do NOT delete the file here. The user owns it now.
  } else if (m_handlers.contains(key)) {
    QFile f(filePath);
    if (f.open(QIODevice::ReadOnly)) {
      QByteArray data = f.readAll();  // Load entire file into RAM
      m_handlers[key](data);
      f.close();
    }
    QFile::remove(filePath);
  } else {
    qWarning() << "No handler registered for file topic:" << key << "Deleting temp file.";
    QFile::remove(filePath);
  }
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

void GrpcConnectionManager::registerFileInternal(const QString& key, FileCallback callback) {
  QMutexLocker lock(&m_mapMutex);
  m_fileHandlers.insert(key, callback);
  if (m_isConnected) {
    broker::BrokerPayload subMsg;
    subMsg.set_handler_key("__SUBSCRIBE__");
    subMsg.set_sender_id(m_appName);
    subMsg.set_topic(key.toStdString());
    sendRawEnvelope(subMsg);
  }
}

void GrpcConnectionManager::sendDataInternal(const QString& key, const QByteArray& data) {
  int totalSize = data.size();

  if (totalSize <= STREAM_CHUNK_SIZE) {
    broker::BrokerPayload msg;
    msg.set_handler_key(key.toStdString());
    msg.set_sender_id(m_appName);
    msg.set_topic(key.toStdString());
    msg.set_raw_data(data.toStdString());
    sendRawEnvelope(msg);
    return;
  }

  const int totalChunks = (totalSize + STREAM_CHUNK_SIZE - 1) / STREAM_CHUNK_SIZE;
  const std::string transferId = QUuid::createUuid().toString().toStdString();

  qDebug() << "Sending large data:" << key << "| Size:" << totalSize << "| Chunks:" << totalChunks;

  for (int i(0); i < totalChunks; ++i) {
    broker::BrokerPayload msg;
    msg.set_handler_key("__CHUNK__");
    msg.set_topic(key.toStdString());
    msg.set_transfer_id(transferId);
    msg.set_sequence_number(i);
    msg.set_sequence_count(totalChunks);

    const int start = i * STREAM_CHUNK_SIZE;
    const int len = std::min(STREAM_CHUNK_SIZE, totalSize - start);

    msg.set_raw_data(data.mid(start, len).toStdString());
    sendRawEnvelope(msg);
  }
}

void GrpcConnectionManager::sendFileInternal(const QString& key, const QString& filePath) {
  QFile file(filePath);

  if (!file.open(QIODevice::ReadOnly)) {
    qWarning() << "Failed to open file for streaming:" << filePath;
    return;
  }

  qint64 totalSize = file.size();

  int totalChunks = (totalSize + STREAM_CHUNK_SIZE - 1) / STREAM_CHUNK_SIZE;
  std::string transferId = QUuid::createUuid().toString().toStdString();

  qDebug() << "Streaming file:" << filePath << "| Chunks:" << totalChunks;

  int sequence = 0;
  while (!file.atEnd()) {
    QByteArray chunkData = file.read(STREAM_CHUNK_SIZE);

    broker::BrokerPayload msg;
    msg.set_handler_key("__CHUNK__");
    msg.set_topic(key.toStdString());
    msg.set_transfer_id(transferId);
    msg.set_sequence_number(sequence++);
    msg.set_sequence_count(totalChunks);
    msg.set_raw_data(chunkData.toStdString());

    sendRawEnvelope(msg);
  }
  file.close();
}

void GrpcConnectionManager::sendRawEnvelope(const broker::BrokerPayload& envelope) {
  if (m_pWorker) {
    m_pWorker->writeMessage(envelope);
  }
}

// void GrpcConnectionManager::onPayloadReceived(const QString& key, const QByteArray& data) {
//   QMutexLocker lock(&m_mapMutex);
//   if (m_handlers.contains(key)) {
//     m_handlers[key](data);
//   }
// }

void GrpcConnectionManager::onWorkerConnected() {
  QMutexLocker lock(&m_mapMutex);
  m_isConnected = true;

  qDebug() << "Broker connection established. Resubscribing to" << m_handlers.size() << "topics.";

  QStringList allTopics = m_handlers.keys() + m_fileHandlers.keys();

  for (const QString& topic : allTopics) {
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
