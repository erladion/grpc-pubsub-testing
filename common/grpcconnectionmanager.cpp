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

  connect(&m_cleanupTimer, &QTimer::timeout, this, &GrpcConnectionManager::onCleanupTimer);
  m_cleanupTimer.start(10000);
}

void GrpcConnectionManager::onEnvelopeReceived(const broker::BrokerPayload& msg) {
  QString handlerKey = QString::fromStdString(msg.handler_key());
  QString transferId = QString::fromStdString(msg.transfer_id());

  if (handlerKey == "__FILE_META__") {
    QMutexLocker lock(&m_mapMutex);
    QJsonDocument doc = QJsonDocument::fromJson(QByteArray::fromStdString(msg.raw_data()));
    QJsonObject obj = doc.object();

    IncomingTransfer newItem;
    newItem.intendedFilename = obj["filename"].toString();
    newItem.originalTopic = QString::fromStdString(msg.topic());
    newItem.totalChunks = -1;
    newItem.receivedChunks = 0;
    newItem.lastUpdateTimestamp = QDateTime::currentMSecsSinceEpoch();

    QString tempDir = QStandardPaths::writableLocation(QStandardPaths::TempLocation);
    QString safeId = transferId;
    safeId.replace("{", "").replace("}", "").replace("-", "");
    newItem.tempFilePath = tempDir + "/grpc_" + safeId + ".dat";
    newItem.tempFile = new QFile(newItem.tempFilePath);
    if (!newItem.tempFile->open(QIODevice::ReadWrite)) {
      qCritical() << "Failed to create temp file:" << newItem.tempFilePath;
      delete newItem.tempFile;
      return;
    }

    newItem.hasher = new QCryptographicHash(QCryptographicHash::Sha256);
    m_incomingTransfers.insert(transferId, newItem);
    qDebug() << "Incoming File Transfer Started:" << newItem.intendedFilename;
    return;
  }

  if (handlerKey == "__CHUNK__") {
    QMutexLocker lock(&m_mapMutex);

    if (!m_incomingTransfers.contains(transferId)) {
      return;
    }

    IncomingTransfer& transfer = m_incomingTransfers[transferId];
    transfer.lastUpdateTimestamp = QDateTime::currentMSecsSinceEpoch();
    transfer.totalChunks = msg.sequence_count();

    QByteArray chunkData = QByteArray::fromStdString(msg.raw_data());
    qint64 offset = static_cast<qint64>(msg.sequence_number()) * STREAM_CHUNK_SIZE;

    if (transfer.tempFile && transfer.tempFile->isOpen()) {
      transfer.tempFile->seek(offset);
      qint64 bytesWritten = transfer.tempFile->write(chunkData);

      if (bytesWritten != chunkData.size()) {
        qCritical() << "Disk full! Write failed for:" << transfer.intendedFilename;

        transfer.tempFile->close();
        QFile::remove(transfer.tempFilePath);
        m_incomingTransfers.remove(transferId);
        return;
      }

      if (transfer.hasher) {
        transfer.hasher->addData(chunkData);
      }

      transfer.receivedChunks++;
    }
    return;
  }

  if (handlerKey == "__FILE_FOOTER__") {
    QMutexLocker lock(&m_mapMutex);
    if (!m_incomingTransfers.contains(transferId)) {
      return;
    }

    IncomingTransfer& transfer = m_incomingTransfers[transferId];

    QByteArray senderHash = QByteArray::fromStdString(msg.raw_data());
    QByteArray localHash = transfer.hasher->result();

    delete transfer.hasher;
    transfer.hasher = nullptr;
    transfer.tempFile->close();
    delete transfer.tempFile;
    transfer.tempFile = nullptr;

    if (localHash != senderHash) {
      qCritical() << "File corruption! Checksum mismatch.";
      QFile::remove(transfer.tempFilePath);
    } else {
      qDebug() << "File Download Verified:" << transfer.intendedFilename;

      QString downDir = QStandardPaths::writableLocation(QStandardPaths::DownloadLocation);
      QString finalPath = downDir + "/" + transfer.intendedFilename;

      int counter = 1;
      while (QFile::exists(finalPath)) {
        QString base = QFileInfo(transfer.intendedFilename).baseName();
        QString ext = QFileInfo(transfer.intendedFilename).completeSuffix();

        finalPath = downDir + "/" + base + "_" + QString::number(counter++) + "." + ext;
      }

      if (QFile::rename(transfer.tempFilePath, finalPath)) {
        QString topic = transfer.originalTopic;
        m_incomingTransfers.remove(transferId);
        lock.unlock();

        processFilePayload(topic, finalPath);
      } else {
        qCritical() << "Failed to rename/move temp file to " << finalPath;
      }
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
      m_handlers[key](f.readAll());  // Load entire file into RAM
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
  if (!m_isConnected) {
    qWarning() << "Cannot stream file: No connection to broker.";
    return;
  }

  QFile file(filePath);
  if (!file.open(QIODevice::ReadOnly)) {
    qWarning() << "Failed to open file for streaming:" << filePath;
    return;
  }

  QFileInfo fileInfo(filePath);
  QString uuid = QUuid::createUuid().toString();
  std::string transferId = uuid.toStdString();
  std::string topic = key.toStdString();
  qint64 totalSize = file.size();

  QJsonObject meta;
  meta["filename"] = fileInfo.fileName();
  meta["size"] = totalSize;
  meta["transfer_id"] = uuid;

  broker::BrokerPayload metaMsg;
  metaMsg.set_handler_key("__FILE_META__");
  metaMsg.set_topic(topic);
  metaMsg.set_transfer_id(transferId);
  metaMsg.set_raw_data(QJsonDocument(meta).toJson(QJsonDocument::Compact).toStdString());

  if (!sendRawEnvelope(metaMsg)) {
    return;
  }

  int totalChunks = (totalSize + STREAM_CHUNK_SIZE - 1) / STREAM_CHUNK_SIZE;
  int sequence = 0;

  qDebug() << "Starting upload:" << filePath;

  QCryptographicHash hasher(QCryptographicHash::Sha256);

  while (!file.atEnd()) {
    QByteArray chunkData = file.read(STREAM_CHUNK_SIZE);
    if (chunkData.isEmpty() && !file.atEnd()) {
      qCritical() << "File read error! Aborting transfer";
      break;
    }

    hasher.addData(chunkData);

    broker::BrokerPayload msg;
    msg.set_handler_key("__CHUNK__");
    msg.set_topic(topic);
    msg.set_transfer_id(transferId);
    msg.set_sequence_number(sequence++);
    msg.set_sequence_count(totalChunks);
    msg.set_raw_data(chunkData.constData(), chunkData.size());

    int retries = 0;
    bool success = false;
    while (retries < 5) {
      if (sendRawEnvelope(msg)) {
        success = true;
        break;
      }
      QThread::msleep(500);
      retries++;
    }

    if (!success) {
      qCritical() << "Upload failed. Connection lost.";
      break;
    }

    if (sequence & 16 == 0) {
      QThread::msleep(1);
    }
  }
  file.close();

  QByteArray finalHash = hasher.result();
  broker::BrokerPayload footerMsg;

  footerMsg.set_handler_key("__FILE_FOOTER__");
  footerMsg.set_topic(key.toStdString());
  footerMsg.set_transfer_id(transferId);
  footerMsg.set_raw_data(finalHash.toStdString());
  sendRawEnvelope(footerMsg);
}

bool GrpcConnectionManager::sendRawEnvelope(const broker::BrokerPayload& envelope) {
  if (!m_pWorker) {
    return false;
  }
  return m_pWorker->writeMessage(envelope);
}

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

void GrpcConnectionManager::onCleanupTimer() {
  QMutexLocker lock(&m_mapMutex);

  qint64 now = QDateTime::currentMSecsSinceEpoch();
  static const qint64 timeoutLimit = 30000;  // 30 secs

  auto it = m_incomingTransfers.begin();
  while (it != m_incomingTransfers.end()) {
    if (now - it.value().lastUpdateTimestamp > timeoutLimit) {
      qWarning() << "Transfer timed out. Cleaning up:" << it.key();

      if (it.value().tempFile) {
        it.value().tempFile->close();
        delete it.value().tempFile;
      }
      QFile::remove(it.value().tempFilePath);
      it = m_incomingTransfers.erase(it);
    } else {
      ++it;
    }
  }
}
