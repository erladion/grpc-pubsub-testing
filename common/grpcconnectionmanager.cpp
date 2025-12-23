#include "grpcconnectionmanager.h"

#include <QCoreApplication>
#include <QDateTime>
#include <QDebug>
#include <QDir>
#include <QFileInfo>
#include <QJsonDocument>
#include <QJsonObject>
#include <QStandardPaths>
#include <QUuid>
#include <QtConcurrent/QtConcurrent>

GrpcConnectionManager* GrpcConnectionManager::m_pInstance = nullptr;
static const int STREAM_CHUNK_SIZE = 64 * 1024;

void GrpcConnectionManager::init(const QString& address, int compressionAlgo, int keepAliveTime, int keepAliveTimeout) {
  if (m_pInstance) {
    return;
  }
  m_pInstance = new GrpcConnectionManager(address, compressionAlgo, keepAliveTime, keepAliveTimeout);
}

void GrpcConnectionManager::shutdown() {
  if (m_pInstance) {
    delete m_pInstance;
    m_pInstance = nullptr;
  }
}

GrpcConnectionManager& GrpcConnectionManager::instance() {
  if (!m_pInstance)
    qFatal("GrpcConnectionManager not initialized!");
  return *m_pInstance;
}

void GrpcConnectionManager::registerStatusCallback(StatusCallback callback) {
  QMutexLocker lock(&instance().m_mapMutex);
  instance().m_statusCallbacks.append(callback);
  bool connected = instance().m_isConnected;
  callback(connected);
}

bool GrpcConnectionManager::sendData(const QString& key, const QByteArray& data) {
  return instance().sendDataInternal(key, data);
}

bool GrpcConnectionManager::sendDataRaw(const QString& key, const char* data, int len) {
  return instance().sendDataRawInternal(key, data, len);
}

bool GrpcConnectionManager::sendFile(const QString& key, const QString& filePath) {
  return instance().sendFileInternal(key, filePath);
}

void GrpcConnectionManager::registerCallback(const QString& key, MessageCallback callback) {
  instance().registerInternal(key, callback);
}
void GrpcConnectionManager::registerFileCallback(const QString& key, FileCallback callback) {
  instance().registerFileInternal(key, callback);
}

GrpcConnectionManager::GrpcConnectionManager(const QString& address, int compressionAlgo, int kaTime, int kaTimeout)
    : m_pWorker(nullptr), m_isConnected(false) {
  if (!QCoreApplication::instance()) {
    qFatal("QCoreApplication required");
  }

  m_appName = QCoreApplication::applicationName().toStdString();
  if (m_appName.empty()) {
    m_appName = "UnknownApp";
  }

  // Build the Config Struct
  WorkerConfig config;
  config.targetAddress = address;
  config.compressionAlgo = compressionAlgo;
  config.keepAliveTime = kaTime;
  config.keepAliveTimeout = kaTimeout;

  m_pWorker = new GrpcWorker(config, nullptr);

  connect(m_pWorker, &GrpcWorker::envelopeReceived, this, &GrpcConnectionManager::onEnvelopeReceived, Qt::QueuedConnection);
  connect(m_pWorker, &GrpcWorker::connected, this, &GrpcConnectionManager::onWorkerConnected, Qt::QueuedConnection);
  connect(m_pWorker, &GrpcWorker::disconnected, this, &GrpcConnectionManager::onWorkerDisconnected, Qt::QueuedConnection);

  m_pWorker->start();

  m_cleanupTimer = new QTimer(this);
  connect(m_cleanupTimer, &QTimer::timeout, this, &GrpcConnectionManager::onCleanupTimer);
  m_cleanupTimer->start(10000);
}

GrpcConnectionManager::~GrpcConnectionManager() {
  if (m_pWorker) {
    m_pWorker->stop();
    delete m_pWorker;
  }
}

bool GrpcConnectionManager::sendRawEnvelope(const broker::BrokerPayload& envelope) {
  if (m_pWorker) {
    return m_pWorker->writeMessage(envelope);
  }
  return false;
}

bool GrpcConnectionManager::sendDataInternal(const QString& key, const QByteArray& data) {
  if (!m_isConnected)
    return false;

  broker::BrokerPayload msg;
  msg.set_handler_key(key.toStdString());
  msg.set_sender_id(m_appName);
  msg.set_topic(key.toStdString());
  msg.set_raw_data(data.toStdString());

  return sendRawEnvelope(msg);
}

bool GrpcConnectionManager::sendDataRawInternal(const QString& key, const char* data, int len) {
  if (!m_isConnected)
    return false;

  broker::BrokerPayload msg;
  msg.set_handler_key(key.toStdString());
  msg.set_sender_id(m_appName);
  msg.set_topic(key.toStdString());
  msg.set_raw_data(data, len);

  return sendRawEnvelope(msg);
}

bool GrpcConnectionManager::sendFileInternal(const QString& key, const QString& filePath) {
  if (!m_isConnected)
    return false;

  QFileInfo check(filePath);
  if (!check.exists() || !check.isReadable())
    return false;

  QtConcurrent::run([this, key, filePath]() {
    QFile file(filePath);
    if (!file.open(QIODevice::ReadOnly))
      return;

    QFileInfo fileInfo(filePath);
    qint64 totalSize = file.size();
    std::string transferId = QUuid::createUuid().toString().toStdString();
    std::string stdTopic = key.toStdString();

    QCryptographicHash hasher(QCryptographicHash::Sha256);

    QJsonObject meta;
    meta["filename"] = fileInfo.fileName();
    meta["size"] = totalSize;
    meta["transfer_id"] = QString::fromStdString(transferId);

    broker::BrokerPayload metaMsg;
    metaMsg.set_handler_key("__FILE_META__");
    metaMsg.set_sender_id(m_appName);
    metaMsg.set_topic(stdTopic);
    metaMsg.set_transfer_id(transferId);
    metaMsg.set_raw_data(QJsonDocument(meta).toJson(QJsonDocument::Compact).toStdString());

    if (!sendRawEnvelope(metaMsg))
      return;

    int totalChunks = (totalSize + STREAM_CHUNK_SIZE - 1) / STREAM_CHUNK_SIZE;
    int sequence = 0;

    while (!file.atEnd()) {
      QByteArray chunkData = file.read(STREAM_CHUNK_SIZE);
      hasher.addData(chunkData);

      broker::BrokerPayload msg;
      msg.set_handler_key("__CHUNK__");
      msg.set_sender_id(m_appName);
      msg.set_topic(stdTopic);
      msg.set_transfer_id(transferId);
      msg.set_sequence_number(sequence++);
      msg.set_sequence_count(totalChunks);
      msg.set_raw_data(chunkData.constData(), chunkData.size());

      if (!sendRawEnvelope(msg)) {
        QThread::msleep(100);
        if (!sendRawEnvelope(msg))
          break;
      }

      if (sequence % 16 == 0)
        QThread::msleep(1);
    }
    file.close();

    QByteArray finalHash = hasher.result();
    broker::BrokerPayload footerMsg;
    footerMsg.set_handler_key("__FILE_FOOTER__");
    footerMsg.set_sender_id(m_appName);
    footerMsg.set_topic(stdTopic);
    footerMsg.set_transfer_id(transferId);
    footerMsg.set_raw_data(finalHash.toStdString());

    sendRawEnvelope(footerMsg);
  });

  return true;
}

void GrpcConnectionManager::registerInternal(const QString& key, MessageCallback callback) {
  QMutexLocker lock(&m_mapMutex);
  m_byteHandlers.insert(key, callback);
  if (m_isConnected) {
    broker::BrokerPayload subMsg;
    subMsg.set_handler_key("__SUBSCRIBE__");
    subMsg.set_sender_id(m_appName);
    subMsg.set_topic(key.toStdString());
    sendRawEnvelope(subMsg);
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
    safeId.replace("{", "").replace("}", "");
    newItem.tempFilePath = tempDir + "/grpc_" + safeId + ".dat";

    newItem.tempFile = new QFile(newItem.tempFilePath);
    if (!newItem.tempFile->open(QIODevice::ReadWrite)) {
      qCritical() << "Failed to create temp file:" << newItem.tempFilePath;
      delete newItem.tempFile;
      return;
    }

    newItem.hasher = new QCryptographicHash(QCryptographicHash::Sha256);
    m_incomingTransfers.insert(transferId, newItem);
    return;
  }

  if (handlerKey == "__CHUNK__") {
    QMutexLocker lock(&m_mapMutex);
    if (!m_incomingTransfers.contains(transferId))
      return;

    IncomingTransfer& transfer = m_incomingTransfers[transferId];
    transfer.lastUpdateTimestamp = QDateTime::currentMSecsSinceEpoch();
    transfer.totalChunks = msg.sequence_count();

    QByteArray chunkData = QByteArray::fromStdString(msg.raw_data());
    qint64 offset = static_cast<qint64>(msg.sequence_number()) * STREAM_CHUNK_SIZE;

    if (transfer.tempFile && transfer.tempFile->isOpen()) {
      transfer.tempFile->seek(offset);
      transfer.tempFile->write(chunkData);
      if (transfer.hasher)
        transfer.hasher->addData(chunkData);
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
      }
    }
    return;
  }

  QByteArray data;
  if (msg.has_payload()) {
    std::string s;
    msg.payload().SerializeToString(&s);
    data = QByteArray::fromStdString(s);
  } else {
    data = QByteArray::fromStdString(msg.raw_data());
  }
  processPayload(handlerKey, data);
}

void GrpcConnectionManager::processPayload(const QString& key, const QByteArray& data) {
  QMutexLocker lock(&m_mapMutex);
  if (m_byteHandlers.contains(key)) {
    m_byteHandlers[key](data);
  }
}

void GrpcConnectionManager::processFilePayload(const QString& key, const QString& filePath) {
  QMutexLocker lock(&m_mapMutex);
  if (m_fileHandlers.contains(key)) {
    m_fileHandlers[key](filePath);
  }
}

void GrpcConnectionManager::onWorkerConnected() {
  QMutexLocker lock(&m_mapMutex);
  m_isConnected = true;
  for (auto& cb : m_statusCallbacks) {
    cb(true);
  }

  QStringList allTopics = m_byteHandlers.keys() + m_fileHandlers.keys();
  allTopics.removeDuplicates();
  for (const QString& topic : std::as_const(allTopics)) {
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
  for (auto& cb : m_statusCallbacks) {
    cb(false);
  }
}

void GrpcConnectionManager::onCleanupTimer() {
  QMutexLocker lock(&m_mapMutex);
  qint64 now = QDateTime::currentMSecsSinceEpoch();
  auto it = m_incomingTransfers.begin();
  while (it != m_incomingTransfers.end()) {
    if (now - it.value().lastUpdateTimestamp > 30000) {
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
