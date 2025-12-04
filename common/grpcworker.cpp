#include "grpcworker.h"

#include <QDebug>

GrpcWorker::GrpcWorker(const QString& targetAddress, QObject* parent) : QThread(parent), m_target(targetAddress), m_running(true) {
  qRegisterMetaType<broker::BrokerPayload>();
}

GrpcWorker::~GrpcWorker() {
  stop();
  wait();
}

void GrpcWorker::stop() {
  m_running = false;

  QMutexLocker lock(&m_streamMutex);
  if (m_context) {
    m_context->TryCancel();
  }
}

void GrpcWorker::run() {
  grpc::ChannelArguments args;
  args.SetInt(GRPC_ARG_KEEPALIVE_TIME_MS, 10000);    // Ping every 10s
  args.SetInt(GRPC_ARG_KEEPALIVE_TIMEOUT_MS, 5000);  // Timeout 5s
  args.SetInt(GRPC_ARG_HTTP2_MAX_PINGS_WITHOUT_DATA, 0);

  m_channel = grpc::CreateCustomChannel(m_target.toStdString(), grpc::InsecureChannelCredentials(), args);
  m_stub = broker::BrokerService::NewStub(m_channel);

  while (m_running) {
    auto newContext = std::make_shared<grpc::ClientContext>();
    newContext->set_compression_algorithm(GRPC_COMPRESS_GZIP);

    auto newStream = m_stub->MessageStream(newContext.get());

    if (!newStream) {
      for (int i(0); i < 30 && m_running; ++i) {
        QThread::msleep(100);
      }
      continue;
    }

    {
      QMutexLocker lock(&m_streamMutex);
      m_context = newContext;
      m_stream = std::move(newStream);
    }

    qDebug() << "gRPC Stream Connected to" << m_target;
    emit connected();

    broker::BrokerPayload incomingMsg;

    while (m_running && m_stream->Read(&incomingMsg)) {
      emit envelopeReceived(incomingMsg);

      QString key = QString::fromStdString(incomingMsg.handler_key());

      std::string rawStr;
      if (incomingMsg.payload().SerializeToString(&rawStr)) {
        QByteArray rawData(rawStr.data(), static_cast<int>(rawStr.size()));
        emit payloadReceived(key, rawData);
      }
    }

    if (m_running) {
      qWarning() << "Disconnected from Broker. Attempting reconnect in 3s...";
      emit disconnected();

      {
        QMutexLocker lock(&m_streamMutex);
        m_context.reset();
        m_stream.reset();
      }

      for (int i = 0; i < 30 && m_running; i++) {
        QThread::msleep(100);
      }
    }
  }
  QMutexLocker lock(&m_streamMutex);
  m_stream.reset();
  m_context.reset();
}

bool GrpcWorker::writeMessage(const broker::BrokerPayload& msg) {
  QMutexLocker lock(&m_streamMutex);
  if (m_stream) {
    grpc::WriteOptions options;

    if (msg.payload().ByteSizeLong() <= 1024) {
      options.set_no_compression();
    }

    if (!m_stream->Write(msg)) {
      qWarning() << "Failed to write message to gRPC stream.";
      return false;
    }
    return true;
  }

  return false;
}
