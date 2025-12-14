#include "grpcworker.h"

#include <QDebug>
#include <chrono>

GrpcWorker::GrpcWorker(const QString& targetAddress, QObject* parent) : QThread(parent), m_target(targetAddress), m_running(true) {
  qRegisterMetaType<broker::BrokerPayload>();
}

GrpcWorker::~GrpcWorker() {
  stop();
  wait();
}

void GrpcWorker::stop() {
  m_running = false;

  {
    QMutexLocker lock(&m_streamMutex);
    if (m_context) {
      m_context->TryCancel();
    }
  }

  m_sleepCv.notify_all();
}

bool GrpcWorker::responsiveSleep(int milliseconds) {
  std::unique_lock<std::mutex> lock(m_sleepMutex);
  return !m_sleepCv.wait_for(lock, std::chrono::milliseconds(milliseconds), [this] { return !m_running; });
}

void GrpcWorker::run() {
  grpc::ChannelArguments args;
  args.SetInt(GRPC_ARG_KEEPALIVE_TIME_MS, 10000);
  args.SetInt(GRPC_ARG_KEEPALIVE_TIMEOUT_MS, 5000);
  args.SetInt(GRPC_ARG_HTTP2_MAX_PINGS_WITHOUT_DATA, 0);
  args.SetInt(GRPC_ARG_KEEPALIVE_PERMIT_WITHOUT_CALLS, 1);
  args.SetInt(GRPC_ARG_MAX_RECEIVE_MESSAGE_LENGTH, 50 * 1024 * 1024);
  args.SetInt(GRPC_ARG_MAX_SEND_MESSAGE_LENGTH, 50 * 1024 * 1024);

  m_channel = grpc::CreateCustomChannel(m_target.toStdString(), grpc::InsecureChannelCredentials(), args);
  m_stub = broker::BrokerService::NewStub(m_channel);

  while (m_running) {
    if (m_channel->GetState(true) != GRPC_CHANNEL_READY) {
      // Sleep 3s, but wake immediately if stop() is called
      if (!responsiveSleep(3000))
        break;
      continue;
    }

    auto newContext = std::make_shared<grpc::ClientContext>();
    newContext->set_compression_algorithm(GRPC_COMPRESS_GZIP);

    auto newStream = m_stub->MessageStream(newContext.get());

    if (!newStream) {
      if (!responsiveSleep(3000))
        break;
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

    // Blocking Read (Will return false if TryCancel is called in stop())
    while (m_running && m_stream->Read(&incomingMsg)) {
      emit envelopeReceived(incomingMsg);
    }

    if (m_running) {
      qWarning() << "Disconnected from Broker. Attempting reconnect in 3s...";
      emit disconnected();

      {
        QMutexLocker lock(&m_streamMutex);
        m_context.reset();
        m_stream.reset();
      }

      if (!responsiveSleep(3000))
        break;
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
    if (msg.payload().ByteSizeLong() <= 1024)
      options.set_no_compression();

    if (!m_stream->Write(msg)) {
      qWarning() << "Failed to write message to gRPC stream.";
      return false;
    }
    return true;
  }
  return false;
}
