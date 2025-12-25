#include "grpcworker.h"
#include <chrono>
#include <iostream>

GrpcWorker::GrpcWorker(const ConnectionConfig& config, SafeQueue<broker::BrokerPayload>* inboundQueue, StatusCallback callback)
    : m_config(config), m_inboundQueue(inboundQueue), m_statusCallback(callback), m_running(false) {}

GrpcWorker::~GrpcWorker() {
  stop();
}

void GrpcWorker::setMessageCallback(MessageCallback callback) {
  std::lock_guard<std::mutex> lock(m_callbackMutex);
  m_messageCallback = callback;
}

void GrpcWorker::start() {
  m_running = true;
  m_workerThread = std::thread(&GrpcWorker::run, this);
}

void GrpcWorker::stop() {
  m_running = false;
  {
    std::lock_guard<std::mutex> lock(m_streamMutex);
    if (m_context) {
      m_context->TryCancel();
    }
  }
  if (m_workerThread.joinable()) {
    m_workerThread.join();
  }
}

void GrpcWorker::run() {
  grpc::ChannelArguments args;
  args.SetInt(GRPC_ARG_KEEPALIVE_TIME_MS, m_config.keepAliveTime);
  args.SetInt(GRPC_ARG_KEEPALIVE_TIMEOUT_MS, m_config.keepAliveTimeout);
  args.SetInt(GRPC_ARG_HTTP2_MAX_PINGS_WITHOUT_DATA, 0);
  args.SetInt(GRPC_ARG_KEEPALIVE_PERMIT_WITHOUT_CALLS, 1);
  args.SetInt(GRPC_ARG_MAX_RECEIVE_MESSAGE_LENGTH, 50 * 1024 * 1024);
  args.SetInt(GRPC_ARG_MAX_SEND_MESSAGE_LENGTH, 50 * 1024 * 1024);

  m_channel = grpc::CreateCustomChannel(m_config.address, grpc::InsecureChannelCredentials(), args);
  m_stub = broker::BrokerService::NewStub(m_channel);

  while (m_running) {
    if (m_channel->GetState(true) != GRPC_CHANNEL_READY) {
      std::this_thread::sleep_for(std::chrono::milliseconds(1000));
      continue;
    }

    auto newContext = std::make_shared<grpc::ClientContext>();
    newContext->set_compression_algorithm(static_cast<grpc_compression_algorithm>(m_config.compressionAlgo));

    auto newStream = m_stub->MessageStream(newContext.get());
    if (!newStream) {
      std::this_thread::sleep_for(std::chrono::milliseconds(3000));
      continue;
    }

    {
      std::lock_guard<std::mutex> lock(m_streamMutex);
      m_context = newContext;
      m_stream = std::move(newStream);
    }

    if (m_statusCallback) {
      m_statusCallback(true);
    }

    broker::BrokerPayload incoming;
    while (m_running && m_stream->Read(&incoming)) {
      if (m_inboundQueue) {
        m_inboundQueue->push(incoming);
      } else {
        if (m_messageCallback) {
          std::lock_guard<std::mutex> lock(m_callbackMutex);
          m_messageCallback(incoming);
        }
      }
    }

    if (m_statusCallback) {
      m_statusCallback(false);
    }

    {
      std::lock_guard<std::mutex> lock(m_streamMutex);
      m_context.reset();
      m_stream.reset();
    }

    if (m_running) {
      std::this_thread::sleep_for(std::chrono::milliseconds(3000));
    }
  }
}

bool GrpcWorker::writeMessage(const broker::BrokerPayload& msg) {
  std::lock_guard<std::mutex> lock(m_streamMutex);
  if (!m_stream) {
    return false;
  }

  grpc::WriteOptions options;
  if (msg.raw_data().size() <= 1024) {
    options.set_no_compression();
  }

  return m_stream->Write(msg, options);
}
