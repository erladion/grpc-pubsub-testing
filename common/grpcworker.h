#ifndef GRPCWORKER_H
#define GRPCWORKER_H

#include <grpcpp/grpcpp.h>

#include <atomic>
#include <condition_variable>
#include <functional>
#include <memory>
#include <thread>

#include "protobuf_forward.h"
#include "safequeue.h"

struct ConnectionConfig {
  std::string address{"127.0.0.1:50051"};
  std::string clientId;
  int keepAliveTime = 10000;
  int keepAliveTimeout = 5000;
  int compressionAlgo = 2;  // GZIP
};

class GrpcWorker {
public:
  using StatusCallback = std::function<void(bool)>;
  using MessageCallback = std::function<void(const broker::BrokerPayload&)>;

  explicit GrpcWorker(const ConnectionConfig& config, SafeQueue<broker::BrokerPayload>* inboundQueue, StatusCallback callback);

  virtual ~GrpcWorker();

  void start();
  void stop();
  bool writeMessage(const broker::BrokerPayload& msg);
  void setMessageCallback(MessageCallback callback);

protected:
  void run();

private:
  ConnectionConfig m_config;
  SafeQueue<broker::BrokerPayload>* m_inboundQueue;
  StatusCallback m_statusCallback;

  std::mutex m_callbackMutex;
  MessageCallback m_messageCallback;

  std::atomic<bool> m_running;
  std::thread m_workerThread;

  std::shared_ptr<grpc::Channel> m_channel;
  std::unique_ptr<broker::BrokerService::Stub> m_stub;

  std::mutex m_streamMutex;
  std::shared_ptr<grpc::ClientContext> m_context;
  std::shared_ptr<grpc::ClientReaderWriter<broker::BrokerPayload, broker::BrokerPayload>> m_stream;
};

#endif  // GRPCWORKER_H
