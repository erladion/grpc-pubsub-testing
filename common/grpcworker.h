#ifndef GRPCWORKER_H
#define GRPCWORKER_H

#include <grpcpp/grpcpp.h>
#include <QByteArray>
#include <QMutex>
#include <QThread>
#include <atomic>
#include <condition_variable>
#include <memory>

#include "protobuf_forward.h"

Q_DECLARE_METATYPE(broker::BrokerPayload)

struct WorkerConfig {
  QString targetAddress;
  int compressionAlgo = 2;  // Default GZIP
  int keepAliveTime = 10000;
  int keepAliveTimeout = 5000;
};

class GrpcWorker : public QThread {
  Q_OBJECT
public:
  explicit GrpcWorker(const WorkerConfig& config, QObject* parent = nullptr);

  ~GrpcWorker() override;

  bool writeMessage(const broker::BrokerPayload& msg);
  void stop();

protected:
  void run() override;

signals:
  void envelopeReceived(const broker::BrokerPayload& msg);
  void connected();
  void disconnected();
  void payloadReceived(const QString& handlerKey, const QByteArray& rawData);

private:
  bool responsiveSleep(int milliseconds);

private:
  WorkerConfig m_config;

  std::atomic<bool> m_running;

  std::mutex m_sleepMutex;
  std::condition_variable m_sleepCv;

  std::shared_ptr<grpc::Channel> m_channel;
  std::unique_ptr<broker::BrokerService::Stub> m_stub;

  std::shared_ptr<grpc::ClientContext> m_context;
  std::shared_ptr<grpc::ClientReaderWriter<broker::BrokerPayload, broker::BrokerPayload>> m_stream;

  QMutex m_streamMutex;
};

#endif  // GRPCWORKER_H
