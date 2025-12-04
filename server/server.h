#ifndef SERVER_H
#define SERVER_H

#include <grpcpp/grpcpp.h>
#include <thread>
#include <vector>
#include "calldata.h"

#include <QDebug>

class AsyncServer {
public:
  ~AsyncServer() {
    qDebug() << "CRITICAL: AsyncServer is being destroyed!";
    m_server->Shutdown();
    m_completionQueue->Shutdown();
  }

  void Run(const std::string& address, int threads = 4) {
    grpc::ServerBuilder builder;
    builder.AddListeningPort(address, grpc::InsecureServerCredentials());
    builder.RegisterService(&m_service);

    // KeepAlive
    builder.AddChannelArgument(GRPC_ARG_KEEPALIVE_TIME_MS, 10000);
    builder.AddChannelArgument(GRPC_ARG_KEEPALIVE_TIMEOUT_MS, 5000);
    builder.AddChannelArgument(GRPC_ARG_HTTP2_MAX_PINGS_WITHOUT_DATA, 0);

    // Set maximum message size
    builder.SetMaxReceiveMessageSize(50 * 1024 * 1024);
    builder.SetMaxSendMessageSize(50 * 1024 * 1024);

    // Create the Completion Queue
    m_completionQueue = builder.AddCompletionQueue();
    m_server = builder.BuildAndStart();

    std::cout << "Async Server listening on " << address << " with " << threads << " threads" << std::endl;

    // Spawn the first CallData to wait for the first connection
    for (int i(0); i < threads; ++i) {
      new CallData(&m_service, m_completionQueue.get());
    }

    for (int i(0); i < threads; ++i) {
      m_threads.emplace_back(&AsyncServer::HandleRpcs, this);
    }

    for (std::thread& thread : m_threads) {
      if (thread.joinable()) {
        thread.join();
      }
    }
  }

private:
  void HandleRpcs() {
    // Handle events
    void* tag;
    bool ok;
    while (true) {
      // BLOCKING WAIT for next event
      if (!m_completionQueue->Next(&tag, &ok)) {
        break;  // Queue shutdown
      }

      // Dispatch
      Tag* t = static_cast<Tag*>(tag);
      t->connection->Proceed(t, ok);
    }
  }

private:
  broker::BrokerService::AsyncService m_service;
  std::unique_ptr<ServerCompletionQueue> m_completionQueue;
  std::unique_ptr<grpc::Server> m_server;
  std::vector<std::thread> m_threads;
};

#endif
