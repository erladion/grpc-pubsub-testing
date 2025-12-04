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

  void Run(const std::string& address) {
    grpc::ServerBuilder builder;
    builder.AddListeningPort(address, grpc::InsecureServerCredentials());
    builder.RegisterService(&m_service);

    // Send a ping every 10 seconds if no data is seen
    builder.AddChannelArgument(GRPC_ARG_KEEPALIVE_TIME_MS, 10000);
    // Wait 5 seconds for a pong response before considering the client dead
    builder.AddChannelArgument(GRPC_ARG_KEEPALIVE_TIMEOUT_MS, 5000);
    // Allow pings even if there are no ongoing RPC calls
    builder.AddChannelArgument(GRPC_ARG_HTTP2_MAX_PINGS_WITHOUT_DATA, 0);

    // Set maximum message size
    builder.SetMaxReceiveMessageSize(50 * 1024 * 1024);
    builder.SetMaxSendMessageSize(50 * 1024 * 1024);

    // Create the Completion Queue
    m_completionQueue = builder.AddCompletionQueue();
    m_server = builder.BuildAndStart();

    std::cout << "Async Server listening on " << address << std::endl;

    // Spawn the first CallData to wait for the first connection
    new CallData(&m_service, m_completionQueue.get());

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
};

#endif
