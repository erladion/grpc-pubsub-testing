#include <QCoreApplication>

#include <QDebug>
#include <QObject>
#include <QThread>

#include <thread>
#include "server.h"

int main(int argc, char* argv[]) {
  QCoreApplication a(argc, argv);

  std::thread serverThread([]() {
    AsyncServer server;
    server.Run({"0.0.0.0:50051", "unix:///tmp/broker.sock"});
  });

  serverThread.detach();
  int result = a.exec();

  return result;
}
