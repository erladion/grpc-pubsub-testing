#include <QCoreApplication>

#include <QDebug>
#include <QObject>
#include <QThread>

#include <thread>
#include "server.h"

// #include "grpcservermanager.h"

int main(int argc, char* argv[]) {
  QCoreApplication a(argc, argv);

  /*
  QThread serverThread;
  GrpcServerManager manager;

  manager.moveToThread(&serverThread);

  QObject::connect(&serverThread, &QThread::started, &manager, std::bind(&GrpcServerManager::startServer, &manager, QString("0.0.0.0:50051")));
  QObject::connect(&manager, &GrpcServerManager::serverStarted, []() { qDebug() << "Broker is active and ready"; });

  QObject::connect(&serverThread, &QThread::finished, &manager, &GrpcServerManager::deleteLater);

  serverThread.start();


  serverThread.quit();
  serverThread.wait();
  */

  std::thread serverThread([]() {
    AsyncServer server;
    server.Run({"0.0.0.0:50051", "unix:///tmp/broker.sock"});
  });

  serverThread.detach();
  int result = a.exec();

  return result;
}
