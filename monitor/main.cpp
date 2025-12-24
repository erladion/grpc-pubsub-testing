#include <QApplication>

#include "grpcconnectionmanager.h"

#include "monitorwindow.h"

int main(int argc, char* argv[]) {
  QApplication a(argc, argv);

  GrpcConnectionManager::init("unix:///tmp/broker.sock");

  MonitorWindow mw;
  mw.show();

  return a.exec();
}
