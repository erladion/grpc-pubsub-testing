#include <QApplication>

#include "grpcconnectionmanager.h"

#include "monitorwindow.h"

int main(int argc, char* argv[]) {
  QApplication a(argc, argv);

  MonitorWindow mw;
  mw.show();

  return a.exec();
}
