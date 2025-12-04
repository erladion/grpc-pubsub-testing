#include <QCoreApplication>

#include <QDateTime>
#include <QTimer>

#include "grpcconnectionmanager.h"

#include "protobuf_forward.h"

#include "update.grpc.pb.h"
#include "update.pb.h"

int main(int argc, char* argv[]) {
  QCoreApplication a(argc, argv);

  GrpcConnectionManager::init();

  GrpcConnectionManager::registerCallback<communication::Update>("MessageReceived",
                                                                 [](const communication::Update& message) { qDebug() << "Got return message"; });

  QTimer t;
  QObject::connect(&t, &QTimer::timeout, []() {
    communication::Update update;
    update.set_id("client2");
    update.set_message("Sending a message");
    update.set_timestamp_utc(QDateTime::currentMSecsSinceEpoch());

    GrpcConnectionManager::sendMessage("test", update);
  });

  t.start(2000);

  return a.exec();
}
