#include <QCoreApplication>

#include <QDateTime>
#include <QTimer>

#include "grpcconnectionmanager.h"

#include "protobuf_forward.h"

#include "update.grpc.pb.h"
#include "update.pb.h"

int main(int argc, char* argv[]) {
  QCoreApplication a(argc, argv);

  GrpcConnectionManager::init("client2");

  GrpcConnectionManager::registerCallback<communication::Update>("MessageReceived",
                                                                 [](const communication::Update& message) { qDebug() << "Got return message"; });

  GrpcConnectionManager::registerCallback("MessageReceived2", [](const std::string& data) {
    communication::Update msg;
    if (msg.ParseFromArray(data.c_str(), data.size())) {
      qDebug() << "Received from C:" << msg.message().c_str();
    } else {
      qWarning() << "Failed to parse C message";
    }
  });

  QTimer t;
  QObject::connect(&t, &QTimer::timeout, []() {
    communication::Update update;
    update.set_id("client2");
    update.set_message("Sending a message");
    update.set_timestamp_utc(QDateTime::currentMSecsSinceEpoch());

    GrpcConnectionManager::sendMessage("test", update);
  });
  t.start(2000);

  QTimer tt;
  QObject::connect(&tt, &QTimer::timeout, []() {
    communication::Update update;
    update.set_id("client2");
    update.set_message("Sending another message");
    update.set_timestamp_utc(QDateTime::currentMSecsSinceEpoch());

    GrpcConnectionManager::sendData("test", update.SerializeAsString());
  });
  tt.start(2500);

  QTimer::singleShot(5000, [&]() { GrpcConnectionManager::sendFile("file", "/mnt/c/Users/johan/Downloads/logo1.png"); });

  return a.exec();
}
