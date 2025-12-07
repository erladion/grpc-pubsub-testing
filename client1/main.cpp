#include <QCoreApplication>

#include <QDateTime>

#include "grpcconnectionmanager.h"

#include "protobuf_forward.h"

#include "update.grpc.pb.h"
#include "update.pb.h"

int main(int argc, char* argv[]) {
  QCoreApplication a(argc, argv);

  GrpcConnectionManager::init();

  GrpcConnectionManager::registerCallback<communication::Update>("test", [](const communication::Update& message) {
    qDebug() << QString::fromStdString(message.message());
    qDebug() << message.timestamp_utc();

    qDebug() << "Received data";
    communication::Update update;
    update.set_id("client1");
    update.set_message("Testing a return message");
    update.set_timestamp_utc(QDateTime::currentMSecsSinceEpoch());

    GrpcConnectionManager::sendMessage("MessageReceived", update);
  });

  GrpcConnectionManager::registerFileCallback("file", [](const QString& path) { qWarning() << "Received file at path:" << path; });

  return a.exec();
}
