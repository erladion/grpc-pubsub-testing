#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>

#include "grpcconnectionapi.h"
#include "update.pb-c.h"

void on_status(int status, void* ctx) {
  if (status == GRPC_STATUS_CONNECTED) {
    printf("[C-Client] Status: CONNECTED\n");
  } else {
    printf("[C-Client] Status: DISCONNECTED (Retrying...)\n");
  }
  fflush(stdout);
}

void on_message(const char* topic, const char* data, int len, void* ctx) {
  printf("[C-Client] Message received on topic: %s\n", topic);

  if (strcmp(topic, "test") != 0) {
    return;
  }

  Communication__Update *msg = communication__update__unpack(NULL, len, (const uint8_t*)data);

  if (msg == NULL) {
    printf("Error: Failed to unpack protobuf.\n");
    return;
  }

  printf("Message Content: %s | ID: %s\n", msg->message, msg->id);
  fflush(stdout);

  communication__update__free_unpacked(msg, NULL);

  Communication__Update response = COMMUNICATION__UPDATE__INIT;
  response.id = "Client-C";
  response.message = "Hello from C";
  response.timestamp_utc = 12345;

  size_t size = communication__update__get_packed_size(&response);
  uint8_t *buffer = malloc(size);

  if (buffer) {
    communication__update__pack(&response, buffer);
    sendData("MessageReceived2", (const char*)buffer, size);
    free(buffer);
  }
}

int main() {
  setvbuf(stdout, NULL, _IONBF, 0);

  GrpcConfig config;
  config.address = "127.0.0.1:50051";
  config.client_id = "c-client-1";

  printf("Registering callbacks...\n");
  fflush(stdout);
  registerStatusCallback(on_status, NULL);
  registerCallback("test", on_message, NULL);

  printf("Initializing...\n");
  fflush(stdout);
  initConnection(&config);

  printf("Running...\n");
  fflush(stdout);
  while(1) { usleep(100000); }
}
