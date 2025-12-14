#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>

#include "grpcconnectionapi.h"
#include "update.pb-c.h"

void on_message(const char* topic, const char* data, int len, void* ctx) {
  // 1. Verify Topic
  if (strcmp(topic, "test") != 0) return;

  // 2. Deserialize (Unpack)
  Communication__Update *msg = communication__update__unpack(NULL, len, (const uint8_t*)data);

  if (msg == NULL) {
    printf("Error: Failed to unpack protobuf.\n");
    return;
  }

  printf("Message: %s | ID: %s\n", msg->message, msg->id);
  fflush(stdout);

  communication__update__free_unpacked(msg, NULL);

  Communication__Update response = COMMUNICATION__UPDATE__INIT;

  response.id = "Client-C";
  response.message = "Hello from the other side!";
  response.timestamp_utc = 12345;

  size_t size = communication__update__get_packed_size(&response);

  uint8_t *buffer = malloc(size);
  if (!buffer) return;

  // Serialize
  communication__update__pack(&response, buffer);

  sendData("MessageReceived2", (const char*)buffer, size);
}

int main() {
  // Connect
  initConnection("127.0.0.1:50051");

  // Subscribe
  registerCallback("test", on_message, NULL);

  // Main Loop
  printf("Listening for C++ messages...\n");
  fflush(stdout);
  while(1) {
    processEvents(); // Process network events
    usleep(10000); // 10ms sleep
  }
  return 0;
}
