#ifndef GRPCCONNECTIONAPI_H
#define GRPCCONNECTIONAPI_H

#ifdef __cplusplus
extern "C" {
#endif

#if defined(_WIN32)
#ifdef BUILDING_GRPC_DLL
#define GRPC_API __declspec(dllexport)
#else
#define GRPC_API __declspec(dllimport)
#endif
#else
#define GRPC_API
#endif

typedef enum {
  GRPC_SUCCESS = 0,
  GRPC_ERROR_GENERIC = -1,
  GRPC_ERROR_NO_CONNECTION = -2,
  GRPC_ERROR_INVALID_ARGS = -3,
  GRPC_ERROR_SEND_FAILED = -4
} GrpcErrorCode;

typedef enum { GRPC_STATUS_DISCONNECTED = 0, GRPC_STATUS_CONNECTING = 1, GRPC_STATUS_CONNECTED = 2 } GrpcConnectionStatus;

typedef struct {
  const char* address;       // e.g. "127.0.0.1:50051"
  const char* client_id;     // NEW: e.g. "Camera-1" (Identifying Name)
  int keepalive_time_ms;     // Default: 10000
  int keepalive_timeout_ms;  // Default: 5000
} GrpcConfig;

typedef void (*GrpcMessageCallback)(const char* topic, const char* data, int len, void* user_data);
typedef void (*GrpcFileCallback)(const char* topic, const char* filepath, void* user_data);
typedef void (*GrpcStatusCallback)(GrpcConnectionStatus status, void* user_data);

GRPC_API int initConnection(const GrpcConfig* config);

GRPC_API void shutdownConnection();

GRPC_API void processEvents();

GRPC_API void registerStatusCallback(GrpcStatusCallback cb, void* user_data);

GRPC_API int sendData(const char* topic, const char* data, int len);

GRPC_API int sendText(const char* topic, const char* text);

GRPC_API int sendFile(const char* topic, const char* filepath);

GRPC_API void registerCallback(const char* topic, GrpcMessageCallback cb, void* user_data);

GRPC_API void registerFileCallback(const char* topic, GrpcFileCallback cb, void* user_data);

#ifdef __cplusplus
}
#endif

#endif  // GRPCCONNECTIONAPI_H
