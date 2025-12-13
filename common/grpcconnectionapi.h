#ifndef GRPCCONNECTIONAPI_H
#define GRPCCONNECTIONAPI_H

#ifdef __cplusplus
extern "C" {
#endif

// Export macros for Windows/Linux compatibility
#if defined(_WIN32)
#ifdef BUILDING_GRPC_DLL
#define GRPC_API __declspec(dllexport)
#else
#define GRPC_API __declspec(dllimport)
#endif
#else
#define GRPC_API
#endif

// --- TYPES ---

// Callback function pointer signature
// topic: The topic the message arrived on
// data:  Pointer to raw bytes
// len:   Length of data
// user_data: Custom pointer passed during registration (context)
typedef void (*GrpcMessageCallback)(const char* topic, const char* data, int len, void* user_data);

// File callback signature
typedef void (*GrpcFileCallback)(const char* topic, const char* filepath, void* user_data);

// Initialize the library (starts Qt Core internally)
// address: "127.0.0.1:50051" or "unix:///tmp/broker.sock"
GRPC_API void grpc_api_init(const char* address);

// Drive the internal event loop.
// MUST be called periodically (e.g., inside a while(1) loop) in the C app.
GRPC_API void grpc_process_events();

// Send raw bytes
GRPC_API void grpc_send_data(const char* topic, const char* data, int len);

// Send a string (convenience wrapper)
GRPC_API void grpc_send_text(const char* topic, const char* text);

// Stream a file from disk
GRPC_API void grpc_send_file(const char* topic, const char* filepath);

// Register a callback for raw messages
GRPC_API void grpc_register_callback(const char* topic, GrpcMessageCallback cb, void* user_data);

// Register a callback for file downloads
GRPC_API void grpc_register_file_callback(const char* topic, GrpcFileCallback cb, void* user_data);

#ifdef __cplusplus
}
#endif

#endif  // GRPCCONNECTIONAPI_H
