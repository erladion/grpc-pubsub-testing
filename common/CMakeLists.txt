add_library(GrpcLib STATIC
    ${PROTO_GENERATED_SOURCES}
    ${PROTO_GENERATED_HEADERS}
    grpcconnectionmanager.cpp
    grpcconnectionmanager.h
    grpcworker.cpp
    grpcworker.h
    protobuf_forward.h
)

target_include_directories(GrpcLib PUBLIC ${GENERATED_DIR})

target_link_libraries(GrpcLib PUBLIC
    Qt${QT_VERSION_MAJOR}::Core
    gRPC::grpc++
    protobuf::libprotobuf
    absl::base
    absl::strings
    absl::cord
    absl::synchronization
    absl::time
    absl::flags
    absl::log
)

add_library(GrpcConnectionLib STATIC
    grpcconnectionapi.cpp
    grpcconnectionapi.h
)

target_link_libraries(GrpcConnectionLib PRIVATE GrpcLib)
target_link_libraries(GrpcConnectionLib PRIVATE Qt6::Core)
