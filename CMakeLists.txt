cmake_minimum_required(VERSION 3.14)

project(grpc LANGUAGES CXX)

set(CMAKE_AUTOUIC ON)
set(CMAKE_AUTOMOC ON)
set(CMAKE_AUTORCC ON)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

set(gRPC_DIR "/home/erladion/.local/lib/cmake/grpc" CACHE PATH "Test")

list(APPEND CMAKE_PREFIX_PATH "/home/erladion/.local/")

find_package(QT NAMES Qt6 Qt5 REQUIRED COMPONENTS Core)
find_package(Qt${QT_VERSION_MAJOR} REQUIRED COMPONENTS Core)

find_package(Protobuf REQUIRED)
find_package(gRPC CONFIG REQUIRED)
find_package(absl CONFIG REQUIRED)

set(PROTO_SRC broker.proto)
set(PROTO_DIR ${CMAKE_CURRENT_SOURCE_DIR}/common)
set(GENERATED_DIR ${CMAKE_CURRENT_SOURCE_DIR}/common/generated)

file(MAKE_DIRECTORY ${GENERATED_DIR})

message(${GENERATED_DIR})
message(${GENERATED_DIR}/${PROTO_SRC})

set(PROTO_GENERATED_SOURCES
    ${GENERATED_DIR}/broker.pb.cc
    ${GENERATED_DIR}/broker.grpc.pb.cc
    ${GENERATED_DIR}/update.pb.cc
    ${GENERATED_DIR}/update.grpc.pb.cc
)

set(PROTO_GENERATED_HEADERS
    ${GENERATED_DIR}/broker.pb.h
    ${GENERATED_DIR}/broker.grpc.pb.h
    ${GENERATED_DIR}/update.pb.h
    ${GENERATED_DIR}/update.grpc.pb.h
)

# server/grpcservermanager.h
# server/grpcservermanager.cpp
# server/grpcbrokerservice.h
# server/grpcbrokerservice.cpp

include_directories(common)

if (DEFINED BUILD_LIB_ONLY)

add_library(GrpcLib STATIC
    ${PROTO_GENERATED_SOURCES}
    ${PROTO_GENERATED_HEADERS}
    common/grpcconnectionmanager.cpp
    common/grpcconnectionmanager.h
    common/grpcworker.cpp
    common/grpcworker.h
    common/protobuf_forward.h
)

target_include_directories(GrpcLib PRIVATE ${GENERATED_DIR})

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
else()

add_executable(server
  server/main.cpp
  common/protobuf_forward.h
  common/grpcworker.h
  common/grpcworker.cpp
  ${PROTO_GENERATED_SOURCES}
  server/async_structs.h
  server/calldata.h
  server/server.h
  server/global_broker.h
  server/global_broker.cpp
  server/safe_logger.h
  server/server_stats.h
)

add_executable(client1
  client1/main.cpp
  common/grpcconnectionmanager.h
  common/grpcconnectionmanager.cpp
  common/grpcworker.h
  common/grpcworker.cpp
  common/protobuf_forward.h
  ${PROTO_GENERATED_SOURCES}
  common/proto/update.proto
)

add_executable(client2
  client2/main.cpp
  common/grpcconnectionmanager.h
  common/grpcconnectionmanager.cpp
  common/grpcworker.h
  common/grpcworker.cpp
  common/protobuf_forward.h
  ${PROTO_GENERATED_SOURCES}
  common/proto/update.proto
)

target_include_directories(client1 PRIVATE ${GENERATED_DIR})
target_include_directories(client2 PRIVATE ${GENERATED_DIR})
target_include_directories(server PRIVATE ${GENERATED_DIR})

target_link_libraries(server
    Qt${QT_VERSION_MAJOR}::Core
    protobuf::libprotobuf
    gRPC::grpc++
    gRPC::grpc
    ${PROTOBUF_LIBRARIES}
)
target_link_libraries(client1
    Qt${QT_VERSION_MAJOR}::Core
    protobuf::libprotobuf
    gRPC::grpc++
    gRPC::grpc
    ${PROTOBUF_LIBRARIES}
)
target_link_libraries(client2
    Qt${QT_VERSION_MAJOR}::Core
    protobuf::libprotobuf
    gRPC::grpc++
    gRPC::grpc
    ${PROTOBUF_LIBRARIES}
)

include(GNUInstallDirs)
install(TARGETS server client1 client2
    LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
    RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
)
endif()
