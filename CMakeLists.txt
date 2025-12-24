cmake_minimum_required(VERSION 3.14)

project(grpc LANGUAGES CXX C)

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
find_package(PkgConfig REQUIRED)

pkg_check_modules(PROTOBUF_C REQUIRED libprotobuf-c)
message(STATUS "Protobuf-C Libs: ${PROTOBUF_C_LIBRARIES}")
message(STATUS "Protobuf-C Path: ${PROTOBUF_C_LIBRARY_DIRS}")

set(PROTO_SRC broker.proto)
set(PROTO_DIR ${CMAKE_CURRENT_SOURCE_DIR}/common)
set(GENERATED_DIR ${CMAKE_CURRENT_SOURCE_DIR}/common/generated)
set(GENERATED_C_DIR ${CMAKE_CURRENT_SOURCE_DIR}/common/generated-c)

file(MAKE_DIRECTORY ${GENERATED_DIR})
file(MAKE_DIRECTORY ${GENERATED_C_DIR})

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

set(PROTO_GENERATED_C_SOURCES
    ${GENERATED_C_DIR}/broker.pb-c.c
    ${GENERATED_C_DIR}/update.pb-c.c
    ${GENERATED_C_DIR}/google/protobuf/any.pb-c.c
)

set(PROTO_GENERATED_C_HEADERS
    ${GENERATED_C_DIR}/broker.pb-c.h
    ${GENERATED_C_DIR}/update.pb-c.h
    ${GENERATED_C_DIR}/google/protobuf/any.pb-c.h
)

include_directories(common)
add_subdirectory(common)

if (NOT DEFINED BUILD_LIB_ONLY)
    add_subdirectory(server)
    add_subdirectory(client1)
    add_subdirectory(client2)
    add_subdirectory(client3-c)
    #add_subdirectory(monitor)

    include(GNUInstallDirs)
    install(TARGETS server client1 client2
        LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
        RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
    )
endif()
