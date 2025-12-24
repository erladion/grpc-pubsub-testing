add_executable(client3-c
    main.c
    ${PROTO_GENERATED_C_SOURCES}
)

target_link_libraries(client3-c PRIVATE
    ${PROTOBUF_C_LIBRARIES}
    GrpcConnectionLib
)

target_link_directories(client3-c PRIVATE ${PROTOBUF_C_LIBRARY_DIRS})

target_include_directories(client3-c PRIVATE
    ${GENERATED_C_DIR}
    ${PROTOBUF_C_INCLUDE_DIRS}
    common
)
