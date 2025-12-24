add_executable(server
  main.cpp
  calldata.h
  server.h
  global_broker.h
  global_broker.cpp
  safe_logger.h
  server_stats.h
  calldata.cpp
)

target_include_directories(server PRIVATE ${GENERATED_DIR})

target_link_libraries(server
    GrpcLib
)

