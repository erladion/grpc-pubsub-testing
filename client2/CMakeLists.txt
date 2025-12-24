add_executable(client2 main.cpp)

target_include_directories(client2 PRIVATE ${GENERATED_DIR})

target_link_libraries(client2 PRIVATE GrpcLib)
