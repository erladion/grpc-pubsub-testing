add_executable(client1 main.cpp)


target_include_directories(client1 PRIVATE ${GENERATED_DIR})

target_link_libraries(client1 PRIVATE GrpcLib)
