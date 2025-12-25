find_package(QT NAMES Qt6 Qt5 REQUIRED COMPONENTS Core)
find_package(Qt${QT_VERSION_MAJOR} REQUIRED COMPONENTS Core)

add_executable(client2 main.cpp)

target_include_directories(client2 PRIVATE ${GENERATED_DIR})

target_link_libraries(client2 PRIVATE
    GrpcLib
    Qt${QT_VERSION_MAJOR}::Core
)
