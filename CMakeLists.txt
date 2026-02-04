cmake_minimum_required(VERSION 3.28)

set(Target TrainSets)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -g -O0")
project(${Target})

set(CMAKE_CXX_STANDARD 17)

set(EXECUTABLE_OUTPUT_PATH ${CMAKE_CURRENT_SOURCE_DIR}/bin)
set(LIBRARY_OUTPUT_PATH ${CMAKE_CURRENT_SOURCE_DIR}/lib)

include_directories(${CMAKE_CURRENT_SOURCE_DIR}/src/common/include)
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/src/fiber/include)
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/src/raft_cli/include)
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/src/raft_core/include)
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/src/raft_grpc/include)
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/src/state_machine/include)

include_directories(/usr/include)
link_directories(/usr/local/lib)
link_directories(${PROJECT_SOURCE_DIR}/lib)

add_subdirectory(src)
add_subdirectory(example)

find_package(Protobuf CONFIG REQUIRED)
find_package(gRPC CONFIG REQUIRED)
find_package(Boost REQUIRED COMPONENTS serialization)

include_directories(${GRPC_INCLUDE_DIRS} ${PROTOBUF_INCLUDE_DIRS})

message(STATUS "Using protobuf-${Protobuf_VERSION}")
message(STATUS "Using gRPC-${gRPC_VERSION}")
# 5. 检查 Boost 是否找到（可选，但推荐，增强鲁棒性）
if(Boost_FOUND)
    message(STATUS "Boost 找到，版本：${Boost_VERSION}")
    message(STATUS "Boost 头文件路径：${Boost_INCLUDE_DIRS}")
    message(STATUS "Boost 库文件路径：${Boost_LIBRARY_DIRS}")
else()
    message(FATAL_ERROR "未找到 Boost 库，请检查 Boost 安装或路径配置")
endif()


add_library(${Target} STATIC ${src_common} ${src_fiber} ${src_raft_core} ${src_raft_grpc} ${src_state_machine})

target_include_directories(${Target} PRIVATE ${Boost_INCLUDE_DIRS})

target_link_libraries(${Target} PUBLIC
        dl
        pthread
#        gRPC::grpc++_reflection
        protobuf::libprotobuf
        gRPC::grpc++
        ${Boost_LIBRARIES}
)


















