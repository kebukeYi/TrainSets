
# 寻找 Abseil 库
#find_package(absl REQUIRED)
find_package(Protobuf CONFIG REQUIRED)
find_package(gRPC CONFIG REQUIRED)


message(STATUS "Using protobuf-${Protobuf_VERSION}")
message(STATUS "Using gRPC-${gRPC_VERSION}")

include_directories(${GRPC_INCLUDE_DIRS} ${PROTOBUF_INCLUDE_DIRS})

set(client client.cpp)

# 头文件, 声明
#add_executable(example_raft_client ${src_common} ${src_raft_grpc} ${src_raft_cli} ${client})
add_executable(example_raft_client ${client} ${src_raft_cli} )
# 库里面的实际定义
#target_link_libraries(example_raft_client PUBLIC TrainSets gRPC::grpc++ protobuf::libprotobuf)
target_link_libraries(example_raft_client PUBLIC TrainSets)

set(raftServer raft_server.cpp)
add_executable(example_raft_server ${raftServer})
#target_link_libraries(example_raft_server PUBLIC TrainSets pthread gRPC::grpc++ protobuf::libprotobuf)
target_link_libraries(example_raft_server PUBLIC TrainSets)
