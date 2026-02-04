# TrainSets

protoc --cpp_out=. --grpc_out=. --plugin=protoc-gen-grpc=`which grpc_cpp_plugin` raft_node_rpc.proto

protoc --cpp_out=. --grpc_out=. --plugin=protoc-gen-grpc=`which grpc_cpp_plugin` application_rpc.proto

mkdir build && cd build && rm -rf * && cmake .. && make -j 4

./example_raft_server -n 3 -f ../conf/raft_node.conf -m ../conf/everysec.conf

./example_raft_client -f ../conf/raft_node.conf


lsof -t -i:27899,27900,27901

kill -9 $(sudo lsof -t -i:27899,27900,27901)

# 临时启用（当前会话有效）
ulimit -c unlimited

# 永久启用（添加到 ~/.bashrc）
echo "ulimit -c unlimited" >> ~/.bashrc
source ~/.bashrc

# 检查 core dump 设置
ulimit -c

# 配置 core 文件位置和命名（可选）
sudo sysctl -w kernel.core_pattern=/tmp/core/core-%e-%p-%t
# 或：sudo sysctl -w kernel.core_pattern=core.%e.%p.%t

# 使用 -g 标志编译（GCC/Clang）
g++ -g -O0 -o myprogram myprogram.cpp

# CMake 中启用调试
cmake -DCMAKE_BUILD_TYPE=Debug ..
# 或者在 CMakeLists.txt 中添加
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -g -O0")


# 基本用法
gdb ./example_raft_server core.12345

# 如果 core 文件不在当前目录
gdb ./example_raft_server /tmp/core-example_raft_server-12345-1700000000

# 或者指定 core 文件
gdb -c core.12345 ./example_raft_server






