cmake_minimum_required(VERSION 3.28)

set(Target TrainDis)

project(${Target})

set(CMAKE_CXX_STANDARD 17)

set(EXECUTABLE_OUTPUT_PATH ${CMAKE_CURRENT_SOURCE_DIR}/bin)
set(LIBRARY_OUTPUT_PATH ${CMAKE_CURRENT_SOURCE_DIR}/lib)

include_directories(${CMAKE_CURRENT_SOURCE_DIR}/src/common/include)
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/src/fiber/include)
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/src/raft_cli/include)
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/src/raft_core/include)
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/src/raft_net/include)
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/src/state_machine/include)

include_directories(/usr/include)
link_directories(/usr/local/lib)
link_directories(${PROJECT_SOURCE_DIR}/lib)

add_subdirectory(src)
add_subdirectory(example)

add_library(${Target} STATIC ${src_fiber} ${src_raft_core} ${src_raft_net} ${src_state_machine})

target_link_libraries(${Target} pthread ld)




















