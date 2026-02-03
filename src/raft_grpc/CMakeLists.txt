
file(GLOB src_list "${CMAKE_CURRENT_SOURCE_DIR}/proto/*.cc")

set(src_raft_grpc ${src_list} CACHE INTERNAL "Description of the variable")