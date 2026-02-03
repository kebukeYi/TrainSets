
# 获取当前源文件所在目录的绝对路径
get_filename_component(SRC_DIR "${CMAKE_CURRENT_SOURCE_DIR}" ABSOLUTE)

# 将该目录下所有源文件添加到 SRC_LIST 变量中
aux_source_directory(${SRC_DIR} SRC_LIST)

# 生成静态库
#add_library(fiber_lib ${SRC_LIST})
# 静态库 链接 静态库
#target_link_libraries(fiber_lib -ldl)

set(src_fiber ${SRC_LIST} CACHE INTERNAL "Description of the variable")