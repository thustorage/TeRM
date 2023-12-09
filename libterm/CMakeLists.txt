cmake_minimum_required(VERSION 3.10)
project(nic-odp)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
add_compile_definitions(RECORDER_VERBOSE=0 RECORDER_SINGLE_THREAD=0)

option (SANITIZE "Turn on sanitization" OFF)
if (SANITIZE)
    add_compile_options(-fsanitize=address)
endif()

# include
include_directories(${PROJECT_SOURCE_DIR}/3rd-party/include)
include_directories(${PROJECT_SOURCE_DIR}/include)

# verbs-pdp
file(GLOB_RECURSE SRCS ${PROJECT_SOURCE_DIR}/ibverbs-pdp/*.cc)
add_library(pdp SHARED ${SRCS})
find_package(fmt)
target_link_libraries(pdp fmt::fmt-header-only ibverbs memcached uring glog boost_coroutine aio)

# test
file(GLOB SRCS ${PROJECT_SOURCE_DIR}/test/*.cc)
foreach(SRC ${SRCS})
    get_filename_component(SRC_NAME ${SRC} NAME_WE)
    add_executable(${SRC_NAME} ${SRC})
    target_link_libraries(${SRC_NAME} fmt::fmt-header-only ibverbs gflags glog memcached)
endforeach()
