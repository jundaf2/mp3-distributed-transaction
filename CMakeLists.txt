cmake_minimum_required(VERSION 2.8)
project(mp3_Distributed_transactions)

macro(use_cxx11)
    if (CMAKE_VERSION VERSION_LESS "3.1")
        if (CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
            set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=gnu++1y")
        endif ()
    else ()
        set (CMAKE_CXX_STANDARD 14)
    endif ()
endmacro(use_cxx11)
set(VERBOSE_BUILD ON)
use_cxx11()
add_executable(client client.cpp message_base.h)
add_executable(server server.cpp message_base.h)

find_package(Threads REQUIRED)
if(THREADS_HAVE_PTHREAD_ARG)
    target_compile_options(client PUBLIC "-pthread")
endif()
if(CMAKE_THREAD_LIBS_INIT)
    target_link_libraries(client "${CMAKE_THREAD_LIBS_INIT}")
    target_link_libraries(server "${CMAKE_THREAD_LIBS_INIT}")
endif()





