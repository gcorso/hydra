cmake_minimum_required(VERSION 3.12)
project(hydra)


set(CMAKE_CXX_STANDARD 17)

if (EXISTS /usr/include/libpq-fe.h)
    add_definitions(-DFEDORA)
endif ()

add_subdirectory(libs)
#add_subdirectory(tests)
add_subdirectory(apps)
