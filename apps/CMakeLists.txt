add_executable(sample sample.cpp)
target_link_libraries(sample log db)
target_include_directories(sample PRIVATE /usr/include/postgres)

add_executable(caput caput.cc)
target_link_libraries(caput worker)