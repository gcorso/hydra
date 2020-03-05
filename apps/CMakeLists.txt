add_executable(sample sample.cpp)
target_link_libraries(sample worker pthread pq boost_program_options)
target_include_directories(sample PRIVATE /usr/include/postgres)

add_executable(hydra hydra.cc)
target_link_libraries(hydra worker)