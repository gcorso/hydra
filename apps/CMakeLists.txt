add_executable(sample sample.cpp)
target_link_libraries(sample worker pthread pq pqxx boost_program_options)

add_executable(hydra hydra.cc)
target_link_libraries(hydra worker)