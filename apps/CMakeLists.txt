add_executable(sample sample.cpp)
target_link_libraries(sample pthread pq pqxx boost_program_options)