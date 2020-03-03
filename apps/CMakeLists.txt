include_directories(${Boost_INCLUDE_DIRS})
add_executable(sample sample.cpp)
add_dependencies(sample boost)
target_link_libraries(sample ${Boost_LIBRARIES})