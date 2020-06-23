project(util)

add_library(${PROJECT_NAME} STATIC util.h executor.cc)

target_link_libraries(${PROJECT_NAME}
        PRIVATE
        log
        )

add_library(libs::${PROJECT_NAME} ALIAS ${PROJECT_NAME})

target_include_directories(${PROJECT_NAME}
        PUBLIC
        $<INSTALL_INTERFACE:include>
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/..>
        /usr/include/postgres
        PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}
        )


target_compile_features(${PROJECT_NAME}
        PRIVATE
        cxx_std_11
        )