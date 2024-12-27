# ENABLE_IF_SUPPORTED(CMAKE_CXX_FLAGS "-Wall")

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_BUILD_TYPE debug)
ENABLE_IF_SUPPORTED(CMAKE_CXX_FLAGS "-std=c++20")
ENABLE_IF_SUPPORTED(CMAKE_CXX_FLAGS "-pthread")

# Find the packages we need.
find_package(Boost COMPONENTS system filesystem REQUIRED)
find_package(cpprestsdk REQUIRED)
find_package(spdlog REQUIRED)

# Add the list of include paths to be used to search for include files
include_directories(${CMAKE_CURRENT_SOURCE_DIR})
include_directories(${BOOST_INCLUDE_DIR})
include_directories(${YAML_CPP_INCLUDE_DIR}) # See yaml-cpp-config.cmake file created in the build or install dir
include_directories(/usr/local/include)

# Search all the .cpp files in the directory where CMakeLists lies and set them to ${SOURCE_FILES}
file(GLOB_RECURSE SOURCE_FILES ${CMAKE_CURRENT_SOURCE_DIR}/*.cpp) 

# Search all the .h files in the directory where CMakeLists lies and set them to ${INCLUDE_FILES}
file(GLOB_RECURSE INCLUDE_FILES ${CMAKE_CURRENT_SOURCE_DIR}/*.h)

# Add the executable Example to be built from the source files
add_executable(${PROJECT_NAME} ${SOURCE_FILES} ${INCLUDE_FILES})

if(APPLE)
    target_link_libraries(${PROJECT_NAME} LINK_PUBLIC ${Boost_LIBRARIES})
    target_link_libraries(${PROJECT_NAME} LINK_PUBLIC ${YAML_CPP_LIBRARIES})

    find_library(YAML_LIB NAMES libyaml-cpp.a PATHS /opt/homebrew/lib/ /usr/local/lib/)
    target_link_libraries(${PROJECT_NAME} LINK_PUBLIC ${YAML_LIB})

    find_library(KAFKA_LIB NAMES rdkafka++ PATHS /usr/local/lib/)
    target_link_libraries(${PROJECT_NAME} LINK_PUBLIC ${KAFKA_LIB})

    find_library(AVRO_LIB NAMES avrocpp PATHS /usr/local/lib/)
    target_link_libraries(${PROJECT_NAME} LINK_PUBLIC ${AVRO_LIB})

    find_library(CRYPTO_LIB NAMES crypto PATHS /opt/homebrew/lib/ /usr/local/lib/)
    target_link_libraries(${PROJECT_NAME} LINK_PUBLIC cpprestsdk::cpprest)
    target_link_libraries(${PROJECT_NAME} LINK_PUBLIC ${CRYPTO_LIB}) # Mac OS - Required for cpprest

    target_link_libraries(${PROJECT_NAME} LINK_PUBLIC spdlog::spdlog) 

    # Serdes
    find_library(SERDES_CPP_LIB NAMES libserdes++.a PATHS /opt/homebrew/lib/ /usr/local/lib/)
    find_library(SERDES_LIB NAMES libserdes.a PATHS /opt/homebrew/lib/ /usr/local/lib/)

    target_link_libraries(${PROJECT_NAME} LINK_PRIVATE ${SERDES_CPP_LIB})
    target_link_libraries(${PROJECT_NAME} LINK_PRIVATE ${SERDES_LIB})
    target_link_libraries(${PROJECT_NAME} LINK_PRIVATE curl)

    find_library(JANSSON_LIB NAMES jansson PATHS /opt/homebrew/lib/)
    target_link_libraries(${PROJECT_NAME} LINK_PRIVATE ${JANSSON_LIB})

    find_library(AVRO_C_LIB NAMES avro PATHS /usr/local/lib/)
    target_link_libraries(${PROJECT_NAME} LINK_PRIVATE ${AVRO_C_LIB})

    find_library(PQXX_LIB pqxx)
    TARGET_LINK_LIBRARIES(${PROJECT_NAME} LINK_PRIVATE ${PQXX_LIB})
endif()

if(UNIX AND NOT APPLE)
endif()