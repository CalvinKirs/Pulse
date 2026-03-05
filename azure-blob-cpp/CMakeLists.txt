cmake_minimum_required(VERSION 3.16)
project(azure_blob_connectivity_client LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

find_package(azure-storage-blobs-cpp CONFIG REQUIRED)

add_executable(azure_blob_connectivity
    src/main.cpp
)

target_link_libraries(azure_blob_connectivity PRIVATE
    Azure::azure-storage-blobs
)
