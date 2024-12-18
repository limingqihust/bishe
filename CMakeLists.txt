cmake_minimum_required(VERSION 3.1)

set(CMAKE_CXX_STANDARD 14)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -g")
project(SimGridTemplateProject) # TODO: give a real name to your project here

set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_SOURCE_DIR}/cmake/Modules/")

find_package(SimGrid 3.27 REQUIRED) # This template requires SimGrid v3.27 because we use templated get()

include_directories(${SimGrid_INCLUDE_DIR})

add_executable(ping-pong ping-pong.cpp)
target_link_libraries(ping-pong ${SimGrid_LIBRARY})

add_executable(master-workers master-workers.cpp)
target_link_libraries(master-workers ${SimGrid_LIBRARY})
add_executable(Splitter "./tera_sort/Splitter.cpp" "./tera_sort/InputSplitter.cpp")

add_executable(my_master_workers 
    "./tera_sort/CodeGeneration.cpp" 
    "./tera_sort/PartitionSampling.cpp" 
    "./tera_sort/Trie.cpp" 
    "./tera_sort/Utility.cpp" 
    "./tera_sort/InputSplitter.cpp"
    "my_master_workers.cpp" 
    "master.cpp"
    "worker.cpp" 
    "tera_sort.cpp" 
    "coded_tera_sort.cpp"
    "online_learning.cpp"
    "bandwidth_config.cpp"
    "job_text.cpp")
target_link_libraries(my_master_workers ${SimGrid_LIBRARY})

add_executable(comm_demo comm_demo.cpp)
target_link_libraries(comm_demo ${SimGrid_LIBRARY})
