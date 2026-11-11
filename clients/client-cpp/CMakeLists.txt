# Queen MQ C++ client: one header, as a CMake target.
#
#   add_subdirectory(path/to/queen/clients/client-cpp)      # or FetchContent, below
#   target_link_libraries(app PRIVATE queen::client)
#
#   include(FetchContent)
#   FetchContent_Declare(queen
#       GIT_REPOSITORY https://github.com/queen-mq/queen.git
#       GIT_TAG        <release tag>
#       SOURCE_SUBDIR  clients/client-cpp)
#   FetchContent_MakeAvailable(queen)
#
# Dependencies: cpp-httplib 0.27+ (found, or fetched when QUEEN_CLIENT_FETCH_HTTPLIB),
# OpenSSL for https:// (QUEEN_CLIENT_TLS), zstd for push_compression and
# framed_pop (QUEEN_CLIENT_ZSTD). nlohmann/json is the program's own when
# find_package finds one, else the copy in third_party/.
cmake_minimum_required(VERSION 3.16)
project(queen_client VERSION 2.8.0 LANGUAGES CXX)

if(CMAKE_SOURCE_DIR STREQUAL CMAKE_CURRENT_SOURCE_DIR)
    set(QUEEN_CLIENT_TOP_LEVEL ON)
else()
    set(QUEEN_CLIENT_TOP_LEVEL OFF)
endif()

option(QUEEN_CLIENT_TLS "https:// support (links OpenSSL)" ON)
option(QUEEN_CLIENT_ZSTD "zstd, for the push_compression and framed_pop options" OFF)
option(QUEEN_CLIENT_FETCH_HTTPLIB "Fetch cpp-httplib v0.27.0 when it is not installed" ON)
option(QUEEN_CLIENT_BUILD_TESTS "Build the broker-free test suites (ctest)" ${QUEEN_CLIENT_TOP_LEVEL})

add_library(queen_client INTERFACE)
add_library(queen::client ALIAS queen_client)
target_compile_features(queen_client INTERFACE cxx_std_17)
target_include_directories(queen_client INTERFACE ${CMAKE_CURRENT_SOURCE_DIR})

find_package(Threads REQUIRED)
target_link_libraries(queen_client INTERFACE Threads::Threads)

# --- TLS -------------------------------------------------------------------
if(QUEEN_CLIENT_TLS)
    find_package(OpenSSL REQUIRED)
    target_link_libraries(queen_client INTERFACE OpenSSL::SSL OpenSSL::Crypto)
else()
    target_compile_definitions(queen_client INTERFACE QUEEN_CLIENT_NO_TLS)
endif()

# --- cpp-httplib -----------------------------------------------------------
find_package(httplib 0.27 CONFIG QUIET)
if(TARGET httplib::httplib)
    target_link_libraries(queen_client INTERFACE httplib::httplib)
    if(APPLE)
        # An installed httplib 0.27 is built with its non-blocking resolver,
        # which calls CFHost (CFNetwork), and its exported target forgets to
        # link that framework.
        target_link_libraries(queen_client INTERFACE "-framework CFNetwork" "-framework CoreFoundation")
    endif()
else()
    find_path(QUEEN_HTTPLIB_INCLUDE_DIR httplib.h)
    if(QUEEN_HTTPLIB_INCLUDE_DIR)
        target_include_directories(queen_client INTERFACE ${QUEEN_HTTPLIB_INCLUDE_DIR})
    elseif(QUEEN_CLIENT_FETCH_HTTPLIB)
        include(FetchContent)
        FetchContent_Declare(httplib
            URL https://github.com/yhirose/cpp-httplib/archive/refs/tags/v0.27.0.tar.gz)
        set(HTTPLIB_REQUIRE_OPENSSL ${QUEEN_CLIENT_TLS} CACHE BOOL "" FORCE)
        set(HTTPLIB_USE_OPENSSL_IF_AVAILABLE ${QUEEN_CLIENT_TLS} CACHE BOOL "" FORCE)
        FetchContent_MakeAvailable(httplib)
        target_link_libraries(queen_client INTERFACE httplib::httplib)
    else()
        message(FATAL_ERROR "queen_client: cpp-httplib 0.27+ not found; install it or set QUEEN_CLIENT_FETCH_HTTPLIB=ON")
    endif()
endif()

# --- nlohmann/json: the program's own, or the vendored copy ----------------
find_package(nlohmann_json 3.11 CONFIG QUIET)
if(TARGET nlohmann_json::nlohmann_json)
    target_link_libraries(queen_client INTERFACE nlohmann_json::nlohmann_json)
endif()

# --- zstd ------------------------------------------------------------------
if(QUEEN_CLIENT_ZSTD)
    find_package(zstd CONFIG QUIET)
    if(TARGET zstd::libzstd_shared)
        target_link_libraries(queen_client INTERFACE zstd::libzstd_shared)
    elseif(TARGET zstd::libzstd_static)
        target_link_libraries(queen_client INTERFACE zstd::libzstd_static)
    else()
        find_path(QUEEN_ZSTD_INCLUDE_DIR zstd.h REQUIRED)
        find_library(QUEEN_ZSTD_LIBRARY zstd REQUIRED)
        target_include_directories(queen_client INTERFACE ${QUEEN_ZSTD_INCLUDE_DIR})
        target_link_libraries(queen_client INTERFACE ${QUEEN_ZSTD_LIBRARY})
    endif()
    target_compile_definitions(queen_client INTERFACE QUEEN_CLIENT_ZSTD)
endif()

# --- tests and the example -------------------------------------------------
if(QUEEN_CLIENT_BUILD_TESTS)
    enable_testing()
    # The suites that need no broker: each serves its own answers in-process.
    set(QUEEN_CLIENT_SUITES
        test_retry429 test_kv_timers test_locks test_conflation test_buffer
        test_ephemeral test_autopilot test_consumer test_wire_forms test_reads
        test_tokens)
    foreach(suite IN LISTS QUEEN_CLIENT_SUITES)
        add_executable(${suite} ${suite}.cpp)
        target_link_libraries(${suite} PRIVATE queen::client)
        add_test(NAME ${suite} COMMAND ${suite})
    endforeach()
    # The live suite: ./test_client <broker-url>
    add_executable(test_client test_client.cpp)
    target_link_libraries(test_client PRIVATE queen::client)
    add_executable(example_basic example_basic.cpp)
    target_link_libraries(example_basic PRIVATE queen::client)
endif()
