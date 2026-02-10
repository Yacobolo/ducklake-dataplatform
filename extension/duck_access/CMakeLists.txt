cmake_minimum_required(VERSION 3.14)

set(TARGET_NAME duck_access)
set(EXTENSION_NAME ${TARGET_NAME}_extension)
set(LOADABLE_EXTENSION_NAME ${TARGET_NAME}_loadable_extension)

project(${TARGET_NAME})

include_directories(src/include src)

set(EXTENSION_SOURCES
    src/duck_access_extension.cpp
    src/duck_access_secret.cpp
    src/duck_access_scan.cpp
    src/duck_access_http.cpp
    src/duck_access_manifest.cpp
)

build_static_extension(${TARGET_NAME} ${EXTENSION_SOURCES})
build_loadable_extension(${TARGET_NAME} "" ${EXTENSION_SOURCES})

# cpp-httplib requires OpenSSL for HTTPS support
find_package(OpenSSL REQUIRED)
target_link_libraries(${EXTENSION_NAME} OpenSSL::SSL OpenSSL::Crypto)
target_link_libraries(${LOADABLE_EXTENSION_NAME} OpenSSL::SSL OpenSSL::Crypto)

# Define CPPHTTPLIB_OPENSSL_SUPPORT for HTTPS
target_compile_definitions(${EXTENSION_NAME} PRIVATE CPPHTTPLIB_OPENSSL_SUPPORT)
target_compile_definitions(${LOADABLE_EXTENSION_NAME} PRIVATE CPPHTTPLIB_OPENSSL_SUPPORT)

# Register the static extension in DuckDB's export set (required for CMake install/export)
install(
    TARGETS ${EXTENSION_NAME}
    EXPORT "${DUCKDB_EXPORT_SET}"
    LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
    ARCHIVE DESTINATION "${INSTALL_LIB_DIR}"
)
