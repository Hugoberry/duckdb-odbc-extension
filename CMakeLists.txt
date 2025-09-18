cmake_minimum_required(VERSION 3.5)
set(CMAKE_CXX_STANDARD 17)

# Set extension name here
set(TARGET_NAME nanodbc)
set(EXTENSION_NAME ${TARGET_NAME}_extension)
set(LOADABLE_EXTENSION_NAME ${TARGET_NAME}_loadable_extension)

set(NANODBC_ENABLE_UNICODE ON CACHE BOOL "Enable Unicode support in nanodbc")

project(${TARGET_NAME})

# Include directories
include_directories(src/include)
include_directories(src)

# Set global visibility settings to ensure symbols are exported correctly
set(CMAKE_CXX_VISIBILITY_PRESET default)
set(CMAKE_VISIBILITY_INLINES_HIDDEN OFF)

# Platform-specific settings
if(APPLE)
    # Support for Apple Silicon (ARM64)
    set(CMAKE_OSX_ARCHITECTURES "arm64" CACHE STRING "Build architectures for macOS" FORCE)

    # Find iconv on macOS (it's part of the system but needs explicit linking)
    find_library(ICONV_LIBRARY NAMES iconv libiconv REQUIRED)
    if(ICONV_LIBRARY)
        message(STATUS "Found iconv library: ${ICONV_LIBRARY}")
    else()
        # On macOS, sometimes we need to add special handling for iconv
        set(ICONV_LIBRARY "iconv")
        message(STATUS "Using default iconv library")
    endif()
    
    # Set RPATH for macOS to make sure libraries are found at runtime
    set(CMAKE_INSTALL_RPATH "@loader_path")
    set(CMAKE_BUILD_WITH_INSTALL_RPATH TRUE)
    set(CMAKE_MACOSX_RPATH ON)
elseif(UNIX AND NOT APPLE)
    # On Linux, include iconv if available
    find_library(ICONV_LIBRARY NAMES iconv)
    if(ICONV_LIBRARY)
        message(STATUS "Found iconv library: ${ICONV_LIBRARY}")
    endif()
endif()

# Platform-specific handling for ODBC
if(WIN32)
    # Windows ODBC is included in the SDK
    set(ODBC_LIBRARIES odbc32)
    set(ODBC_FOUND TRUE)
    set(ODBC_INCLUDE_DIRS "")
    message(STATUS "Using Windows SDK ODBC libraries")
else()
    # For macOS and Linux, use vcpkg-provided unixODBC
    find_package(ODBC REQUIRED)
    
    if(NOT ODBC_FOUND)
        message(FATAL_ERROR "ODBC not found. Please ensure your vcpkg.json includes unixodbc as a dependency.")
    endif()
    
    message(STATUS "Found ODBC libraries: ${ODBC_LIBRARIES}")
    message(STATUS "Found ODBC includes: ${ODBC_INCLUDE_DIRS}")

    # Add iconv to ODBC libraries on non-Windows platforms
    if(ICONV_LIBRARY)
        list(APPEND ODBC_LIBRARIES ${ICONV_LIBRARY})
        message(STATUS "Added iconv to ODBC libraries: ${ODBC_LIBRARIES}")
    endif()
    
    # On Linux, make sure we're using the right linking flags
    if(UNIX AND NOT APPLE)
        set(CMAKE_SHARED_LINKER_FLAGS "${CMAKE_SHARED_LINKER_FLAGS} -Wl,--no-as-needed")
    endif()
endif()

# Include ODBC headers
include_directories(${ODBC_INCLUDE_DIRS})

# Nanodbc library files
set(NANODBC_SOURCES
    src/nanodbc/nanodbc.cpp
)

# Extension sources
set(EXTENSION_SOURCES 
    src/odbc_connection.cpp
    src/odbc_statement.cpp
    src/odbc_utils.cpp
    src/odbc_scanner.cpp
    src/nanodbc_extension.cpp
    src/odbc_parameters.cpp
    src/odbc_encoding.cpp
    src/odbc_storage.cpp
    src/storage/odbc_catalog.cpp
    src/storage/odbc_transaction_manager.cpp
    src/storage/odbc_transaction.cpp
    src/storage/odbc_schema_entry.cpp
    src/storage/odbc_table_entry.cpp
)

# Combined sources
set(ALL_SOURCES ${EXTENSION_SOURCES} ${NANODBC_SOURCES})

# Create the static extension
add_library(${EXTENSION_NAME} STATIC ${ALL_SOURCES})
target_link_libraries(${EXTENSION_NAME} PRIVATE ${ODBC_LIBRARIES})

# Build loadable extension with ODBC parameter
set(PARAMETERS "-l${ODBC_LIBRARIES}")
build_loadable_extension(${TARGET_NAME} "${PARAMETERS}" ${ALL_SOURCES})

# Install the extension
install(
  TARGETS ${EXTENSION_NAME}
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")