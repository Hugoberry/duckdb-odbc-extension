#pragma once

// This header ensures proper ODBC header inclusion across platforms

#ifdef _WIN32
    // Windows-specific includes
    #include <windows.h>

    // Ensure ODBCVER is defined before including ODBC headers
    #ifndef ODBCVER
        #define ODBCVER 0x0380 // We target ODBC 3.8
    #endif

    // Make sure SQLLEN is properly defined
    #ifdef _WIN64
        #ifndef SQLLEN
            typedef INT64 SQLLEN;
        #endif
        #ifndef SQLULEN
            typedef UINT64 SQLULEN;
        #endif
        #ifndef SQLSETPOSIROW
            typedef UINT64 SQLSETPOSIROW;
        #endif
    #else
        #ifndef SQLLEN
            typedef SQLINTEGER SQLLEN;
        #endif
        #ifndef SQLULEN
            typedef SQLUINTEGER SQLULEN;
        #endif
        #ifndef SQLSETPOSIROW
            typedef SQLUSMALLINT SQLSETPOSIROW;
        #endif
    #endif

    // Include ODBC headers in the correct order
    #include <sql.h>
    #include <sqlext.h>
    #include <sqltypes.h>
    #include <sqlucode.h>
#else
    // Unix/macOS includes
    
    // Ensure ODBCVER is defined before including ODBC headers
    #ifndef ODBCVER
        #define ODBCVER 0x0380 // We target ODBC 3.8
    #endif
    
    // On some Unix platforms, we need to define these ourselves
    #ifndef SQLLEN
        #include <stdint.h>
        typedef int64_t SQLLEN;
        typedef uint64_t SQLULEN;
    #endif
    
    #include <sql.h>
    #include <sqlext.h>
    #include <sqltypes.h>
    #include <sqlucode.h>
#endif

// Define additional ODBC constants that might be missing on some platforms
#ifndef SQL_MAX_MESSAGE_LENGTH
    #define SQL_MAX_MESSAGE_LENGTH 512
#endif

// Define SQL_BOOLEAN if not available (some older ODBC drivers may not define it)
#ifndef SQL_BOOLEAN
    #define SQL_BOOLEAN 16
#endif

// Snowflake-specific type defines
#ifndef SQL_TIMESTAMP_WITH_TIMEZONE
    #define SQL_TIMESTAMP_WITH_TIMEZONE 95
#endif

#ifndef SQL_BIT_VARYING
    #define SQL_BIT_VARYING (-3)
#endif

namespace duckdb {
    // Helper function to safely check if a SQLGetData call needs to continue
    inline bool NeedMoreData(SQLRETURN ret, SQLLEN indicator, size_t buffer_size) {
        return (ret == SQL_SUCCESS_WITH_INFO) && 
               (indicator != SQL_NULL_DATA) && 
               (indicator > buffer_size || indicator == SQL_NO_TOTAL);
    }
}