#pragma once

// This header ensures proper nanodbc and ODBC header inclusion across platforms

#ifdef _WIN32
    // Windows-specific includes
    #include <windows.h>

    // Ensure ODBCVER is defined before including ODBC headers
    #ifndef ODBCVER
        #define ODBCVER 0x0380
    #endif
#endif

// Include nanodbc
#include <nanodbc/nanodbc.h>

// We still include the raw ODBC headers for some type definitions and constants
#include <sql.h>
#include <sqlext.h>
#include <sqltypes.h>
#include <sqlucode.h>