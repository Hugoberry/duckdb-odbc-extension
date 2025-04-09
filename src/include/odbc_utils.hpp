#pragma once

#include "duckdb.hpp"
#include "odbc_headers.hpp"
#include <string>

namespace duckdb {

class ODBCUtils {
public:
    // Check ODBC error and throw exception if necessary
    static void Check(SQLRETURN rc, SQLSMALLINT handle_type, SQLHANDLE handle, const std::string &operation);
    
    // Get error message from ODBC
    static std::string GetErrorMessage(SQLSMALLINT handle_type, SQLHANDLE handle);
    
    // Convert ODBC type to string representation
    static std::string TypeToString(SQLSMALLINT odbc_type);
    
    // Sanitize string for ODBC usage
    static std::string SanitizeString(const std::string &input);
    
    // Convert DuckDB type to ODBC type
    static SQLSMALLINT ToODBCType(const LogicalType &input);
    
    // Convert ODBC type to DuckDB LogicalType with improved handling
    static LogicalType TypeToLogicalType(SQLSMALLINT odbc_type, SQLULEN column_size, SQLSMALLINT decimal_digits);
    
    // Check if a column contains timezone information
    static bool IsTimestampWithTimezone(SQLHSTMT stmt, SQLUSMALLINT column_index);
};

} // namespace duckdb