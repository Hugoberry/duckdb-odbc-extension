#pragma once

#include "duckdb.hpp"
#include "duckdb/common/string_util.hpp"
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
    
    // Convert ODBC type to DuckDB LogicalType
    static LogicalType TypeToLogicalType(SQLSMALLINT odbc_type, SQLULEN column_size, SQLSMALLINT decimal_digits);

    // Method for reading variable-length data
    static bool ReadVarColumn(SQLHSTMT hstmt, SQLUSMALLINT col_idx, SQLSMALLINT ctype, 
        bool& isNull, std::vector<char>& result);

    // Helper to determine if a type is binary
    static bool IsBinaryType(SQLSMALLINT sqltype);

    // Helper to determine if a type is wide (Unicode)
    static bool IsWideType(SQLSMALLINT sqltype);
};

} // namespace duckdb