#pragma once

#include "duckdb.hpp"
#include "duckdb/common/string_util.hpp"
#include "odbc_headers.hpp"
#include <string>

namespace duckdb {

class OdbcUtils {
public:
    // Handle nanodbc exceptions and convert to error message
    static std::string HandleException(const nanodbc::database_error& e);
    
    // Convert ODBC type to string representation
    static std::string TypeToString(SQLSMALLINT odbc_type);
    
    // Sanitize string for ODBC usage
    static std::string SanitizeString(const std::string& input);
    
    // Convert DuckDB type to ODBC type
    static SQLSMALLINT ToODBCType(const LogicalType& input);
    
    // Convert ODBC type to DuckDB LogicalType
    static LogicalType TypeToLogicalType(SQLSMALLINT odbc_type, SQLULEN column_size, SQLSMALLINT decimal_digits);

    // Method for reading variable-length data from nanodbc result
    static bool ReadVarColumn(nanodbc::result& result, idx_t col_idx, bool& isNull, std::vector<char>& output);

    // Helper to determine if a type is binary
    static bool IsBinaryType(SQLSMALLINT sqltype);

    // Helper to determine if a type is wide (Unicode)
    static bool IsWideType(SQLSMALLINT sqltype);
    
    // Get nanodbc-compatible type for binding parameters
    static int GetOdbcType(const LogicalType& type);
    
    // Get metadata from nanodbc result
    static void GetColumnMetadata(nanodbc::result& result, idx_t col_idx, 
                                SQLSMALLINT& type, SQLULEN& column_size, SQLSMALLINT& decimal_digits);
};

// Define an alias for backward compatibility
using ODBCUtils = OdbcUtils;

} // namespace duckdb