#pragma once

#include "duckdb.hpp"
#include "odbc_headers.hpp"
#include <string>
#include <unordered_map>

namespace duckdb {

class OdbcUtils {
public:
    // Format error message and throw a DuckDB exception
    static void ThrowException(const std::string& operation, const nanodbc::database_error& e);
    
    // Sanitize string for ODBC usage (escape quotes)
    static std::string SanitizeString(const std::string& input);
    
    // Type conversion lookups
    static LogicalType OdbcTypeToLogicalType(SQLSMALLINT odbcType, SQLULEN columnSize, SQLSMALLINT decimalDigits);

    static bool IsVarcharType(SQLSMALLINT sqlType);
#ifdef _WIN32
    static std::string ConvertToUTF8(const std::string& input, int codepage);
#endif
private:
    // Lookup tables for type conversion
    static const std::unordered_map<SQLSMALLINT, LogicalTypeId> ODBC_TO_DUCKDB_TYPES;
    static const std::unordered_map<SQLSMALLINT, std::string> TYPE_NAMES;
};

} // namespace duckdb