#include "odbc_utils.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/exception.hpp"

namespace duckdb {

// Type conversion lookup tables
const std::unordered_map<SQLSMALLINT, LogicalTypeId> OdbcUtils::ODBC_TO_DUCKDB_TYPES = {
    {SQL_BIT, LogicalTypeId::BOOLEAN},
    {SQL_TINYINT, LogicalTypeId::TINYINT},
    {SQL_SMALLINT, LogicalTypeId::SMALLINT},
    {SQL_INTEGER, LogicalTypeId::INTEGER},
    {SQL_BIGINT, LogicalTypeId::BIGINT},
    {SQL_REAL, LogicalTypeId::FLOAT},
    {SQL_FLOAT, LogicalTypeId::FLOAT},
    {SQL_DOUBLE, LogicalTypeId::DOUBLE},
    {SQL_DECIMAL, LogicalTypeId::DECIMAL},
    {SQL_NUMERIC, LogicalTypeId::DECIMAL},
    {SQL_CHAR, LogicalTypeId::VARCHAR},
    {SQL_VARCHAR, LogicalTypeId::VARCHAR},
    {SQL_LONGVARCHAR, LogicalTypeId::VARCHAR},
    {SQL_WCHAR, LogicalTypeId::VARCHAR},
    {SQL_WVARCHAR, LogicalTypeId::VARCHAR},
    {SQL_WLONGVARCHAR, LogicalTypeId::VARCHAR},
    {SQL_BINARY, LogicalTypeId::BLOB},
    {SQL_VARBINARY, LogicalTypeId::BLOB},
    {SQL_LONGVARBINARY, LogicalTypeId::BLOB},
    {SQL_DATE, LogicalTypeId::DATE},
    {SQL_TYPE_DATE, LogicalTypeId::DATE},
    {SQL_TIME, LogicalTypeId::TIME},
    {SQL_TYPE_TIME, LogicalTypeId::TIME},
    {SQL_TIMESTAMP, LogicalTypeId::TIMESTAMP},
    {SQL_TYPE_TIMESTAMP, LogicalTypeId::TIMESTAMP},
    {SQL_GUID, LogicalTypeId::UUID}
};

const std::unordered_map<SQLSMALLINT, std::string> OdbcUtils::TYPE_NAMES = {
    {SQL_CHAR, "CHAR"},
    {SQL_VARCHAR, "VARCHAR"},
    {SQL_LONGVARCHAR, "LONGVARCHAR"},
    {SQL_WCHAR, "WCHAR"},
    {SQL_WVARCHAR, "WVARCHAR"},
    {SQL_WLONGVARCHAR, "WLONGVARCHAR"},
    {SQL_DECIMAL, "DECIMAL"},
    {SQL_NUMERIC, "NUMERIC"},
    {SQL_SMALLINT, "SMALLINT"},
    {SQL_INTEGER, "INTEGER"},
    {SQL_REAL, "REAL"},
    {SQL_FLOAT, "FLOAT"},
    {SQL_DOUBLE, "DOUBLE"},
    {SQL_BIT, "BIT"},
    {SQL_TINYINT, "TINYINT"},
    {SQL_BIGINT, "BIGINT"},
    {SQL_BINARY, "BINARY"},
    {SQL_VARBINARY, "VARBINARY"},
    {SQL_LONGVARBINARY, "LONGVARBINARY"},
    {SQL_DATE, "DATE"},
    {SQL_TIME, "TIME"},
    {SQL_TIMESTAMP, "TIMESTAMP"},
    {SQL_TYPE_DATE, "DATE"},
    {SQL_TYPE_TIME, "TIME"},
    {SQL_TYPE_TIMESTAMP, "TIMESTAMP"},
    {SQL_GUID, "GUID"}
};

void OdbcUtils::ThrowException(const std::string& operation, const nanodbc::database_error& e) {
    throw BinderException("ODBC error: Failed to " + operation + ": " + e.what());
}

std::string OdbcUtils::SanitizeString(const std::string& input) {
    return StringUtil::Replace(input, "\"", "\"\"");
}

std::string OdbcUtils::GetTypeName(SQLSMALLINT odbcType) {
    auto it = TYPE_NAMES.find(odbcType);
    if (it != TYPE_NAMES.end()) {
        return it->second;
    }
    return "UNKNOWN";
}

LogicalType OdbcUtils::OdbcTypeToLogicalType(SQLSMALLINT odbcType, SQLULEN columnSize, SQLSMALLINT decimalDigits) {
    auto it = ODBC_TO_DUCKDB_TYPES.find(odbcType);
    if (it != ODBC_TO_DUCKDB_TYPES.end()) {
        LogicalTypeId typeId = it->second;
        
        // Special handling for decimal
        if (typeId == LogicalTypeId::DECIMAL) {
            if (columnSize == 0) columnSize = 38;  // Default precision
            // if (decimalDigits == 0 && odbcType == SQL_DECIMAL) decimalDigits = 2;  // Default scale
            return LogicalType::DECIMAL(columnSize, decimalDigits);
        }
        
        return LogicalType(typeId);
    }
    
    // Default to VARCHAR for unknown types
    return LogicalType::VARCHAR;
}

bool OdbcUtils::IsVarcharType(SQLSMALLINT sqlType) {
    return sqlType == SQL_CHAR || sqlType == SQL_VARCHAR || 
           sqlType == SQL_LONGVARCHAR || sqlType == SQL_WCHAR || 
           sqlType == SQL_WVARCHAR || sqlType == SQL_WLONGVARCHAR;
}

#ifdef _WIN32
#include <windows.h>
std::string OdbcUtils::ConvertToUTF8(const std::string& input, int codepage) {
    if (input.empty()) {
        return input;
    }
    
    // First, convert from specified codepage to UTF-16
    int wide_size = MultiByteToWideChar(codepage, MB_ERR_INVALID_CHARS,
                                      input.c_str(), -1, nullptr, 0);
    
    if (wide_size == 0) {
        // If conversion fails, return original string
        return input;
    }
    
    std::vector<wchar_t> wide_str(wide_size);
    if (MultiByteToWideChar(codepage, MB_ERR_INVALID_CHARS, 
                           input.c_str(), -1, 
                           wide_str.data(), wide_size) == 0) {
        // If conversion fails, return original string
        return input;
    }
    
    // Then convert from UTF-16 to UTF-8
    int utf8_size = WideCharToMultiByte(CP_UTF8, 0,
                                       wide_str.data(), -1,
                                       nullptr, 0, nullptr, nullptr);
    
    if (utf8_size == 0) {
        // If conversion fails, return original string
        return input;
    }
    
    std::vector<char> utf8_str(utf8_size);
    if (WideCharToMultiByte(CP_UTF8, 0,
                           wide_str.data(), -1,
                           utf8_str.data(), utf8_size,
                           nullptr, nullptr) == 0) {
        // If conversion fails, return original string
        return input;
    }
    
    // Remove null terminator if present
    if (utf8_size > 0 && utf8_str[utf8_size - 1] == '\0') {
        return std::string(utf8_str.data(), utf8_size - 1);
    }
    
    return std::string(utf8_str.data(), utf8_size);
}
#endif

} // namespace duckdb