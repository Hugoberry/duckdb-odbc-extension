#include "odbc_utils.hpp"

namespace duckdb {

void ODBCUtils::Check(SQLRETURN rc, SQLSMALLINT handle_type, SQLHANDLE handle, const std::string &operation) {
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO) {
        std::string error_message = GetErrorMessage(handle_type, handle);
        throw std::runtime_error("ODBC Error in " + operation + ": " + error_message);
    }
}

std::string ODBCUtils::GetErrorMessage(SQLSMALLINT handle_type, SQLHANDLE handle) {
    SQLCHAR sql_state[6];
    SQLINTEGER native_error;
    SQLCHAR message_text[SQL_MAX_MESSAGE_LENGTH];
    SQLSMALLINT text_length;
    
    SQLRETURN ret = SQLGetDiagRec(handle_type, handle, 1, sql_state, &native_error, 
                                 message_text, sizeof(message_text), &text_length);
    
    if (ret == SQL_SUCCESS || ret == SQL_SUCCESS_WITH_INFO) {
        return std::string((char*)message_text, text_length);
    } else {
        return "Unknown error";
    }
}

std::string ODBCUtils::TypeToString(SQLSMALLINT odbc_type) {
    switch (odbc_type) {
        case SQL_CHAR:         return "CHAR";
        case SQL_VARCHAR:      return "VARCHAR";
        case SQL_LONGVARCHAR:  return "LONGVARCHAR";
        case SQL_WCHAR:        return "WCHAR";
        case SQL_WVARCHAR:     return "WVARCHAR";
        case SQL_WLONGVARCHAR: return "WLONGVARCHAR";
        case SQL_DECIMAL:      return "DECIMAL";
        case SQL_NUMERIC:      return "NUMERIC";
        case SQL_SMALLINT:     return "SMALLINT";
        case SQL_INTEGER:      return "INTEGER";
        case SQL_REAL:         return "REAL";
        case SQL_FLOAT:        return "FLOAT";
        case SQL_DOUBLE:       return "DOUBLE";
        case SQL_BIT:          return "BIT";
        case SQL_TINYINT:      return "TINYINT";
        case SQL_BIGINT:       return "BIGINT";
        case SQL_BINARY:       return "BINARY";
        case SQL_VARBINARY:    return "VARBINARY";
        case SQL_LONGVARBINARY:return "LONGVARBINARY";
        case SQL_DATE:         return "DATE";
        case SQL_TIME:         return "TIME";
        case SQL_TIMESTAMP:    return "TIMESTAMP";
        case SQL_TYPE_DATE:    return "TYPE_DATE";
        case SQL_TYPE_TIME:    return "TYPE_TIME";
        case SQL_TYPE_TIMESTAMP:return "TYPE_TIMESTAMP";
        case SQL_GUID:         return "GUID";
        default:               return "UNKNOWN(" + std::to_string(odbc_type) + ")";
    }
}

std::string ODBCUtils::SanitizeString(const std::string &input) {
    return StringUtil::Replace(input, "\"", "\"\"");
}

SQLSMALLINT ODBCUtils::ToODBCType(const LogicalType &input) {
    switch (input.id()) {
        case LogicalTypeId::BOOLEAN:     return SQL_BIT;
        case LogicalTypeId::TINYINT:     return SQL_TINYINT;
        case LogicalTypeId::SMALLINT:    return SQL_SMALLINT;
        case LogicalTypeId::INTEGER:     return SQL_INTEGER;
        case LogicalTypeId::BIGINT:      return SQL_BIGINT;
        case LogicalTypeId::FLOAT:       return SQL_REAL;
        case LogicalTypeId::DOUBLE:      return SQL_DOUBLE;
        case LogicalTypeId::VARCHAR:     return SQL_VARCHAR;
        case LogicalTypeId::BLOB:        return SQL_VARBINARY;
        case LogicalTypeId::TIMESTAMP:   return SQL_TYPE_TIMESTAMP;
        case LogicalTypeId::DATE:        return SQL_TYPE_DATE;
        case LogicalTypeId::TIME:        return SQL_TYPE_TIME;
        case LogicalTypeId::DECIMAL:     return SQL_DECIMAL;
        case LogicalTypeId::UTINYINT:    return SQL_TINYINT;
        case LogicalTypeId::USMALLINT:   return SQL_SMALLINT;
        case LogicalTypeId::UINTEGER:    return SQL_INTEGER;
        case LogicalTypeId::UBIGINT:     return SQL_BIGINT;
        case LogicalTypeId::HUGEINT:     return SQL_VARCHAR; // Convert to string for HUGEINT
        case LogicalTypeId::LIST:        return SQL_VARCHAR; // Serialize lists as strings
        case LogicalTypeId::STRUCT:      return SQL_VARCHAR; // Serialize structs as strings
        case LogicalTypeId::MAP:         return SQL_VARCHAR; // Serialize maps as strings
        case LogicalTypeId::UUID:        return SQL_GUID;    // Use GUID type if available
        default:                         return SQL_VARCHAR; // Default to VARCHAR for unknown types
    }
}

// Enhanced TypeToLogicalType with better precision and scale handling
LogicalType ODBCUtils::TypeToLogicalType(SQLSMALLINT odbc_type, SQLULEN column_size, SQLSMALLINT decimal_digits) {
    switch (odbc_type) {
        case SQL_BIT:
#ifdef SQL_BOOLEAN
        case SQL_BOOLEAN:
#endif
            return LogicalType::BOOLEAN;
            
        case SQL_TINYINT:
            return LogicalType::TINYINT;
            
        case SQL_SMALLINT:
            return LogicalType::SMALLINT;
            
        case SQL_INTEGER:
            return LogicalType::INTEGER;
            
        case SQL_BIGINT:
            return LogicalType::BIGINT;
            
        case SQL_REAL:
            return LogicalType::FLOAT;
            
        case SQL_FLOAT:
        case SQL_DOUBLE:
            return LogicalType::DOUBLE;
            
        case SQL_DECIMAL:
        case SQL_NUMERIC:
            // Improved handling for DECIMAL/NUMERIC types
            if (decimal_digits == 0) {
                // For whole numbers, map to integer types based on precision
                if (column_size <= 2) {
                    return LogicalType::TINYINT;
                } else if (column_size <= 4) {
                    return LogicalType::SMALLINT;
                } else if (column_size <= 9) {
                    return LogicalType::INTEGER;
                } else if (column_size <= 18) {
                    return LogicalType::BIGINT;
                } else {
                    // For very large numbers like NUMBER(38,0) - use HUGEINT or DECIMAL
                    if (column_size <= 38) {
                        // HUGEINT can store up to 38 digits
                        return LogicalType::HUGEINT;
                    } else {
                        // Fall back to DECIMAL for larger precision
                        return LogicalType::DECIMAL(column_size, decimal_digits);
                    }
                }
            } else {
                // With decimal places, use DECIMAL with the specified precision and scale
                return LogicalType::DECIMAL(column_size, decimal_digits);
            }
            
        case SQL_CHAR:
        case SQL_VARCHAR:
        case SQL_LONGVARCHAR:
        case SQL_WCHAR:
        case SQL_WVARCHAR:
        case SQL_WLONGVARCHAR:
            return LogicalType::VARCHAR;
            
        case SQL_BINARY:
        case SQL_VARBINARY:
        case SQL_LONGVARBINARY:
            return LogicalType::BLOB;
            
        case SQL_DATE:
        case SQL_TYPE_DATE:
            return LogicalType::DATE;
            
        case SQL_TIME:
        case SQL_TYPE_TIME:
            return LogicalType::TIME;
            
        case SQL_TIMESTAMP:
        case SQL_TYPE_TIMESTAMP:
            // Check if this might be a timestamp with timezone based on column size
            // Most drivers use a larger column size for TIMESTAMP WITH TIMEZONE
            if (column_size > 19) { // Standard timestamp is 'YYYY-MM-DD HH:MM:SS' (19 chars)
                // This is a heuristic - some databases might use a different format
                // For now we still use TIMESTAMP, but we could consider TIMESTAMP WITH TIME ZONE
                // when DuckDB adds better support for it
                return LogicalType::TIMESTAMP;
            }
            return LogicalType::TIMESTAMP;
            
        case SQL_GUID:
            return LogicalType::UUID;
            
        default:
            // For unknown types, fall back to VARCHAR
            return LogicalType::VARCHAR;
    }
}

// Detect if a column contains timezone information (helper function)
bool ODBCUtils::IsTimestampWithTimezone(SQLHSTMT stmt, SQLUSMALLINT column_index) {
    char column_name[256];
    SQLSMALLINT name_len;
    SQLRETURN ret;
    
    // Try to get column name
    ret = SQLColAttribute(stmt, column_index, SQL_DESC_NAME, 
                         column_name, sizeof(column_name), &name_len, NULL);
    
    if (ret == SQL_SUCCESS || ret == SQL_SUCCESS_WITH_INFO) {
        std::string col_name = std::string(column_name, name_len);
        
        // Look for common timezone indicators in column name
        if (StringUtil::Contains(StringUtil::Lower(col_name), "tz") ||
            StringUtil::Contains(StringUtil::Lower(col_name), "timezone") ||
            StringUtil::Contains(StringUtil::Lower(col_name), "with_timezone")) {
            return true;
        }
    }
    
    // Try to get type name
    char type_name[256];
    SQLSMALLINT type_name_len;
    
    ret = SQLColAttribute(stmt, column_index, SQL_DESC_TYPE_NAME, 
                         type_name, sizeof(type_name), &type_name_len, NULL);
    
    if (ret == SQL_SUCCESS || ret == SQL_SUCCESS_WITH_INFO) {
        std::string col_type = std::string(type_name, type_name_len);
        
        // Look for timezone indicators in type name
        if (StringUtil::Contains(StringUtil::Lower(col_type), "tz") ||
            StringUtil::Contains(StringUtil::Lower(col_type), "timezone") ||
            StringUtil::Contains(StringUtil::Lower(col_type), "with time zone")) {
            return true;
        }
    }
    
    return false;
}

} // namespace duckdb