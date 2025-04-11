#include "odbc_utils.hpp"

namespace duckdb {

void ODBCUtils::Check(SQLRETURN rc, SQLSMALLINT handle_type, SQLHANDLE handle, const std::string &operation) {
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO) {
        std::string error_message = GetErrorMessage(handle_type, handle);
        throw std::runtime_error("ODBC Error in " + operation + ": " + error_message);
    }
}

std::string ODBCUtils::GetErrorMessage(SQLSMALLINT handle_type, SQLHANDLE handle) {
    SQLCHAR sql_state[6] = {0};
    SQLINTEGER native_error;
    SQLCHAR message_text[SQL_MAX_MESSAGE_LENGTH] = {0};
    SQLSMALLINT text_length = 0;
    duckdb::vector<std::string> error_parts;
    
    SQLSMALLINT i = 1;
    while (SQLGetDiagRec(handle_type, handle, i, sql_state, &native_error,
                        message_text, SQL_MAX_MESSAGE_LENGTH, &text_length) == SQL_SUCCESS) {
        // Convert ODBC fields to strings
        std::string state_str(reinterpret_cast<char*>(sql_state), 5);
        std::string message(reinterpret_cast<char*>(message_text), text_length);

        // Add context-sensitive description
        std::string description;
        if (state_str == "HY000") {
            description = " (General Error)";
        } else if (state_str == "HYT00") {
            description = " (Timeout Expired)";
        } else if (state_str == "08S01") {
            description = " (Communication Link Failure)";
        }

        // Format using DuckDB's string utilities
        auto formatted_part = StringUtil::Format("[%s] %s%s",
            state_str.c_str(),
            message.c_str(),
            description.c_str()
        );
        
        error_parts.push_back(formatted_part);
        i++;
    }
    
    return error_parts.empty() ? "No ODBC error information available" 
                              : StringUtil::Join(error_parts, string(" | "));
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
        case SQL_GUID:         return "GUID";
        default:               return "UNKNOWN";
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
        case LogicalTypeId::TIMESTAMP:   return SQL_TIMESTAMP;
        case LogicalTypeId::DATE:        return SQL_DATE;
        case LogicalTypeId::TIME:        return SQL_TIME;
        case LogicalTypeId::DECIMAL:     return SQL_DECIMAL;
        case LogicalTypeId::UTINYINT:    return SQL_TINYINT;
        case LogicalTypeId::USMALLINT:   return SQL_SMALLINT;
        case LogicalTypeId::UINTEGER:    return SQL_INTEGER;
        case LogicalTypeId::UBIGINT:     return SQL_BIGINT;
        case LogicalTypeId::HUGEINT:     return SQL_VARCHAR; // Convert to string for HUGEINT
        case LogicalTypeId::LIST:        return SQL_VARCHAR; // Serialize lists as strings
        case LogicalTypeId::STRUCT:      return SQL_VARCHAR; // Serialize structs as strings
        case LogicalTypeId::MAP:         return SQL_VARCHAR; // Serialize maps as strings
        default:                         return SQL_VARCHAR; // Default to VARCHAR for unknown types
    }
}

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
        case SQL_FLOAT:
            return LogicalType::FLOAT;
            
        case SQL_DOUBLE:
            return LogicalType::DOUBLE;
            
        case SQL_DECIMAL:
        case SQL_NUMERIC:
            return LogicalType::DECIMAL(column_size, decimal_digits);
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
        case SQL_TYPE_TIME:    
        case SQL_TIME:
            return LogicalType::TIME;
        case SQL_TYPE_TIMESTAMP:    
        case SQL_TIMESTAMP:
            return LogicalType::TIMESTAMP;
            
        case SQL_GUID:
            return LogicalType::UUID;
            
        default:
            return LogicalType::VARCHAR;
    }
}

bool ODBCUtils::IsBinaryType(SQLSMALLINT sqltype) {
    switch (sqltype) {
    case SQL_BINARY:
    case SQL_VARBINARY:
    case SQL_LONGVARBINARY:
        return true;
    }
    return false;
}

bool ODBCUtils::IsWideType(SQLSMALLINT sqltype) {
    switch (sqltype) {
    case SQL_WCHAR:
    case SQL_WVARCHAR:
    case SQL_WLONGVARCHAR:
    // case SQL_SS_XML:
    // case SQL_DB2_XML:
        return true;
    }
    return false;
}

bool ODBCUtils::ReadVarColumn(SQLHSTMT hstmt, SQLUSMALLINT col_idx, SQLSMALLINT ctype, 
                             bool& isNull, std::vector<char>& result) {
    isNull = false;
    result.clear();
    
    // Determine if null terminator is needed (text vs binary)
    const bool needsNullTerm = !IsBinaryType(ctype);
    const size_t nullTermSize = needsNullTerm ? (IsWideType(ctype) ? sizeof(SQLWCHAR) : sizeof(SQLCHAR)) : 0;
    
    // Start with a reasonable buffer size
    size_t bufferSize = 4096;
    result.resize(bufferSize);
    size_t totalRead = 0;
    
    SQLRETURN ret = SQL_SUCCESS_WITH_INFO;
    
    do {
        SQLLEN cbData = 0;
        size_t availableSpace = bufferSize - totalRead;
        
        ret = SQLGetData(hstmt, col_idx, ctype, 
                        result.data() + totalRead, 
                        (SQLLEN)availableSpace, &cbData);
        
        if (!SQL_SUCCEEDED(ret) && ret != SQL_NO_DATA) {
            std::string error = GetErrorMessage(SQL_HANDLE_STMT, hstmt);
            throw std::runtime_error("Failed to read variable data: " + error);
        }
        
        if (cbData == SQL_NULL_DATA) {
            isNull = true;
            return true;
        }
        
        if (ret == SQL_SUCCESS) {
            // We got all the data
            totalRead += (size_t)cbData;
            result.resize(totalRead);
            break;
        } else if (ret == SQL_SUCCESS_WITH_INFO) {
            // We need more space
            if (cbData == SQL_NO_TOTAL) {
                // Driver can't tell us how much data remains
                totalRead += (availableSpace - nullTermSize);
                bufferSize *= 2; // Double the buffer size
            } else if ((size_t)cbData >= availableSpace) {
                // We read what we could fit but there's more
                totalRead += (availableSpace - nullTermSize);
                size_t remaining = (size_t)cbData - (availableSpace - nullTermSize);
                bufferSize = totalRead + remaining + nullTermSize;
            } else {
                // Unexpected case - success with info but buffer not full?
                totalRead += (size_t)cbData;
                break;
            }
            
            result.resize(bufferSize);
        }
    } while (ret == SQL_SUCCESS_WITH_INFO);
    
    return true;
}

} // namespace duckdb