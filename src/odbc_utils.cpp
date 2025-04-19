#include "odbc_utils.hpp"

namespace duckdb {

std::string OdbcUtils::HandleException(const nanodbc::database_error& e) {
    // Format and return the error message
    std::string message = e.what();
    
    // nanodbc::database_error doesn't publicly expose native_error or state
    // We can only access what's in the public interface, which is the message from what()
    
    return message;
}

std::string OdbcUtils::TypeToString(SQLSMALLINT odbc_type) {
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

std::string OdbcUtils::SanitizeString(const std::string &input) {
    return StringUtil::Replace(input, "\"", "\"\"");
}

SQLSMALLINT OdbcUtils::ToODBCType(const LogicalType &input) {
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

int OdbcUtils::GetOdbcType(const LogicalType& type) {
    // Map DuckDB types to nanodbc-compatible C types
    switch (type.id()) {
        case LogicalTypeId::BOOLEAN:     return SQL_C_BIT;
        case LogicalTypeId::TINYINT:     return SQL_C_STINYINT;
        case LogicalTypeId::SMALLINT:    return SQL_C_SSHORT;
        case LogicalTypeId::INTEGER:     return SQL_C_SLONG;
        case LogicalTypeId::BIGINT:      return SQL_C_SBIGINT;
        case LogicalTypeId::FLOAT:       return SQL_C_FLOAT;
        case LogicalTypeId::DOUBLE:      return SQL_C_DOUBLE;
        case LogicalTypeId::VARCHAR:     return SQL_C_CHAR;
        case LogicalTypeId::BLOB:        return SQL_C_BINARY;
        case LogicalTypeId::TIMESTAMP:   return SQL_C_TYPE_TIMESTAMP;
        case LogicalTypeId::DATE:        return SQL_C_TYPE_DATE;
        case LogicalTypeId::TIME:        return SQL_C_TYPE_TIME;
        case LogicalTypeId::DECIMAL:     return SQL_C_CHAR; // DECIMAL through strings
        default:                         return SQL_C_CHAR; // Default to string for other types
    }
}

LogicalType OdbcUtils::TypeToLogicalType(SQLSMALLINT odbc_type, SQLULEN column_size, SQLSMALLINT decimal_digits) {
    switch (odbc_type) {
        case SQL_BIT:
#ifdef SQL_BOOLEAN
        case SQL_BOOLEAN:
#endif
            return LogicalType(LogicalTypeId::BOOLEAN);
            
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

bool OdbcUtils::IsBinaryType(SQLSMALLINT sqltype) {
    switch (sqltype) {
    case SQL_BINARY:
    case SQL_VARBINARY:
    case SQL_LONGVARBINARY:
        return true;
    }
    return false;
}

bool OdbcUtils::IsWideType(SQLSMALLINT sqltype) {
    switch (sqltype) {
    case SQL_WCHAR:
    case SQL_WVARCHAR:
    case SQL_WLONGVARCHAR:
        return true;
    }
    return false;
}

bool OdbcUtils::ReadVarColumn(nanodbc::result& result, idx_t col_idx, bool& isNull, std::vector<char>& output) {
    isNull = false;
    output.clear();
    
    try {
        if (result.is_null(col_idx)) {
            isNull = true;
            return true;
        }
        
        // Get the data using nanodbc
        std::string value = result.get<std::string>(col_idx);
        
        // Copy to output vector
        output.assign(value.begin(), value.end());
        return true;
    } catch (const nanodbc::database_error& e) {
        throw std::runtime_error("Failed to read variable column: " + HandleException(e));
    }
}

void OdbcUtils::GetColumnMetadata(nanodbc::result& result, idx_t col_idx, 
                                   SQLSMALLINT& type, SQLULEN& column_size, SQLSMALLINT& decimal_digits) {
    // Use nanodbc's metadata functions
    try {
        // Get the column data type
        type = result.column_datatype(col_idx);
        
        // Get the column size and decimal digits
        column_size = 0;
        decimal_digits = 0;
        
        // For some data types, we need additional metadata
        if (type == SQL_NUMERIC || type == SQL_DECIMAL) {
            column_size = result.column_size(col_idx);
            decimal_digits = result.column_decimal_digits(col_idx);
        } else if (type == SQL_CHAR || type == SQL_VARCHAR || type == SQL_WCHAR || type == SQL_WVARCHAR) {
            column_size = result.column_size(col_idx);
        } else if (type == SQL_BINARY || type == SQL_VARBINARY) {
            column_size = result.column_size(col_idx);
        }
    } catch (const nanodbc::database_error& e) {
        throw std::runtime_error("Failed to get column metadata: " + HandleException(e));
    }
}

} // namespace duckdb