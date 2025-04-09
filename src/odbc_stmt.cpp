#include "odbc_stmt.hpp"
#include "odbc_db.hpp"
#include "odbc_utils.hpp"
#include "odbc_scanner.hpp"
#include "duckdb/common/types/date.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/common/types/time.hpp"

namespace duckdb {

ODBCStatement::ODBCStatement() : hdbc(nullptr), hstmt(nullptr) {
}

ODBCStatement::ODBCStatement(SQLHDBC hdbc, SQLHSTMT hstmt) : hdbc(hdbc), hstmt(hstmt) {
}

ODBCStatement::~ODBCStatement() {
    Close();
}

ODBCStatement::ODBCStatement(ODBCStatement &&other) noexcept {
    hdbc = other.hdbc;
    hstmt = other.hstmt;
    other.hdbc = nullptr;
    other.hstmt = nullptr;
}

ODBCStatement &ODBCStatement::operator=(ODBCStatement &&other) noexcept {
    if (this != &other) {
        Close();
        hdbc = other.hdbc;
        hstmt = other.hstmt;
        other.hdbc = nullptr;
        other.hstmt = nullptr;
    }
    return *this;
}

bool ODBCStatement::Step() {
    if (!hdbc || !hstmt) {
        return false;
    }
    
    SQLRETURN ret = SQLExecute(hstmt);
    if (ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO) {
        if (ret == SQL_NO_DATA) {
            return false;
        }
        std::string error = ODBCUtils::GetErrorMessage(SQL_HANDLE_STMT, hstmt);
        throw std::runtime_error("Failed to execute statement: " + error);
    }
    
    ret = SQLFetch(hstmt);
    if (ret == SQL_NO_DATA) {
        return false;
    }
    
    if (ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO) {
        std::string error = ODBCUtils::GetErrorMessage(SQL_HANDLE_STMT, hstmt);
        throw std::runtime_error("Failed to fetch row: " + error);
    }
    
    return true;
}

void ODBCStatement::Reset() {
    if (hstmt) {
        SQLFreeStmt(hstmt, SQL_CLOSE);
    }
}

void ODBCStatement::Close() {
    if (hstmt) {
        SQLFreeHandle(SQL_HANDLE_STMT, hstmt);
        hstmt = nullptr;
    }
    hdbc = nullptr;
}

bool ODBCStatement::IsOpen() {
    return hstmt != nullptr;
}

SQLSMALLINT ODBCStatement::GetODBCType(idx_t col) {
    if (!hstmt) {
        throw std::runtime_error("Statement is not open");
    }
    
    SQLSMALLINT data_type;
    SQLULEN column_size;
    SQLSMALLINT decimal_digits;
    SQLSMALLINT nullable;
    
    SQLRETURN ret = SQLDescribeCol(hstmt, col + 1, nullptr, 0, nullptr, 
                                  &data_type, &column_size, &decimal_digits, &nullable);
    
    if (ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO) {
        std::string error = ODBCUtils::GetErrorMessage(SQL_HANDLE_STMT, hstmt);
        throw std::runtime_error("Failed to get column type: " + error);
    }
    
    return data_type;
}

// Get column attributes including precision and scale
bool ODBCStatement::GetColumnAttributes(idx_t col, SQLSMALLINT &data_type, 
                                        SQLULEN &column_size, SQLSMALLINT &decimal_digits) {
    if (!hstmt) {
        return false;
    }
    
    SQLSMALLINT nullable; // We don't need this but SQLDescribeCol requires it
    
    SQLRETURN ret = SQLDescribeCol(hstmt, col + 1, nullptr, 0, nullptr, 
                                  &data_type, &column_size, &decimal_digits, &nullable);
    
    return (ret == SQL_SUCCESS || ret == SQL_SUCCESS_WITH_INFO);
}

int ODBCStatement::GetType(idx_t col) {
    SQLSMALLINT odbc_type = GetODBCType(col);
    
    // Get precision and scale for better type conversion
    SQLSMALLINT data_type;
    SQLULEN column_size;
    SQLSMALLINT decimal_digits;
    
    if (GetColumnAttributes(col, data_type, column_size, decimal_digits)) {
        auto logical_type = ODBCUtils::TypeToLogicalType(odbc_type, column_size, decimal_digits);
        return (int)logical_type.id();
    }
    
    // Fallback to basic mapping without precision/scale if getting attributes failed
    switch (odbc_type) {
        case SQL_CHAR:
        case SQL_VARCHAR:
        case SQL_LONGVARCHAR:
        case SQL_WCHAR:
        case SQL_WVARCHAR:
        case SQL_WLONGVARCHAR:
            return (int)LogicalTypeId::VARCHAR;
        case SQL_BINARY:
        case SQL_VARBINARY:
        case SQL_LONGVARBINARY:
            return (int)LogicalTypeId::BLOB;
        case SQL_SMALLINT:
            return (int)LogicalTypeId::SMALLINT;
        case SQL_INTEGER:
            return (int)LogicalTypeId::INTEGER;
        case SQL_TINYINT:
            return (int)LogicalTypeId::TINYINT;
        case SQL_BIGINT:
            return (int)LogicalTypeId::BIGINT;
        case SQL_REAL:
        case SQL_FLOAT:
            return (int)LogicalTypeId::FLOAT;
        case SQL_DOUBLE:
            return (int)LogicalTypeId::DOUBLE;
        case SQL_DECIMAL:
        case SQL_NUMERIC:
            // Need precision/scale for proper decimal handling,
            // but since we don't have it here, use DOUBLE
            return (int)LogicalTypeId::DOUBLE;
        case SQL_BIT:
#ifdef SQL_BOOLEAN
        case SQL_BOOLEAN:
#endif
            return (int)LogicalTypeId::BOOLEAN;
        case SQL_DATE:
        case SQL_TYPE_DATE:
            return (int)LogicalTypeId::DATE;
        case SQL_TIME:
        case SQL_TYPE_TIME:
            return (int)LogicalTypeId::TIME;
        case SQL_TIMESTAMP:
        case SQL_TYPE_TIMESTAMP:
            // Check for timezone info
            if (ODBCUtils::IsTimestampWithTimezone(hstmt, col + 1)) {
                // For timestamp with timezone, consider a special mapping
                // but we don't have this type in DuckDB yet, use TIMESTAMP
                return (int)LogicalTypeId::TIMESTAMP;
            }
            return (int)LogicalTypeId::TIMESTAMP;
        case SQL_GUID:
            return (int)LogicalTypeId::UUID;
        default:
            return (int)LogicalTypeId::VARCHAR;
    }
}

std::string ODBCStatement::GetName(idx_t col) {
    if (!hstmt) {
        throw std::runtime_error("Statement is not open");
    }
    
    SQLCHAR column_name[256];
    SQLSMALLINT name_length;
    
    SQLRETURN ret = SQLColAttribute(hstmt, col + 1, SQL_DESC_NAME, 
                                   column_name, sizeof(column_name), &name_length, nullptr);
    
    if (ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO) {
        std::string error = ODBCUtils::GetErrorMessage(SQL_HANDLE_STMT, hstmt);
        throw std::runtime_error("Failed to get column name: " + error);
    }
    
    return std::string((char*)column_name, name_length);
}

idx_t ODBCStatement::GetColumnCount() {
    if (!hstmt) {
        throw std::runtime_error("Statement is not open");
    }
    
    SQLSMALLINT column_count;
    SQLRETURN ret = SQLNumResultCols(hstmt, &column_count);
    
    if (ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO) {
        std::string error = ODBCUtils::GetErrorMessage(SQL_HANDLE_STMT, hstmt);
        throw std::runtime_error("Failed to get column count: " + error);
    }
    
    return column_count;
}

// Implementation for string value retrieval with potential chunking
template <>
std::string ODBCStatement::GetValue(idx_t col) {
    if (!hstmt) {
        throw std::runtime_error("Statement is not open");
    }
    
    // Try with a reasonably sized buffer first
    char initial_buffer[4096];
    SQLLEN indicator;
    
    SQLRETURN ret = SQLGetData(hstmt, col + 1, SQL_C_CHAR, initial_buffer, 
                              sizeof(initial_buffer), &indicator);
    
    if (ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO) {
        std::string error = ODBCUtils::GetErrorMessage(SQL_HANDLE_STMT, hstmt);
        throw std::runtime_error("Failed to get string value: " + error);
    }
    
    if (indicator == SQL_NULL_DATA) {
        return std::string();
    }
    
    // If the string fits in our buffer, return it
    if (indicator < sizeof(initial_buffer)) {
        return std::string(initial_buffer, indicator < 0 ? 0 : indicator);
    }
    
    // Large string - need to fetch it in chunks
    std::string large_string;
    large_string.reserve(indicator > 0 ? indicator : 8192);
    
    // Add what we've already read
    large_string.append(initial_buffer, sizeof(initial_buffer) - 1);
    
    // Buffer for subsequent chunks
    char chunk_buffer[8192];
    bool more_data = true;
    
    while (more_data) {
        ret = SQLGetData(hstmt, col + 1, SQL_C_CHAR, chunk_buffer, 
                       sizeof(chunk_buffer), &indicator);
        
        if (ret == SQL_SUCCESS || ret == SQL_SUCCESS_WITH_INFO) {
            if (indicator == SQL_NULL_DATA) {
                break;
            }
            
            // Determine how much to append
            size_t chunk_size;
            if (indicator < sizeof(chunk_buffer)) {
                chunk_size = indicator < 0 ? 0 : indicator;
            } else {
                chunk_size = sizeof(chunk_buffer) - 1;
            }
            
            large_string.append(chunk_buffer, chunk_size);
            more_data = (ret == SQL_SUCCESS_WITH_INFO);
        } else {
            break;
        }
    }
    
    return large_string;
}

template <>
int ODBCStatement::GetValue(idx_t col) {
    if (!hstmt) {
        throw std::runtime_error("Statement is not open");
    }
    
    int value;
    SQLLEN indicator;
    
    SQLRETURN ret = SQLGetData(hstmt, col + 1, SQL_C_LONG, &value, sizeof(value), &indicator);
    
    if (ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO) {
        std::string error = ODBCUtils::GetErrorMessage(SQL_HANDLE_STMT, hstmt);
        throw std::runtime_error("Failed to get int value: " + error);
    }
    
    if (indicator == SQL_NULL_DATA) {
        return 0;
    }
    
    return value;
}

template <>
int64_t ODBCStatement::GetValue(idx_t col) {
    if (!hstmt) {
        throw std::runtime_error("Statement is not open");
    }
    
    int64_t value;
    SQLLEN indicator;
    
    SQLRETURN ret = SQLGetData(hstmt, col + 1, SQL_C_SBIGINT, &value, sizeof(value), &indicator);
    
    if (ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO) {
        std::string error = ODBCUtils::GetErrorMessage(SQL_HANDLE_STMT, hstmt);
        throw std::runtime_error("Failed to get int64 value: " + error);
    }
    
    if (indicator == SQL_NULL_DATA) {
        return 0;
    }
    
    return value;
}

template <>
double ODBCStatement::GetValue(idx_t col) {
    if (!hstmt) {
        throw std::runtime_error("Statement is not open");
    }
    
    double value;
    SQLLEN indicator;
    
    SQLRETURN ret = SQLGetData(hstmt, col + 1, SQL_C_DOUBLE, &value, sizeof(value), &indicator);
    
    if (ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO) {
        std::string error = ODBCUtils::GetErrorMessage(SQL_HANDLE_STMT, hstmt);
        throw std::runtime_error("Failed to get double value: " + error);
    }
    
    if (indicator == SQL_NULL_DATA) {
        return 0.0;
    }
    
    return value;
}

template <>
timestamp_t ODBCStatement::GetValue(idx_t col) {
    if (!hstmt) {
        throw std::runtime_error("Statement is not open");
    }
    
    SQL_TIMESTAMP_STRUCT ts;
    SQLLEN indicator;
    
    SQLRETURN ret = SQLGetData(hstmt, col + 1, SQL_C_TYPE_TIMESTAMP, &ts, sizeof(ts), &indicator);
    
    if (ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO) {
        std::string error = ODBCUtils::GetErrorMessage(SQL_HANDLE_STMT, hstmt);
        throw std::runtime_error("Failed to get timestamp value: " + error);
    }
    
    if (indicator == SQL_NULL_DATA) {
        // Return epoch for null
        return Timestamp::FromEpochSeconds(0);
    }
    
    // Convert SQL_TIMESTAMP_STRUCT to duckdb timestamp
    date_t date = Date::FromDate(ts.year, ts.month, ts.day);
    dtime_t time = Time::FromTime(ts.hour, ts.minute, ts.second, ts.fraction / 1000000);
    return Timestamp::FromDatetime(date, time);
}

template <>
hugeint_t ODBCStatement::GetValue(idx_t col) {
    // Try to get as BIGINT first
    try {
        int64_t value;
        SQLLEN indicator;
        
        SQLRETURN ret = SQLGetData(hstmt, col + 1, SQL_C_SBIGINT, &value, sizeof(value), &indicator);
        
        if (ret == SQL_SUCCESS || ret == SQL_SUCCESS_WITH_INFO) {
            if (indicator == SQL_NULL_DATA) {
                return hugeint_t(0);
            }
            return hugeint_t(value);
        }
    } catch (...) {
        // Fall through to string parsing if direct retrieval fails
    }
    
    // If BIGINT retrieval fails, try to get as string and parse
    char buffer[100]; // Should be enough for any number
    SQLLEN indicator;
    
    SQLRETURN ret = SQLGetData(hstmt, col + 1, SQL_C_CHAR, buffer, sizeof(buffer), &indicator);
    
    if (ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO) {
        std::string error = ODBCUtils::GetErrorMessage(SQL_HANDLE_STMT, hstmt);
        throw std::runtime_error("Failed to get hugeint value: " + error);
    }
    
    if (indicator == SQL_NULL_DATA) {
        return hugeint_t(0);
    }
    
    // Custom parsing of the string value to create a hugeint
    std::string str_val(buffer, indicator < sizeof(buffer) ? indicator : (sizeof(buffer) - 1));
    hugeint_t result;
    
    // Simple parsing: try to convert to int64_t first if possible
    try {
        int64_t int_val = std::stoll(str_val);
        return hugeint_t(int_val);
    } catch (...) {
        // Fall through to manual parsing
    }
    
    // Very basic implementation for huge numbers - in a real implementation,
    // you'd want a more sophisticated parsing algorithm
    bool negative = false;
    result.lower = 0;
    result.upper = 0;
    
    size_t start_idx = 0;
    if (str_val[0] == '-') {
        negative = true;
        start_idx = 1;
    } else if (str_val[0] == '+') {
        start_idx = 1;
    }
    
    // Process digits from left to right
    for (size_t i = start_idx; i < str_val.size(); i++) {
        char c = str_val[i];
        if (c < '0' || c > '9') {
            continue;  // Skip non-digit characters
        }
        
        // Multiply current value by 10 and add the new digit
        hugeint_t ten(10);
        hugeint_t digit(c - '0');
        
        // result = result * 10 + digit
        // For simplicity, we'll do a very basic implementation
        uint64_t old_lower = result.lower;
        result.lower = result.lower * 10 + (c - '0');
        // Handle overflow
        if (result.lower < old_lower) {
            result.upper = result.upper * 10 + 1;  // Carry to upper
        } else {
            result.upper = result.upper * 10;
        }
    }
    
    if (negative) {
        // Negate the result for negative numbers
        result.upper = ~result.upper;
        result.lower = ~result.lower;
        hugeint_t one(1);
        result.lower += 1;
        if (result.lower == 0) {
            result.upper += 1;
        }
    }
    
    return result;
}

SQLLEN ODBCStatement::GetValueLength(idx_t col) {
    if (!hstmt) {
        throw std::runtime_error("Statement is not open");
    }
    
    SQLLEN indicator;
    SQLGetData(hstmt, col + 1, SQL_C_BINARY, NULL, 0, &indicator);
    return indicator;
}

template <>
void ODBCStatement::Bind(idx_t col, int value) {
    if (!hstmt) {
        throw std::runtime_error("Statement is not open");
    }
    
    SQLRETURN ret = SQLBindParameter(hstmt, col + 1, SQL_PARAM_INPUT, SQL_C_LONG, SQL_INTEGER, 
                                   0, 0, (SQLPOINTER)&value, 0, nullptr);
    
    if (ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO) {
        std::string error = ODBCUtils::GetErrorMessage(SQL_HANDLE_STMT, hstmt);
        throw std::runtime_error("Failed to bind int parameter: " + error);
    }
}

template <>
void ODBCStatement::Bind(idx_t col, int64_t value) {
    if (!hstmt) {
        throw std::runtime_error("Statement is not open");
    }
    
    SQLRETURN ret = SQLBindParameter(hstmt, col + 1, SQL_PARAM_INPUT, SQL_C_SBIGINT, SQL_BIGINT, 
                                   0, 0, (SQLPOINTER)&value, 0, nullptr);
    
    if (ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO) {
        std::string error = ODBCUtils::GetErrorMessage(SQL_HANDLE_STMT, hstmt);
        throw std::runtime_error("Failed to bind int64 parameter: " + error);
    }
}

template <>
void ODBCStatement::Bind(idx_t col, double value) {
    if (!hstmt) {
        throw std::runtime_error("Statement is not open");
    }
    
    SQLRETURN ret = SQLBindParameter(hstmt, col + 1, SQL_PARAM_INPUT, SQL_C_DOUBLE, SQL_DOUBLE, 
                                   0, 0, (SQLPOINTER)&value, 0, nullptr);
    
    if (ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO) {
        std::string error = ODBCUtils::GetErrorMessage(SQL_HANDLE_STMT, hstmt);
        throw std::runtime_error("Failed to bind double parameter: " + error);
    }
}

template <>
void ODBCStatement::Bind(idx_t col, std::nullptr_t value) {
    if (!hstmt) {
        throw std::runtime_error("Statement is not open");
    }
    
    SQLRETURN ret = SQLBindParameter(hstmt, col + 1, SQL_PARAM_INPUT, SQL_C_DEFAULT, SQL_NULL_DATA, 
                                   0, 0, nullptr, 0, nullptr);
    
    if (ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO) {
        std::string error = ODBCUtils::GetErrorMessage(SQL_HANDLE_STMT, hstmt);
        throw std::runtime_error("Failed to bind null parameter: " + error);
    }
}

template <>
void ODBCStatement::Bind(idx_t col, hugeint_t value) {
    if (!hstmt) {
        throw std::runtime_error("Statement is not open");
    }
    
    // Check if the value fits in an int64_t
    // A hugeint_t consists of a lower and upper component
    // If the upper is 0 (or -1 for negative numbers that fit in int64) we can safely convert
    if ((value.upper == 0 && value.lower <= (uint64_t)NumericLimits<int64_t>::Maximum()) ||
        (value.upper == -1 && value.lower > (uint64_t)NumericLimits<int64_t>::Maximum())) {
        // Safe to convert to int64_t - it's just the lower bits
        int64_t int64_val = (int64_t)value.lower;
        Bind<int64_t>(col, int64_val);
        return;
    }
    
    // Otherwise, convert to string and bind as VARCHAR
    std::string str_val = Hugeint::ToString(value);
    
    SQLLEN str_len = str_val.length();
    SQLRETURN ret = SQLBindParameter(hstmt, col + 1, SQL_PARAM_INPUT, SQL_C_CHAR, SQL_VARCHAR, 
                                   str_val.length(), 0, (SQLPOINTER)str_val.c_str(), 0, &str_len);
    
    if (ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO) {
        std::string error = ODBCUtils::GetErrorMessage(SQL_HANDLE_STMT, hstmt);
        throw std::runtime_error("Failed to bind hugeint parameter: " + error);
    }
}

void ODBCStatement::BindBlob(idx_t col, const string_t &value) {
    if (!hstmt) {
        throw std::runtime_error("Statement is not open");
    }
    
    SQLLEN len = value.GetSize();
    SQLRETURN ret = SQLBindParameter(hstmt, col + 1, SQL_PARAM_INPUT, SQL_C_BINARY, SQL_BINARY, 
                                   value.GetSize(), 0, (SQLPOINTER)value.GetDataUnsafe(), len, &len);
    
    if (ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO) {
        std::string error = ODBCUtils::GetErrorMessage(SQL_HANDLE_STMT, hstmt);
        throw std::runtime_error("Failed to bind blob parameter: " + error);
    }
}

void ODBCStatement::BindText(idx_t col, const string_t &value) {
    if (!hstmt) {
        throw std::runtime_error("Statement is not open");
    }
    
    SQLLEN len = SQL_NTS;
    SQLRETURN ret = SQLBindParameter(hstmt, col + 1, SQL_PARAM_INPUT, SQL_C_CHAR, SQL_VARCHAR, 
                                   value.GetSize(), 0, (SQLPOINTER)value.GetDataUnsafe(), 0, &len);
    
    if (ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO) {
        std::string error = ODBCUtils::GetErrorMessage(SQL_HANDLE_STMT, hstmt);
        throw std::runtime_error("Failed to bind text parameter: " + error);
    }
}

void ODBCStatement::BindValue(Vector &col, idx_t c, idx_t r) {
    auto &mask = FlatVector::Validity(col);
    if (!mask.RowIsValid(r)) {
        Bind<std::nullptr_t>(c, nullptr);
    } else {
        switch (col.GetType().id()) {
            case LogicalTypeId::BIGINT:
                Bind<int64_t>(c, FlatVector::GetData<int64_t>(col)[r]);
                break;
            case LogicalTypeId::INTEGER:
                Bind<int>(c, FlatVector::GetData<int>(col)[r]);
                break;
            case LogicalTypeId::SMALLINT:
                Bind<int>(c, FlatVector::GetData<int16_t>(col)[r]);
                break;
            case LogicalTypeId::TINYINT:
                Bind<int>(c, FlatVector::GetData<int8_t>(col)[r]);
                break;
            case LogicalTypeId::DOUBLE:
                Bind<double>(c, FlatVector::GetData<double>(col)[r]);
                break;
            case LogicalTypeId::FLOAT:
                Bind<double>(c, FlatVector::GetData<float>(col)[r]);
                break;
            case LogicalTypeId::HUGEINT:
                Bind<hugeint_t>(c, FlatVector::GetData<hugeint_t>(col)[r]);
                break;
            case LogicalTypeId::BLOB:
                BindBlob(c, FlatVector::GetData<string_t>(col)[r]);
                break;
            case LogicalTypeId::VARCHAR:
                BindText(c, FlatVector::GetData<string_t>(col)[r]);
                break;
            default:
                throw std::runtime_error("Unsupported type for ODBC::BindValue: " + col.GetType().ToString());
        }
    }
}

void ODBCStatement::CheckTypeMatches(const ODBCBindData &bind_data, SQLLEN indicator, SQLSMALLINT odbc_type, 
                                    SQLSMALLINT expected_type, idx_t col_idx) {
    if (bind_data.all_varchar) {
        // No type check needed if all columns are treated as varchar
        return;
    }
    
    if (indicator == SQL_NULL_DATA) {
        // Null values don't need type checking
        return;
    }
    
    if (odbc_type != expected_type) {
        std::string column_name = GetName(col_idx);
        std::string message = "Invalid type in column \"" + column_name + "\": column was declared as " +
                              ODBCUtils::TypeToString(expected_type) + ", found " +
                              ODBCUtils::TypeToString(odbc_type) + " instead.";
        message += "\n* SET odbc_all_varchar=true to load all columns as VARCHAR "
                  "and skip type conversions";
        throw std::runtime_error(message);
    }
}

void ODBCStatement::CheckTypeIsFloatOrInteger(SQLSMALLINT odbc_type, idx_t col_idx) {
    if (odbc_type != SQL_FLOAT && odbc_type != SQL_DOUBLE && odbc_type != SQL_REAL &&
        odbc_type != SQL_INTEGER && odbc_type != SQL_SMALLINT && odbc_type != SQL_TINYINT && 
        odbc_type != SQL_BIGINT && odbc_type != SQL_DECIMAL && odbc_type != SQL_NUMERIC) {
        std::string column_name = GetName(col_idx);
        std::string message = "Invalid type in column \"" + column_name + "\": expected float or integer, found " +
                              ODBCUtils::TypeToString(odbc_type) + " instead.";
        message += "\n* SET odbc_all_varchar=true to load all columns as VARCHAR "
                  "and skip type conversions";
        throw std::runtime_error(message);
    }
}

void ODBCStatement::CheckError(SQLRETURN ret, const std::string &operation) {
    if (ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO) {
        std::string error = ODBCUtils::GetErrorMessage(SQL_HANDLE_STMT, hstmt);
        throw std::runtime_error("ODBC Error in " + operation + ": " + error);
    }
}

} // namespace duckdb