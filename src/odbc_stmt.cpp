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

SQLSMALLINT ODBCStatement::GetODBCType(idx_t col, SQLULEN* column_size, SQLSMALLINT* decimal_digits) {
    if (!hstmt) {
        throw std::runtime_error("Statement is not open");
    }
    
    SQLSMALLINT data_type;
    SQLULEN size;
    SQLSMALLINT digits;
    SQLSMALLINT nullable;
    
    SQLRETURN ret = SQLDescribeCol(hstmt, col + 1, nullptr, 0, nullptr, 
                                  &data_type, &size, &digits, &nullable);
    
    if (ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO) {
        std::string error = ODBCUtils::GetErrorMessage(SQL_HANDLE_STMT, hstmt);
        throw std::runtime_error("Failed to get column type: " + error);
    }
    
    // Pass back column size and decimal digits if requested
    if (column_size) *column_size = size;
    if (decimal_digits) *decimal_digits = digits;
    
    return data_type;
}

int ODBCStatement::GetType(idx_t col) {
    SQLSMALLINT odbc_type = GetODBCType(col);
    
    // Map ODBC types to DuckDB internal type numbers
    // This is a simplified version - a real implementation would map all ODBC types
    switch (odbc_type) {
        case SQL_CHAR:
        case SQL_VARCHAR:
        case SQL_LONGVARCHAR:
            return (int)LogicalTypeId::VARCHAR;
        case SQL_WCHAR:
        case SQL_WVARCHAR:
        case SQL_WLONGVARCHAR:
            return (int)LogicalTypeId::VARCHAR;
        case SQL_BINARY:
        case SQL_VARBINARY:
        case SQL_LONGVARBINARY:
            return (int)LogicalTypeId::BLOB;
        case SQL_SMALLINT:
        case SQL_INTEGER:
        case SQL_TINYINT:
            return (int)LogicalTypeId::INTEGER;
        case SQL_BIGINT:
            return (int)LogicalTypeId::BIGINT;
        case SQL_REAL:
        case SQL_FLOAT:
        case SQL_DOUBLE:
            return (int)LogicalTypeId::DOUBLE;
        case SQL_DECIMAL:
        case SQL_NUMERIC:
            return (int)LogicalTypeId::DECIMAL;
        case SQL_BIT:
#ifdef SQL_BOOLEAN
        case SQL_BOOLEAN:
#endif
            return (int)LogicalTypeId::BOOLEAN;
        case SQL_DATE:
            return (int)LogicalTypeId::DATE;
        case SQL_TIME:
            return (int)LogicalTypeId::TIME;
        case SQL_TIMESTAMP:
            return (int)LogicalTypeId::TIMESTAMP;
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

template <>
std::string ODBCStatement::GetValue(idx_t col) {
    if (!hstmt) {
        throw std::runtime_error("Statement is not open");
    }
    
    char buffer[4096];
    SQLLEN indicator;
    
    SQLRETURN ret = SQLGetData(hstmt, col + 1, SQL_C_CHAR, buffer, sizeof(buffer), &indicator);
    
    if (ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO) {
        std::string error = ODBCUtils::GetErrorMessage(SQL_HANDLE_STMT, hstmt);
        throw std::runtime_error("Failed to get string value: " + error);
    }
    
    if (indicator == SQL_NULL_DATA) {
        return std::string();
    }
    
    return std::string(buffer, indicator < sizeof(buffer) ? indicator : sizeof(buffer));
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
            case LogicalTypeId::DOUBLE:
                Bind<double>(c, FlatVector::GetData<double>(col)[r]);
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