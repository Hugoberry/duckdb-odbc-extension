#include "odbc_stmt.hpp"
#include "odbc_db.hpp"
#include "odbc_utils.hpp"
#include "odbc_scanner.hpp"
#include "duckdb/common/types/date.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/common/types/time.hpp"

namespace duckdb {

OdbcStatement::OdbcStatement() : has_result(false), executed(false) {
}

OdbcStatement::OdbcStatement(nanodbc::connection &conn, const std::string &query)
    : has_result(false), executed(false) {
    try {
        // Prepare the statement
        stmt = nanodbc::statement(conn, query);
    } catch (const nanodbc::database_error &e) {
        // Wrap and rethrow with a descriptive message
        throw std::runtime_error("Failed to prepare statement: " +
                                 OdbcUtils::HandleException(e));
    }
}


OdbcStatement::~OdbcStatement() {
    Close();
}

OdbcStatement::OdbcStatement(OdbcStatement &&other) noexcept
    : stmt(std::move(other.stmt)),
      result(std::move(other.result)),
      has_result(other.has_result),
      executed(other.executed) {
    // Reset the moved‐from instance so its destructor is a no‐op
    other.stmt = nanodbc::statement();
    other.result = nanodbc::result();
    other.has_result = false;
    other.executed = false;
}

OdbcStatement &OdbcStatement::operator=(OdbcStatement &&other) noexcept {
    if (this != &other) {
        // Clean up this’s current handles
        Close();
        // Move in the new handles
        stmt = std::move(other.stmt);
        result = std::move(other.result);
        has_result = other.has_result;
        executed = other.executed;
        // Reset the moved‐from so its destructor won’t double‐free
        other.stmt = nanodbc::statement();
        other.result = nanodbc::result();
        other.has_result = false;
        other.executed = false;
    }
    return *this;
}

bool OdbcStatement::Step() {
    if (!IsOpen()) {
        return false;
    }
    try {
        // On the very first call, execute the statement; on every call, advance the cursor.
        if (!executed) {
            result = stmt.execute();
            executed = true;
        }
        // result.next() moves to the *first* row on the first invocation,
        // and to subsequent rows thereafter.
        return result.next();
    } catch (const nanodbc::database_error &e) {
        throw std::runtime_error("Failed to execute statement: " + OdbcUtils::HandleException(e));
    }
}

void OdbcStatement::Reset() {
    if (IsOpen()) {
        try {
            stmt.close();
            has_result = false;
            executed = false;
        } catch (const nanodbc::database_error& e) {
            throw std::runtime_error("Failed to reset statement: " + OdbcUtils::HandleException(e));
        }
    }
}

void OdbcStatement::Close() {
    if (IsOpen()) {
        try {
            stmt.close();
            has_result = false;
            executed = false;
        } catch (...) {
            // Ignore exceptions during close
        }
    }
}

bool OdbcStatement::IsOpen() {
    return stmt.connected();
}

SQLSMALLINT OdbcStatement::GetODBCType(idx_t col, SQLULEN* column_size, SQLSMALLINT* decimal_digits) {
    if (!IsOpen()) {
        throw std::runtime_error("Statement is not open");
    }
    
    try {
        if (!executed) {
            // We need to execute the statement to get metadata
            result = stmt.execute();
            executed = true;
            has_result = true;
        }
        
        SQLSMALLINT data_type;
        SQLULEN size;
        SQLSMALLINT digits;
        
        // Get column metadata from nanodbc
        OdbcUtils::GetColumnMetadata(result, col, data_type, size, digits);
        
        // Pass back column size and decimal digits if requested
        if (column_size) *column_size = size;
        if (decimal_digits) *decimal_digits = digits;
        
        return data_type;
    } catch (const nanodbc::database_error& e) {
        throw std::runtime_error("Failed to get column type: " + OdbcUtils::HandleException(e));
    }
}

int OdbcStatement::GetType(idx_t col) {
    SQLSMALLINT odbc_type = GetODBCType(col);
    
    // Map ODBC types to DuckDB internal type numbers
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

std::string OdbcStatement::GetName(idx_t col) {
    if (!IsOpen()) {
        throw std::runtime_error("Statement is not open");
    }
    
    try {
        if (!executed) {
            // We need to execute the statement to get metadata
            result = stmt.execute();
            executed = true;
            has_result = true;
        }
        
        // Get column name from nanodbc
        return result.column_name(col);
    } catch (const nanodbc::database_error& e) {
        throw std::runtime_error("Failed to get column name: " + OdbcUtils::HandleException(e));
    }
}

idx_t OdbcStatement::GetColumnCount() {
    if (!IsOpen()) {
        throw std::runtime_error("Statement is not open");
    }
    
    try {
        if (!executed) {
            // We need to execute the statement to get metadata
            result = stmt.execute();
            executed = true;
            has_result = true;
        }
        
        return result.columns();
    } catch (const nanodbc::database_error& e) {
        throw std::runtime_error("Failed to get column count: " + OdbcUtils::HandleException(e));
    }
}

template <>
std::string OdbcStatement::GetValue(idx_t col) {
    if (!IsOpen() || !has_result) {
        throw std::runtime_error("Statement is not open or no result available");
    }
    
    try {
        if (result.is_null(col)) {
            return std::string();
        }
        
        return result.get<std::string>(col);
    } catch (const nanodbc::database_error& e) {
        throw std::runtime_error("Failed to get string value: " + OdbcUtils::HandleException(e));
    }
}

template <>
int OdbcStatement::GetValue(idx_t col) {
    if (!IsOpen() || !has_result) {
        throw std::runtime_error("Statement is not open or no result available");
    }
    
    try {
        if (result.is_null(col)) {
            return 0;
        }
        
        return result.get<int>(col);
    } catch (const nanodbc::database_error& e) {
        throw std::runtime_error("Failed to get int value: " + OdbcUtils::HandleException(e));
    }
}

template <>
int64_t OdbcStatement::GetValue(idx_t col) {
    if (!IsOpen() || !has_result) {
        throw std::runtime_error("Statement is not open or no result available");
    }
    
    try {
        if (result.is_null(col)) {
            return 0;
        }
        
        return result.get<int64_t>(col);
    } catch (const nanodbc::database_error& e) {
        throw std::runtime_error("Failed to get int64 value: " + OdbcUtils::HandleException(e));
    }
}

template <>
double OdbcStatement::GetValue(idx_t col) {
    if (!IsOpen() || !has_result) {
        throw std::runtime_error("Statement is not open or no result available");
    }
    
    try {
        if (result.is_null(col)) {
            return 0.0;
        }
        
        return result.get<double>(col);
    } catch (const nanodbc::database_error& e) {
        throw std::runtime_error("Failed to get double value: " + OdbcUtils::HandleException(e));
    }
}

template <>
timestamp_t OdbcStatement::GetValue(idx_t col) {
    if (!IsOpen() || !has_result) {
        throw std::runtime_error("Statement is not open or no result available");
    }
    
    try {
        if (result.is_null(col)) {
            // Return epoch for null
            return Timestamp::FromEpochSeconds(0);
        }
        
        // Get timestamp using nanodbc
        nanodbc::timestamp ts = result.get<nanodbc::timestamp>(col);
        
        // Convert to DuckDB timestamp
        date_t date = Date::FromDate(ts.year, ts.month, ts.day);
        dtime_t time = Time::FromTime(ts.hour, ts.min, ts.sec, ts.fract / 1000000);
        return Timestamp::FromDatetime(date, time);
    } catch (const nanodbc::database_error& e) {
        throw std::runtime_error("Failed to get timestamp value: " + OdbcUtils::HandleException(e));
    }
}

SQLLEN OdbcStatement::GetValueLength(idx_t col) {
    if (!IsOpen() || !has_result) {
        throw std::runtime_error("Statement is not open or no result available");
    }
    
    try {
        // With nanodbc, we need to get the actual data to know its length
        // For variable length data, use a different approach
        if (result.is_null(col)) {
            return SQL_NULL_DATA;
        }
        
        SQLSMALLINT type;
        SQLULEN column_size;
        SQLSMALLINT decimal_digits;
        OdbcUtils::GetColumnMetadata(result, col, type, column_size, decimal_digits);
        
        // For variable length data like strings and blobs
        if (OdbcUtils::IsBinaryType(type) || type == SQL_VARCHAR || type == SQL_CHAR || 
            type == SQL_WVARCHAR || type == SQL_WCHAR) {
            std::string value = result.get<std::string>(col);
            return value.length();
        }
        
        // For fixed length data, return the column size
        return column_size;
    } catch (const nanodbc::database_error& e) {
        throw std::runtime_error("Failed to get value length: " + OdbcUtils::HandleException(e));
    }
}

template <>
void OdbcStatement::Bind(idx_t col, int value) {
    if (!IsOpen()) {
        throw std::runtime_error("Statement is not open");
    }
    
    try {
        stmt.bind(col, &value);
    } catch (const nanodbc::database_error& e) {
        throw std::runtime_error("Failed to bind int parameter: " + OdbcUtils::HandleException(e));
    }
}

template <>
void OdbcStatement::Bind(idx_t col, int64_t value) {
    if (!IsOpen()) {
        throw std::runtime_error("Statement is not open");
    }
    
    try {
        stmt.bind(col, &value);
    } catch (const nanodbc::database_error& e) {
        throw std::runtime_error("Failed to bind int64 parameter: " + OdbcUtils::HandleException(e));
    }
}

template <>
void OdbcStatement::Bind(idx_t col, double value) {
    if (!IsOpen()) {
        throw std::runtime_error("Statement is not open");
    }
    
    try {
        stmt.bind(col, &value);
    } catch (const nanodbc::database_error& e) {
        throw std::runtime_error("Failed to bind double parameter: " + OdbcUtils::HandleException(e));
    }
}

template <>
void OdbcStatement::Bind(idx_t col, std::nullptr_t value) {
    if (!IsOpen()) {
        throw std::runtime_error("Statement is not open");
    }
    
    try {
        stmt.bind_null(col);
    } catch (const nanodbc::database_error& e) {
        throw std::runtime_error("Failed to bind null parameter: " + OdbcUtils::HandleException(e));
    }
}

void OdbcStatement::BindBlob(idx_t col, const string_t &value) {
    if (!IsOpen()) {
        throw std::runtime_error("Statement is not open");
    }
    
    try {
        // Bind binary data using nanodbc
        stmt.bind(col, value.GetDataUnsafe(), value.GetSize());
    } catch (const nanodbc::database_error& e) {
        throw std::runtime_error("Failed to bind blob parameter: " + OdbcUtils::HandleException(e));
    }
}

void OdbcStatement::BindText(idx_t col, const string_t &value) {
    if (!IsOpen()) {
        throw std::runtime_error("Statement is not open");
    }
    
    try {
        std::string str(value.GetDataUnsafe(), value.GetSize());
        stmt.bind(col, str.c_str());
    } catch (const nanodbc::database_error& e) {
        throw std::runtime_error("Failed to bind text parameter: " + OdbcUtils::HandleException(e));
    }
}

void OdbcStatement::BindValue(Vector &col, idx_t c, idx_t r) {
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

void OdbcStatement::CheckTypeMatches(const ODBCBindData &bind_data, SQLLEN indicator, SQLSMALLINT odbc_type, 
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
                              OdbcUtils::TypeToString(expected_type) + ", found " +
                              OdbcUtils::TypeToString(odbc_type) + " instead.";
        message += "\n* SET odbc_all_varchar=true to load all columns as VARCHAR "
                  "and skip type conversions";
        throw std::runtime_error(message);
    }
}

void OdbcStatement::CheckTypeIsFloatOrInteger(SQLSMALLINT odbc_type, idx_t col_idx) {
    if (odbc_type != SQL_FLOAT && odbc_type != SQL_DOUBLE && odbc_type != SQL_REAL &&
        odbc_type != SQL_INTEGER && odbc_type != SQL_SMALLINT && odbc_type != SQL_TINYINT && 
        odbc_type != SQL_BIGINT && odbc_type != SQL_DECIMAL && odbc_type != SQL_NUMERIC) {
        std::string column_name = GetName(col_idx);
        std::string message = "Invalid type in column \"" + column_name + "\": expected float or integer, found " +
                              OdbcUtils::TypeToString(odbc_type) + " instead.";
        message += "\n* SET odbc_all_varchar=true to load all columns as VARCHAR "
                  "and skip type conversions";
        throw std::runtime_error(message);
    }
}

} // namespace duckdb