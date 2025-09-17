#include "odbc_statement.hpp"
#include "odbc_utils.hpp"
#include "duckdb/common/types/date.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/common/types/time.hpp"
#include "duckdb/common/windows_util.hpp"

namespace duckdb {

OdbcStatement::OdbcStatement(nanodbc::connection &conn, const std::string &query) : has_result(false), executed(false) {
    try {
        // Prepare the statement
        stmt = nanodbc::statement(conn, query);
    } catch (const nanodbc::database_error &e) {
        OdbcUtils::ThrowException("prepare statement", e);
    }
}

OdbcStatement::~OdbcStatement() {
    Close();
}

OdbcStatement::OdbcStatement(OdbcStatement &&other) noexcept
    : stmt(std::move(other.stmt))
    , result(std::move(other.result))
    , has_result(other.has_result)
    , executed(other.executed) {
    // Reset the moved-from instance
    other.has_result = false;
    other.executed = false;
}

OdbcStatement &OdbcStatement::operator=(OdbcStatement &&other) noexcept {
    if (this != &other) {
        // Clean up current handles
        Close();
        // Move in the new handles
        stmt = std::move(other.stmt);
        result = std::move(other.result);
        has_result = other.has_result;
        executed = other.executed;
        // Reset the moved-from object
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
        // On the first call, execute; on subsequent calls, advance the cursor
        if (!executed) {
            result = stmt.execute();
            executed = true;
            has_result = true;
        }
        
        // The first call to next() moves to the first row
        return result.next();
    } catch (const nanodbc::database_error &e) {
        OdbcUtils::ThrowException("execute statement", e);
        return false; // Won't reach here due to exception
    }
}

void OdbcStatement::Reset() {
    if (IsOpen()) {
        try {
            stmt.close();
            has_result = false;
            executed = false;
        } catch (const nanodbc::database_error& e) {
            OdbcUtils::ThrowException("reset statement", e);
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

bool OdbcStatement::IsOpen() const {
    return stmt.connected();
}

SQLSMALLINT OdbcStatement::GetOdbcType(idx_t colIdx, SQLULEN* columnSize, SQLSMALLINT* decimalDigits) {
    if (!IsOpen()) {
        throw BinderException("Statement is not open");
    }
    
    try {
        if (!executed) {
            // Execute to get metadata
            result = stmt.execute();
            executed = true;
            has_result = true;
        }
        
        SQLSMALLINT dataType;
        SQLULEN size;
        SQLSMALLINT digits;
        
        // Get column metadata
        OdbcUtils::GetColumnMetadata(result, colIdx, dataType, size, digits);
        
        // Pass back column size and decimal digits if requested
        if (columnSize) *columnSize = size;
        if (decimalDigits) *decimalDigits = digits;
        
        return dataType;
    } catch (const nanodbc::database_error& e) {
        OdbcUtils::ThrowException("get column type", e);
        return SQL_UNKNOWN_TYPE; // Won't reach here due to exception
    }
}

std::string OdbcStatement::GetName(idx_t colIdx) {
    if (!IsOpen()) {
        throw BinderException("Statement is not open");
    }

    try {
        if (!executed) {
            // Execute to get metadata
            result = stmt.execute();
            executed = true;
            has_result = true;
        }

        return result.column_name(colIdx);
    } catch (const nanodbc::database_error& e) {
        OdbcUtils::ThrowException("get column name", e);
        return {};
    }
}

idx_t OdbcStatement::GetColumnCount() {
    if (!IsOpen()) {
        throw BinderException("Statement is not open");
    }

    try {
        if (!executed) {
            // Execute to get metadata
            result = stmt.execute();
            executed = true;
            has_result = true;
        }

        return result.columns();
    } catch (const nanodbc::database_error& e) {
        OdbcUtils::ThrowException("get column count", e);
        return 0;
    }
}

bool OdbcStatement::IsNull(idx_t colIdx) const {
    return result.is_null(colIdx);
}

std::string OdbcStatement::GetString(idx_t colIdx) {
    return result.get<std::string>((short)colIdx, std::string());
}

int32_t OdbcStatement::GetInt32(idx_t colIdx) {
    return result.get<int32_t>((short)colIdx, 0);
}

int64_t OdbcStatement::GetInt64(idx_t colIdx) {
    return result.get<int64_t>((short)colIdx, 0);
}

double OdbcStatement::GetDouble(idx_t colIdx) {
    return result.get<double>((short)colIdx, 0.0);
}

dtime_t OdbcStatement::GetTime(idx_t colIdx) {
    if (!has_result) {
        throw BinderException("No result available");
    }
    
    try {
        if (result.is_null(colIdx)) {
            // Return midnight for null
            return Time::FromTime(0, 0, 0, 0);
        }
        
        // Get timestamp using nanodbc
        nanodbc::time ts = result.get<nanodbc::time>(colIdx);

        // Convert to DuckDB timestamp
        return Time::FromTime(ts.hour, ts.min, ts.sec);
    } catch (const nanodbc::database_error& e) {
        OdbcUtils::ThrowException("get timestamp value", e);
        return Time::FromTime(0, 0, 0, 0); // Won't reach here due to exception
    }
}

timestamp_t OdbcStatement::GetTimestamp(idx_t colIdx) {
    if (!has_result) {
        throw BinderException("No result available");
    }
    
    try {
        if (result.is_null(colIdx)) {
            // Return epoch for null
            return Timestamp::FromEpochSeconds(0);
        }
        
        // Get timestamp using nanodbc
        nanodbc::timestamp ts = result.get<nanodbc::timestamp>(colIdx);
        
        // Convert to DuckDB timestamp
        date_t date = Date::FromDate(ts.year, ts.month, ts.day);
        dtime_t time = Time::FromTime(ts.hour, ts.min, ts.sec, ts.fract / 1000000);
        return Timestamp::FromDatetime(date, time);
    } catch (const nanodbc::database_error& e) {
        OdbcUtils::ThrowException("get timestamp value", e);
        return Timestamp::FromEpochSeconds(0); // Won't reach here due to exception
    }
}

void OdbcStatement::BindNull(idx_t colIdx) {
    if (!IsOpen()) {
        throw BinderException("Statement is not open");
    }
    
    try {
        stmt.bind_null(colIdx);
    } catch (const nanodbc::database_error& e) {
        OdbcUtils::ThrowException("bind null parameter", e);
    }
}

void OdbcStatement::BindInt32(idx_t colIdx, int32_t value) {
    if (!IsOpen()) {
        throw BinderException("Statement is not open");
    }
    
    try {
        stmt.bind(colIdx, &value);
    } catch (const nanodbc::database_error& e) {
        OdbcUtils::ThrowException("bind int32 parameter", e);
    }
}

void OdbcStatement::BindInt64(idx_t colIdx, int64_t value) {
    if (!IsOpen()) {
        throw BinderException("Statement is not open");
    }
    
    try {
        stmt.bind(colIdx, &value);
    } catch (const nanodbc::database_error& e) {
        OdbcUtils::ThrowException("bind int64 parameter", e);
    }
}

void OdbcStatement::BindDouble(idx_t colIdx, double value) {
    if (!IsOpen()) {
        throw BinderException("Statement is not open");
    }
    
    try {
        stmt.bind(colIdx, &value);
    } catch (const nanodbc::database_error& e) {
        OdbcUtils::ThrowException("bind double parameter", e);
    }
}

void OdbcStatement::BindString(idx_t colIdx, const std::string& value) {
    if (!IsOpen()) {
        throw BinderException("Statement is not open");
    }
    
    try {
        stmt.bind(colIdx, value.c_str());
    } catch (const nanodbc::database_error& e) {
        OdbcUtils::ThrowException("bind string parameter", e);
    }
}

void OdbcStatement::BindBlob(idx_t colIdx, const char* data, size_t size) {
    if (!IsOpen()) {
        throw BinderException("Statement is not open");
    }
    
    try {
        stmt.bind(colIdx, data, size);
    } catch (const nanodbc::database_error& e) {
        OdbcUtils::ThrowException("bind blob parameter", e);
    }
}

void OdbcStatement::BindValue(Vector &col, idx_t colIdx, idx_t rowIdx) {
    auto &mask = FlatVector::Validity(col);
    if (!mask.RowIsValid(rowIdx)) {
        BindNull(colIdx);
        return;
    }
    
    switch (col.GetType().id()) {
        case LogicalTypeId::BOOLEAN:
            BindInt32(colIdx, FlatVector::GetData<bool>(col)[rowIdx] ? 1 : 0);
            break;
        case LogicalTypeId::TINYINT:
            BindInt32(colIdx, FlatVector::GetData<int8_t>(col)[rowIdx]);
            break;
        case LogicalTypeId::SMALLINT:
            BindInt32(colIdx, FlatVector::GetData<int16_t>(col)[rowIdx]);
            break;
        case LogicalTypeId::INTEGER:
            BindInt32(colIdx, FlatVector::GetData<int32_t>(col)[rowIdx]);
            break;
        case LogicalTypeId::BIGINT:
            BindInt64(colIdx, FlatVector::GetData<int64_t>(col)[rowIdx]);
            break;
        case LogicalTypeId::FLOAT:
            BindDouble(colIdx, FlatVector::GetData<float>(col)[rowIdx]);
            break;
        case LogicalTypeId::DOUBLE:
            BindDouble(colIdx, FlatVector::GetData<double>(col)[rowIdx]);
            break;
        case LogicalTypeId::VARCHAR: {
            auto str = FlatVector::GetData<string_t>(col)[rowIdx];
            BindString(colIdx, std::string(str.GetString(), str.GetSize()));
            break;
        }
        case LogicalTypeId::BLOB: {
            auto blob = FlatVector::GetData<string_t>(col)[rowIdx];
            BindBlob(colIdx, blob.GetDataUnsafe(), blob.GetSize());
            break;
        }
        default:
            throw BinderException("Unsupported type for binding: " + col.GetType().ToString());
    }
}

} // namespace duckdb