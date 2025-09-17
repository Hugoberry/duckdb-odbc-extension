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
    if (!executed) {
        // Execute to get metadata
        result = stmt.execute();
        executed = true;
        has_result = true;
    }
    
    auto dataType = result.column_datatype(colIdx);
    if (columnSize) *columnSize = result.column_size(colIdx);
    if (decimalDigits) *decimalDigits = result.column_decimal_digits(colIdx);

    return dataType;
}

std::string OdbcStatement::GetName(idx_t colIdx) {
    if (!executed) {
        // Execute to get metadata
        result = stmt.execute();
        executed = true;
        has_result = true;
    }

    return result.column_name(colIdx);
}

idx_t OdbcStatement::GetColumnCount() {

    if (!executed) {
        // Execute to get metadata
        result = stmt.execute();
        executed = true;
        has_result = true;
    }

    return result.columns();
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
} // namespace duckdb