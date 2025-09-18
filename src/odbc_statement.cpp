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

} // namespace duckdb