#pragma once

#include "duckdb.hpp"
#include "odbc_headers.hpp"

namespace duckdb {

/**
 * @brief ODBC prepared statement
 * Manages statement execution and result processing
 */
class OdbcStatement {
public:
    // Default constructor
    OdbcStatement() = default;
    
    // Create with connection and query
    OdbcStatement(nanodbc::connection& conn, const std::string& query);
    
    // Destructor
    ~OdbcStatement();
    
    // Move semantics
    OdbcStatement(OdbcStatement &&other) noexcept;
    OdbcStatement &operator=(OdbcStatement &&other) noexcept;
    
    // Forbid copying
    OdbcStatement(const OdbcStatement &) = delete;
    OdbcStatement &operator=(const OdbcStatement &) = delete;
    
    // Execute and fetch next row
    bool Step();
    
    // Reset statement for re-execution
    void Reset();
    
    // Close statement and free resources
    void Close();
    
    // Check if statement is open
    bool IsOpen() const;
    
    // Get metadata
    SQLSMALLINT GetOdbcType(idx_t colIdx, SQLULEN* columnSize = nullptr, SQLSMALLINT* decimalDigits = nullptr);
    std::string GetName(idx_t colIdx);
    idx_t GetColumnCount();
    
    // Get value from result
    bool IsNull(idx_t colIdx) const;
    std::string GetString(idx_t colIdx);
    int32_t GetInt32(idx_t colIdx);
    int64_t GetInt64(idx_t colIdx);
    double GetDouble(idx_t colIdx);
    dtime_t GetTime(idx_t colIdx);
    timestamp_t GetTimestamp(idx_t colIdx);
    
    // Make result accessible to scanner
    nanodbc::statement stmt;
    nanodbc::result result;
    
private:
    bool has_result = false;
    bool executed = false;
};

} // namespace duckdb