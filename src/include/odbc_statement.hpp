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
    
    // Close statement and free resources
    void Close();
    
    // Check if statement is open
    bool IsOpen() const;
    
    // Get metadata
    SQLSMALLINT GetOdbcType(idx_t colIdx, SQLULEN* columnSize = nullptr, SQLSMALLINT* decimalDigits = nullptr);
    std::string GetName(idx_t colIdx);
    idx_t GetColumnCount();
    
    // Make result accessible to scanner
    nanodbc::statement stmt;
    nanodbc::result result;
    
private:
    bool has_result = false;
    bool executed = false;
};

} // namespace duckdb