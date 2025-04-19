#pragma once

#include "duckdb.hpp"
#include "odbc_db.hpp"
#include "odbc_headers.hpp"

namespace duckdb {

struct ODBCBindData;

class OdbcStatement {
public:
    OdbcStatement();
    OdbcStatement(nanodbc::connection& conn, const std::string& query);
    ~OdbcStatement();

    OdbcStatement(OdbcStatement &&other) noexcept;
    OdbcStatement &operator=(OdbcStatement &&other) noexcept;

    // Forbid copying
    OdbcStatement(const OdbcStatement &) = delete;
    OdbcStatement &operator=(const OdbcStatement &) = delete;

    // Execute and fetch next row
    bool Step();
    
    // Reset statement for re-execution
    void Reset();
    
    // Close statement
    void Close();
    
    // Check if statement is open
    bool IsOpen();
    
    // Get the ODBC type of a column
    SQLSMALLINT GetODBCType(idx_t col, SQLULEN* column_size = nullptr, SQLSMALLINT* decimal_digits = nullptr);
    
    // Get the DuckDB type of a column
    int GetType(idx_t col);
    
    // Get column name
    std::string GetName(idx_t col);
    
    // Get number of columns
    idx_t GetColumnCount();
    
    // Get value from result set
    template <typename T>
    T GetValue(idx_t col);
    
    // Get length/indicator value
    SQLLEN GetValueLength(idx_t col);
    
    // Bind parameter
    template <typename T>
    void Bind(idx_t col, T value);
    
    // Bind blob parameter
    void BindBlob(idx_t col, const string_t &value);
    
    // Bind text parameter
    void BindText(idx_t col, const string_t &value);
    
    // Bind a value from a vector
    void BindValue(Vector &col, idx_t c, idx_t r);
    
    // Type checking functions
    void CheckTypeMatches(const ODBCBindData &bind_data, SQLLEN indicator, SQLSMALLINT odbc_type, 
                         SQLSMALLINT expected_type, idx_t col_idx);
    
    void CheckTypeIsFloatOrInteger(SQLSMALLINT odbc_type, idx_t col_idx);

    // Internal statement and result handles
    nanodbc::statement stmt;
    nanodbc::result result;
    
private:
    bool has_result = false;
    bool executed = false;
};

// Define an alias for backward compatibility
using ODBCStatement = OdbcStatement;

} // namespace duckdb