#pragma once

#include "duckdb.hpp"
#include "odbc_db.hpp"
#include "odbc_headers.hpp"

namespace duckdb {

struct ODBCBindData;

class ODBCStatement {
public:
    ODBCStatement();
    ODBCStatement(SQLHDBC hdbc, SQLHSTMT hstmt);
    ~ODBCStatement();

    ODBCStatement(ODBCStatement &&other) noexcept;
    ODBCStatement &operator=(ODBCStatement &&other) noexcept;

    // Forbid copying
    ODBCStatement(const ODBCStatement &) = delete;
    ODBCStatement &operator=(const ODBCStatement &) = delete;

    // Execute and fetch next row
    bool Step();
    
    // Reset statement for re-execution
    void Reset();
    
    // Close statement
    void Close();
    
    // Check if statement is open
    bool IsOpen();
    
    // Get the ODBC type of a column
    SQLSMALLINT GetODBCType(idx_t col, SQLULEN* column_size = nullptr, SQLSMALLINT* decimal_digits = nullptr);;
    
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

    // Internal ODBC statement handle
    SQLHDBC hdbc;
    SQLHSTMT hstmt;
    
private:
    void CheckError(SQLRETURN ret, const std::string &operation);
};

} // namespace duckdb