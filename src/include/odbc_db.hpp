#pragma once

#include "duckdb.hpp"
#include "odbc_headers.hpp"

namespace duckdb {

struct ODBCOpenOptions {
    bool read_only = true; // We'll enforce read-only mode
    std::string connection_timeout = "60"; // Default 60s timeout
    idx_t login_timeout = 30; // Default 30s login timeout
    std::vector<std::pair<std::string, std::string>> additional_connection_strings;
};

class ODBCStatement;

class ODBCDB {
public:
    ODBCDB();
    ODBCDB(SQLHENV henv, SQLHDBC hdbc);
    ~ODBCDB();

    ODBCDB(ODBCDB &&other) noexcept;
    ODBCDB &operator=(ODBCDB &&other) noexcept;

    // Forbid copying
    ODBCDB(const ODBCDB &) = delete;
    ODBCDB &operator=(const ODBCDB &) = delete;

    // Open connection using DSN
    static ODBCDB OpenWithDSN(const std::string &dsn, const std::string &username = "",
                              const std::string &password = "", const ODBCOpenOptions &options = ODBCOpenOptions());

    // Open connection using connection string
    static ODBCDB OpenWithConnectionString(const std::string &connection_string,
                                           const ODBCOpenOptions &options = ODBCOpenOptions());

    // Prepare a statement
    ODBCStatement Prepare(const std::string &query);
    bool TryPrepare(const std::string &query, ODBCStatement &stmt);

    // Execute a simple statement (no results)
    void Execute(const std::string &query);

    // Check if the connection is open
    bool IsOpen() const;

    // Close the connection
    void Close();

    // Get tables from the connection
    std::vector<std::string> GetTables();

    // Get columns for a table
    void GetTableInfo(const std::string &table_name, ColumnList &columns, 
                      std::vector<std::unique_ptr<Constraint>> &constraints, bool all_varchar = false);

    // Checks if a column exists in the specified table
    bool ColumnExists(const std::string &table_name, const std::string &column_name);

    // Get catalog entry type
    CatalogType GetEntryType(const std::string &name);

    // Debug helper
    static void DebugSetPrintQueries(bool print);

private:
    SQLHENV henv;
    SQLHDBC hdbc;
    bool owner;

    void CheckError(SQLRETURN ret, SQLSMALLINT handle_type, SQLHANDLE handle, const std::string &operation);
};

} // namespace duckdb