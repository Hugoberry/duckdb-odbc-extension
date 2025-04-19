#pragma once

#include "duckdb.hpp"
#include "odbc_headers.hpp"

namespace duckdb {

// Keep the same options structure for compatibility
struct ODBCOpenOptions {
    bool read_only = true; // We'll enforce read-only mode
    std::string connection_timeout = "60"; // Default 60s timeout
};

// Forward declaration
class OdbcStatement;

class OdbcDB {
public:
    OdbcDB();
    ~OdbcDB();

    OdbcDB(OdbcDB &&other) noexcept;
    OdbcDB &operator=(OdbcDB &&other) noexcept;

    // Forbid copying
    OdbcDB(const OdbcDB &) = delete;
    OdbcDB &operator=(const OdbcDB &) = delete;

    // Open connection using DSN
    static OdbcDB OpenWithDSN(const std::string &dsn, const std::string &username = "",
                              const std::string &password = "", const ODBCOpenOptions &options = ODBCOpenOptions());

    // Open connection using connection string
    static OdbcDB OpenWithConnectionString(const std::string &connection_string,
                                           const ODBCOpenOptions &options = ODBCOpenOptions());

    // Prepare a statement
    OdbcStatement Prepare(const std::string &query);
    bool TryPrepare(const std::string &query, OdbcStatement &stmt);

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

    // Get raw nanodbc connection (for advanced usage)
    nanodbc::connection& GetConnection() { return conn; }
    const nanodbc::connection& GetConnection() const { return conn; }

private:
    nanodbc::connection conn;
    bool owner;
    static bool debug_print_queries;
};

// Define an alias for backward compatibility
using ODBCDB = OdbcDB;

} // namespace duckdb