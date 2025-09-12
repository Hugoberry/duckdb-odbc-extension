#pragma once

#include "duckdb.hpp"
#include "odbc_headers.hpp"
#include <memory>

namespace duckdb {

// Forward declarations
class OdbcStatement;

/**
 * @brief Unified connection parameters
 * Handles both DSN and connection string approaches
 */
class ConnectionParams {
public:
    // Default constructor
    ConnectionParams() : timeout(60), read_only(true), is_dsn(false) {}
    
    // Create with either DSN or connection string
    ConnectionParams(std::string connection_info, 
                   std::string username = "",
                   std::string password = "",
                   int timeout = 60,
                   bool read_only = true);
    
    // Check if we have valid connection information
    bool IsValid() const;
    
    // Get the connection string to use
    std::string GetConnectionString() const;
    
    // Getters
    const std::string& GetDsn() const { return dsn; }
    const std::string& GetUsername() const { return username; }
    const std::string& GetPassword() const { return password; }
    int GetTimeout() const { return timeout; }
    bool IsReadOnly() const { return read_only; }
    
private:
    std::string dsn;
    std::string connection_string;
    std::string username;
    std::string password;
    int timeout;
    bool read_only;
    bool is_dsn;
};

/**
 * @brief ODBC database connection
 * Manages connection to ODBC data sources
 */
class OdbcConnection {
public:
    // Default constructor
    OdbcConnection() = default;
    
    // Destructor
    ~OdbcConnection();
    
    // Move semantics
    OdbcConnection(OdbcConnection &&other) noexcept;
    OdbcConnection &operator=(OdbcConnection &&other) noexcept;
    
    // Forbid copying
    OdbcConnection(const OdbcConnection &) = delete;
    OdbcConnection &operator=(const OdbcConnection &) = delete;
    
    // Connect to ODBC data source
    static unique_ptr<OdbcConnection> Connect(const ConnectionParams& params);
    
    // Prepare a statement
    unique_ptr<OdbcStatement> Prepare(const std::string &query);
    
    // Execute a simple statement (no results)
    void Execute(const std::string &query);
    
    // Check if the connection is open
    bool IsOpen() const;
    
    // Get tables from the connection
    std::vector<std::string> GetTables();
    std::vector<std::string> GetViews();
    
    // Get columns for a table
    void GetTableInfo(const std::string &tableName, const std::string &schemaName, ColumnList &columns, 
                     std::vector<std::unique_ptr<Constraint>> &constraints, bool allVarchar = false);
    
    // Get raw nanodbc connection (for advanced usage)
    nanodbc::connection& GetNativeConnection() { return connection; }
    const nanodbc::connection& GetNativeConnection() const { return connection; }
    
private:
    nanodbc::connection connection;
};

} // namespace duckdb