#pragma once

#include "duckdb.hpp"
#include "odbc_db.hpp"
#include "odbc_stmt.hpp"
#include "odbc_headers.hpp"

namespace duckdb {

struct ODBCBindData : public TableFunctionData {
    std::string connection_string;
    std::string dsn;
    std::string username;
    std::string password;
    std::string table_name;
    std::string sql;
    std::vector<std::string> names;
    std::vector<LogicalType> types;
    bool all_varchar = false;
    ODBCDB *global_db = nullptr;
    TableCatalogEntry *table = nullptr;
};

struct ODBCLocalState : public LocalTableFunctionState {
    ODBCDB *db;
    ODBCDB owned_db;
    ODBCStatement stmt;
    bool done = false;
    std::vector<column_t> column_ids;
    idx_t scan_count = 0;
};

struct ODBCGlobalState : public GlobalTableFunctionState {
    explicit ODBCGlobalState(idx_t max_threads) : max_threads(max_threads) {}

    std::mutex lock;
    idx_t position = 0;
    idx_t max_threads;

    idx_t MaxThreads() const override {
        return max_threads;
    }
};

class ODBCScanFunction : public TableFunction {
public:
    ODBCScanFunction();
};

class ODBCAttachFunction : public TableFunction {
public:
    ODBCAttachFunction();
};

class ODBCQueryFunction : public TableFunction {
public:
    ODBCQueryFunction();
};

class ODBCDriversFunction : public TableFunction {
public:
    ODBCDriversFunction();
};
    
LogicalType GetDuckDBType(SQLSMALLINT odbc_type, SQLULEN column_size, SQLSMALLINT decimal_digits);
int GetODBCSQLType(const LogicalType &type);

} // namespace duckdb