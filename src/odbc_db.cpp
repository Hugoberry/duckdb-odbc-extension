#include "odbc_db.hpp"
#include "odbc_stmt.hpp"
#include "odbc_utils.hpp"
#include "duckdb/parser/column_list.hpp"
#include "duckdb/parser/constraints/not_null_constraint.hpp"
#include "duckdb/parser/constraints/unique_constraint.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/parser.hpp"

namespace duckdb {

bool OdbcDB::debug_print_queries = false;

OdbcDB::OdbcDB() : owner(false) {
}

OdbcDB::~OdbcDB() {
    Close();
}

OdbcDB::OdbcDB(OdbcDB &&other) noexcept {
    conn = std::move(other.conn);
    owner = other.owner;
    other.owner = false;
}

OdbcDB &OdbcDB::operator=(OdbcDB &&other) noexcept {
    if (this != &other) {
        Close();
        conn = std::move(other.conn);
        owner = other.owner;
        other.owner = false;
    }
    return *this;
}

OdbcDB OdbcDB::OpenWithDSN(const string &dsn, const string &username, const string &password, const ODBCOpenOptions &options) {
    try {
        OdbcDB db;
        
        // Set connection timeout
        int timeout = std::stoi(options.connection_timeout);
        
        // Connect to ODBC data source
        if (username.empty() && password.empty()) {
            db.conn = nanodbc::connection(dsn, "", "", timeout);
        } else {
            db.conn = nanodbc::connection(dsn, username, password, timeout);
        }
        
        // Set read-only mode if supported
        if (options.read_only) {
            try {
                // Use native ODBC call for setting connection attributes that nanodbc doesn't expose
                SQLHDBC native_handle = db.conn.native_dbc_handle();
                SQLSetConnectAttr(native_handle, SQL_ATTR_ACCESS_MODE, (SQLPOINTER)SQL_MODE_READ_ONLY, 0);
            } catch (const std::exception& e) {
                // Just log warning, don't fail if read-only setting is not supported
                fprintf(stderr, "Warning: Failed to set read-only mode: %s\n", e.what());
            }
        }
        
        db.owner = true;
        return db;
    } catch (const nanodbc::database_error& e) {
        throw std::runtime_error("Failed to connect to DSN '" + dsn + "': " + OdbcUtils::HandleException(e));
    }
}

OdbcDB OdbcDB::OpenWithConnectionString(const string &connection_string, const ODBCOpenOptions &options) {
    try {
        OdbcDB db;
        
        // Set connection timeout
        int timeout = std::stoi(options.connection_timeout);
        
        // Connect using the connection string
        db.conn = nanodbc::connection(connection_string, timeout);
        
        // Set read-only mode if supported
        if (options.read_only) {
            try {
                // Use native ODBC call for setting connection attributes that nanodbc doesn't expose
                SQLHDBC native_handle = db.conn.native_dbc_handle();
                SQLSetConnectAttr(native_handle, SQL_ATTR_ACCESS_MODE, (SQLPOINTER)SQL_MODE_READ_ONLY, 0);
            } catch (const std::exception& e) {
                // Just log warning, don't fail if read-only setting is not supported
                fprintf(stderr, "Warning: Failed to set read-only mode: %s\n", e.what());
            }
        }
        
        db.owner = true;
        return db;
    } catch (const nanodbc::database_error& e) {
        throw std::runtime_error("Failed to connect with connection string: " + OdbcUtils::HandleException(e));
    }
}

bool OdbcDB::TryPrepare(const std::string &query, OdbcStatement &stmt) {
    if (!IsOpen()) {
        return false;
    }
    try {
        // Prepare via statement constructor
        stmt = OdbcStatement(conn, query);
        return true;
    } catch (const nanodbc::database_error &) {
        return false;
    } catch (const std::exception &) {
        return false;
    }
}

OdbcStatement OdbcDB::Prepare(const std::string &query) {
    OdbcStatement stmt;
    // First try the fast path
    if (!TryPrepare(query, stmt)) {
        // Fallback: rethrow any error to get the actual message
        try {
            stmt = OdbcStatement(conn, query);
        } catch (const nanodbc::database_error &e) {
            throw std::runtime_error(
                "Failed to prepare query \"" + query + "\": " +
                OdbcUtils::HandleException(e)
            );
        }
    }
    return stmt;
}

void OdbcDB::Execute(const string &query) {
    if (debug_print_queries) {
        printf("ODBC Query: %s\n", query.c_str());
    }
    
    try {
        nanodbc::just_execute(conn, query);
    } catch (const nanodbc::database_error& e) {
        throw std::runtime_error("Failed to execute query \"" + query + "\": " + OdbcUtils::HandleException(e));
    }
}

bool OdbcDB::IsOpen() const {
    return conn.connected();
}

void OdbcDB::Close() {
    if (!owner) {
        return;
    }
    
    if (IsOpen()) {
        try {
            conn.disconnect();
        } catch (...) {
            // Ignore exceptions during disconnect
        }
    }
    
    owner = false;
}

std::vector<std::string> OdbcDB::GetTables() {
    std::vector<std::string> tables;
    
    try {
        // Use nanodbc's catalog functions to get tables
        nanodbc::catalog catalog(conn);
        auto table_results = catalog.find_tables(std::string(), std::string("TABLE"), std::string(), std::string());
        
        while (table_results.next()) {
            // Use the table_name() method instead of get<std::string>()
            std::string table_name = table_results.table_name();
            tables.push_back(table_name);
        }
    } catch (const nanodbc::database_error& e) {
        throw std::runtime_error("Failed to get table list: " + OdbcUtils::HandleException(e));
    }
    
    return tables;
}

void OdbcDB::GetTableInfo(const std::string &table_name, ColumnList &columns, 
                           std::vector<std::unique_ptr<Constraint>> &constraints, bool all_varchar) {
    try {
        // Get column information using nanodbc catalog
        nanodbc::catalog catalog(conn);
        auto column_results = catalog.find_columns(std::string(), table_name, std::string(), std::string());
        
        idx_t column_index = 0;
        std::vector<idx_t> not_null_columns;
        
        while (column_results.next()) {
            std::string name = column_results.column_name();
            SQLSMALLINT data_type = column_results.data_type();
            SQLULEN column_size = column_results.column_size();
            SQLSMALLINT decimal_digits = column_results.decimal_digits();
            SQLSMALLINT nullable = column_results.nullable();
            
            LogicalType column_type;
            
            if (all_varchar) {
                column_type = LogicalType::VARCHAR;
            } else {
                column_type = OdbcUtils::TypeToLogicalType(data_type, column_size, decimal_digits);
            }
            
            ColumnDefinition column(std::move(name), column_type);
            columns.AddColumn(std::move(column));
            
            if (nullable == SQL_NO_NULLS) {
                constraints.push_back(make_uniq<NotNullConstraint>(LogicalIndex(column_index)));
            }
            
            column_index++;
        }
        
        if (column_index == 0) {
            throw std::runtime_error("No columns found for table '" + table_name + "'");
        }
        
        // Get primary key information
        auto pk_results = catalog.find_primary_keys(table_name, std::string(), std::string());
        std::vector<std::string> primary_keys;
        
        while (pk_results.next()) {
            // Use column_name() method instead of get<std::string>()
            std::string pk_name = pk_results.column_name();
            primary_keys.push_back(pk_name);
        }
        
        if (!primary_keys.empty()) {
            if (primary_keys.size() == 1) {
                // Find the column index of this primary key
                for (idx_t i = 0; i < columns.LogicalColumnCount(); i++) {
                    if (columns.GetColumn(LogicalIndex(i)).GetName() == primary_keys[0]) {
                        constraints.push_back(make_uniq<UniqueConstraint>(LogicalIndex(i), true));
                        break;
                    }
                }
            } else {
                constraints.push_back(make_uniq<UniqueConstraint>(std::move(primary_keys), true));
            }
        }
    } catch (const nanodbc::database_error& e) {
        throw std::runtime_error("Failed to get table info for '" + table_name + "': " + OdbcUtils::HandleException(e));
    }
}

bool OdbcDB::ColumnExists(const std::string &table_name, const std::string &column_name) {
    try {
        nanodbc::catalog catalog(conn);
        auto column_results = catalog.find_columns(column_name, table_name, std::string(), std::string());
        
        return column_results.next();
    } catch (const nanodbc::database_error&) {
        return false;
    }
}

CatalogType OdbcDB::GetEntryType(const std::string &name) {
    try {
        nanodbc::catalog catalog(conn);
        
        // Check if it's a table
        auto table_results = catalog.find_tables(name, std::string("TABLE"), std::string(), std::string());
        if (table_results.next()) {
            return CatalogType::TABLE_ENTRY;
        }
        
        // Check if it's a view
        auto view_results = catalog.find_tables(name, std::string("VIEW"), std::string(), std::string());
        if (view_results.next()) {
            return CatalogType::VIEW_ENTRY;
        }
    } catch (const nanodbc::database_error&) {
        // Ignore exceptions during type check
    }
    
    return CatalogType::INVALID;
}

void OdbcDB::DebugSetPrintQueries(bool print) {
    debug_print_queries = print;
}

} // namespace duckdb