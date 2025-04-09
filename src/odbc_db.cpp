#include "odbc_db.hpp"
#include "odbc_stmt.hpp"
#include "odbc_utils.hpp"
#include "duckdb/parser/column_list.hpp"
#include "duckdb/parser/constraints/not_null_constraint.hpp"
#include "duckdb/parser/constraints/unique_constraint.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/parser.hpp"

namespace duckdb {

static bool debug_odbc_print_queries = false;

ODBCDB::ODBCDB() : henv(nullptr), hdbc(nullptr), owner(false) {
}

ODBCDB::ODBCDB(SQLHENV henv, SQLHDBC hdbc) : henv(henv), hdbc(hdbc), owner(true) {
}

ODBCDB::~ODBCDB() {
    Close();
}

ODBCDB::ODBCDB(ODBCDB &&other) noexcept {
    henv = other.henv;
    hdbc = other.hdbc;
    owner = other.owner;
    other.henv = nullptr;
    other.hdbc = nullptr;
    other.owner = false;
}

ODBCDB &ODBCDB::operator=(ODBCDB &&other) noexcept {
    if (this != &other) {
        Close();
        henv = other.henv;
        hdbc = other.hdbc;
        owner = other.owner;
        other.henv = nullptr;
        other.hdbc = nullptr;
        other.owner = false;
    }
    return *this;
}

ODBCDB ODBCDB::OpenWithDSN(const string &dsn, const string &username, const string &password, const ODBCOpenOptions &options) {
    SQLHENV henv;
    SQLHDBC hdbc;
    SQLRETURN ret;

    // Allocate environment handle
    ret = SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &henv);
    if (ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO) {
        throw std::runtime_error("Failed to allocate ODBC environment handle");
    }

    // Set ODBC version
    ret = SQLSetEnvAttr(henv, SQL_ATTR_ODBC_VERSION, (SQLPOINTER)SQL_OV_ODBC3, 0);
    if (ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO) {
        SQLFreeHandle(SQL_HANDLE_ENV, henv);
        throw std::runtime_error("Failed to set ODBC version");
    }

    // Allocate connection handle
    ret = SQLAllocHandle(SQL_HANDLE_DBC, henv, &hdbc);
    if (ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO) {
        SQLFreeHandle(SQL_HANDLE_ENV, henv);
        throw std::runtime_error("Failed to allocate ODBC connection handle");
    }

    // Set connection timeout
    int timeout = std::stoi(options.connection_timeout);
    ret = SQLSetConnectAttr(hdbc, SQL_ATTR_CONNECTION_TIMEOUT, (SQLPOINTER)(intptr_t)timeout, 0);
    if (ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO) {
        // Just log warning, don't fail if timeout setting is not supported
        fprintf(stderr, "Warning: Failed to set connection timeout\n");
    }

    // Set read-only mode if supported by the driver
    if (options.read_only) {
        ret = SQLSetConnectAttr(hdbc, SQL_ATTR_ACCESS_MODE, (SQLPOINTER)SQL_MODE_READ_ONLY, 0);
        if (ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO) {
            // Just log warning, don't fail if read-only setting is not supported
            fprintf(stderr, "Warning: Failed to set read-only mode\n");
        }
    }
    
    // Set login timeout if specified
    if (options.login_timeout > 0) {
        ret = SQLSetConnectAttr(hdbc, SQL_ATTR_LOGIN_TIMEOUT, (SQLPOINTER)(intptr_t)options.login_timeout, 0);
        if (ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO) {
            // Just log warning, don't fail if login timeout setting is not supported
            fprintf(stderr, "Warning: Failed to set login timeout\n");
        }
    }

    // Connect to the data source
    ret = SQLConnect(hdbc, 
                    (SQLCHAR*)dsn.c_str(), SQL_NTS,
                    (SQLCHAR*)(username.empty() ? nullptr : username.c_str()), SQL_NTS,
                    (SQLCHAR*)(password.empty() ? nullptr : password.c_str()), SQL_NTS);
    
    if (ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO) {
        std::string error = ODBCUtils::GetErrorMessage(SQL_HANDLE_DBC, hdbc);
        SQLFreeHandle(SQL_HANDLE_DBC, hdbc);
        SQLFreeHandle(SQL_HANDLE_ENV, henv);
        throw std::runtime_error("Failed to connect to DSN '" + dsn + "': " + error);
    }
    
    // Apply additional connection attributes if specified
    if (!options.additional_connection_strings.empty()) {
        for (auto &attr_pair : options.additional_connection_strings) {
            // This code just as an example - would need to be implemented correctly
            // based on the specific attributes needed
            fprintf(stderr, "Note: Additional connection attributes not fully implemented\n");
        }
    }

    return ODBCDB(henv, hdbc);
}

ODBCDB ODBCDB::OpenWithConnectionString(const string &connection_string, const ODBCOpenOptions &options) {
    SQLHENV henv;
    SQLHDBC hdbc;
    SQLRETURN ret;

    // Allocate environment handle
    ret = SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &henv);
    if (ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO) {
        throw std::runtime_error("Failed to allocate ODBC environment handle");
    }

    // Set ODBC version
    ret = SQLSetEnvAttr(henv, SQL_ATTR_ODBC_VERSION, (SQLPOINTER)SQL_OV_ODBC3, 0);
    if (ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO) {
        SQLFreeHandle(SQL_HANDLE_ENV, henv);
        throw std::runtime_error("Failed to set ODBC version");
    }

    // Allocate connection handle
    ret = SQLAllocHandle(SQL_HANDLE_DBC, henv, &hdbc);
    if (ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO) {
        SQLFreeHandle(SQL_HANDLE_ENV, henv);
        throw std::runtime_error("Failed to allocate ODBC connection handle");
    }

    // Set connection timeout
    int timeout = std::stoi(options.connection_timeout);
    ret = SQLSetConnectAttr(hdbc, SQL_ATTR_CONNECTION_TIMEOUT, (SQLPOINTER)(intptr_t)timeout, 0);
    if (ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO) {
        // Just log warning, don't fail if timeout setting is not supported
        fprintf(stderr, "Warning: Failed to set connection timeout\n");
    }

    // Set read-only mode if supported by the driver
    if (options.read_only) {
        ret = SQLSetConnectAttr(hdbc, SQL_ATTR_ACCESS_MODE, (SQLPOINTER)SQL_MODE_READ_ONLY, 0);
        if (ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO) {
            // Just log warning, don't fail if read-only setting is not supported
            fprintf(stderr, "Warning: Failed to set read-only mode\n");
        }
    }
    
    // Set login timeout if specified
    if (options.login_timeout > 0) {
        ret = SQLSetConnectAttr(hdbc, SQL_ATTR_LOGIN_TIMEOUT, (SQLPOINTER)(intptr_t)options.login_timeout, 0);
        if (ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO) {
            // Just log warning, don't fail if login timeout setting is not supported
            fprintf(stderr, "Warning: Failed to set login timeout\n");
        }
    }

    // Connect using the connection string
    SQLCHAR outstr[1024];
    SQLSMALLINT outstrlen;
    
    ret = SQLDriverConnect(hdbc, 
                          NULL, 
                          (SQLCHAR*)connection_string.c_str(), SQL_NTS,
                          outstr, sizeof(outstr), &outstrlen,
                          SQL_DRIVER_NOPROMPT);
    
    if (ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO) {
        std::string error = ODBCUtils::GetErrorMessage(SQL_HANDLE_DBC, hdbc);
        SQLFreeHandle(SQL_HANDLE_DBC, hdbc);
        SQLFreeHandle(SQL_HANDLE_ENV, henv);
        throw std::runtime_error("Failed to connect with connection string: " + error);
    }

    return ODBCDB(henv, hdbc);
}

bool ODBCDB::TryPrepare(const string &query, ODBCStatement &stmt) {
    if (!IsOpen()) {
        return false;
    }
    
    stmt.hdbc = hdbc;
    if (debug_odbc_print_queries) {
        printf("ODBC Query: %s\n", query.c_str());
    }
    
    SQLHSTMT hstmt;
    SQLRETURN ret = SQLAllocHandle(SQL_HANDLE_STMT, hdbc, &hstmt);
    if (ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO) {
        return false;
    }
    
    // Set prefetch size to improve performance
    SQLULEN prefetch_rows = 1000; // Adjust based on memory constraints and row size
    ret = SQLSetStmtAttr(hstmt, SQL_ATTR_ROW_ARRAY_SIZE, (SQLPOINTER)prefetch_rows, 0);
    if (ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO) {
        // Just log warning, don't fail if prefetch setting is not supported
        fprintf(stderr, "Warning: Failed to set prefetch size\n");
    }
    
    ret = SQLPrepare(hstmt, (SQLCHAR*)query.c_str(), SQL_NTS);
    if (ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO) {
        SQLFreeHandle(SQL_HANDLE_STMT, hstmt);
        return false;
    }
    
    stmt.hstmt = hstmt;
    return true;
}

ODBCStatement ODBCDB::Prepare(const string &query) {
    ODBCStatement stmt;
    if (!TryPrepare(query, stmt)) {
        std::string error = ODBCUtils::GetErrorMessage(SQL_HANDLE_DBC, hdbc);
        throw std::runtime_error("Failed to prepare query \"" + query + "\": " + error);
    }
    return stmt;
}

void ODBCDB::Execute(const string &query) {
    if (debug_odbc_print_queries) {
        printf("ODBC Query: %s\n", query.c_str());
    }
    
    ODBCStatement stmt = Prepare(query);
    SQLRETURN ret = SQLExecute(stmt.hstmt);
    
    if (ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO) {
        std::string error = ODBCUtils::GetErrorMessage(SQL_HANDLE_STMT, stmt.hstmt);
        throw std::runtime_error("Failed to execute query \"" + query + "\": " + error);
    }
}

bool ODBCDB::IsOpen() const {
    return hdbc != nullptr;
}

void ODBCDB::Close() {
    if (!owner) {
        return;
    }
    
    if (hdbc) {
        SQLDisconnect(hdbc);
        SQLFreeHandle(SQL_HANDLE_DBC, hdbc);
        hdbc = nullptr;
    }
    
    if (henv) {
        SQLFreeHandle(SQL_HANDLE_ENV, henv);
        henv = nullptr;
    }
    
    owner = false;
}

std::vector<std::string> ODBCDB::GetTables() {
    std::vector<std::string> tables;
    
    SQLHSTMT hstmt;
    SQLRETURN ret = SQLAllocHandle(SQL_HANDLE_STMT, hdbc, &hstmt);
    if (ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO) {
        throw std::runtime_error("Failed to allocate statement handle for listing tables");
    }
    
    // Get list of tables
    ret = SQLTables(hstmt, NULL, 0, NULL, 0, NULL, 0, (SQLCHAR*)"TABLE", SQL_NTS);
    if (ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO) {
        std::string error = ODBCUtils::GetErrorMessage(SQL_HANDLE_STMT, hstmt);
        SQLFreeHandle(SQL_HANDLE_STMT, hstmt);
        throw std::runtime_error("Failed to get table list: " + error);
    }
    
    // Bind columns for table name
    SQLCHAR table_name[256];
    SQLLEN table_name_len;
    
    ret = SQLBindCol(hstmt, 3, SQL_C_CHAR, table_name, sizeof(table_name), &table_name_len);
    if (ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO) {
        std::string error = ODBCUtils::GetErrorMessage(SQL_HANDLE_STMT, hstmt);
        SQLFreeHandle(SQL_HANDLE_STMT, hstmt);
        throw std::runtime_error("Failed to bind columns: " + error);
    }
    
    // Fetch results
    while (SQLFetch(hstmt) == SQL_SUCCESS) {
        std::string name((char*)table_name);
        tables.push_back(name);
    }
    
    SQLFreeHandle(SQL_HANDLE_STMT, hstmt);
    return tables;
}

void ODBCDB::GetTableInfo(const std::string &table_name, ColumnList &columns, 
                         std::vector<std::unique_ptr<Constraint>> &constraints, bool all_varchar) {
    SQLHSTMT hstmt;
    SQLRETURN ret = SQLAllocHandle(SQL_HANDLE_STMT, hdbc, &hstmt);
    if (ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO) {
        throw std::runtime_error("Failed to allocate statement handle for table info");
    }
    
    // Get column information
    ret = SQLColumns(hstmt, NULL, 0, NULL, 0, (SQLCHAR*)table_name.c_str(), SQL_NTS, NULL, 0);
    if (ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO) {
        std::string error = ODBCUtils::GetErrorMessage(SQL_HANDLE_STMT, hstmt);
        SQLFreeHandle(SQL_HANDLE_STMT, hstmt);
        throw std::runtime_error("Failed to get column information for table '" + table_name + "': " + error);
    }
    
    // Bind columns for column attributes
    SQLCHAR column_name[256];
    SQLLEN column_name_len;
    SQLSMALLINT data_type;
    SQLLEN data_type_len;
    SQLULEN column_size;
    SQLLEN column_size_len;
    SQLSMALLINT decimal_digits;
    SQLLEN decimal_digits_len;
    SQLSMALLINT nullable;
    SQLLEN nullable_len;
    SQLCHAR type_name[256];
    SQLLEN type_name_len;
    
    ret = SQLBindCol(hstmt, 4, SQL_C_CHAR, column_name, sizeof(column_name), &column_name_len);
    ret = SQLBindCol(hstmt, 5, SQL_C_SHORT, &data_type, 0, &data_type_len);
    ret = SQLBindCol(hstmt, 6, SQL_C_CHAR, type_name, sizeof(type_name), &type_name_len);
    ret = SQLBindCol(hstmt, 7, SQL_C_ULONG, &column_size, 0, &column_size_len);
    ret = SQLBindCol(hstmt, 9, SQL_C_SHORT, &decimal_digits, 0, &decimal_digits_len);
    ret = SQLBindCol(hstmt, 11, SQL_C_SHORT, &nullable, 0, &nullable_len);
    
    // Fetch results and build column list
    idx_t column_index = 0;
    std::vector<idx_t> not_null_columns;
    
    while (SQLFetch(hstmt) == SQL_SUCCESS) {
        std::string name((char*)column_name);
        std::string native_type((char*)type_name, type_name_len);
        LogicalType column_type;
        
        if (all_varchar) {
            column_type = LogicalType::VARCHAR;
        } else {
            // Check for special types based on type name
            if (StringUtil::Contains(StringUtil::Lower(native_type), "timestamp_tz") ||
                StringUtil::Contains(StringUtil::Lower(native_type), "timestamptz")) {
                // Special handling for timestamp with timezone
                column_type = LogicalType::TIMESTAMP;
            } else {
                // Use standard type conversion
                column_type = ODBCUtils::TypeToLogicalType(data_type, column_size, decimal_digits);
                
                // Special handling for large numeric types (common in data warehouses)
                if ((data_type == SQL_NUMERIC || data_type == SQL_DECIMAL) && 
                    decimal_digits == 0 && column_size >= 19) {
                    // For large integers like NUMBER(38,0) in Snowflake, use HUGEINT
                    column_type = LogicalType::HUGEINT;
                }
            }
        }
        
        ColumnDefinition column(std::move(name), column_type);
        columns.AddColumn(std::move(column));
        
        if (nullable == SQL_NO_NULLS) {
            constraints.push_back(make_uniq<NotNullConstraint>(LogicalIndex(column_index)));
        }
        
        column_index++;
    }
    
    SQLFreeHandle(SQL_HANDLE_STMT, hstmt);
    
    if (column_index == 0) {
        throw std::runtime_error("No columns found for table '" + table_name + "'");
    }
    
    // Get primary key information
    ret = SQLAllocHandle(SQL_HANDLE_STMT, hdbc, &hstmt);
    ret = SQLPrimaryKeys(hstmt, NULL, 0, NULL, 0, (SQLCHAR*)table_name.c_str(), SQL_NTS);
    
    if (ret == SQL_SUCCESS || ret == SQL_SUCCESS_WITH_INFO) {
        SQLCHAR pk_column_name[256];
        SQLLEN pk_column_name_len;
        SQLSMALLINT key_seq;
        SQLLEN key_seq_len;
        
        ret = SQLBindCol(hstmt, 4, SQL_C_CHAR, pk_column_name, sizeof(pk_column_name), &pk_column_name_len);
        ret = SQLBindCol(hstmt, 5, SQL_C_SHORT, &key_seq, 0, &key_seq_len);
        
        std::vector<std::string> primary_keys;
        
        while (SQLFetch(hstmt) == SQL_SUCCESS) {
            std::string pk_name((char*)pk_column_name);
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
    }
    
    SQLFreeHandle(SQL_HANDLE_STMT, hstmt);
}

bool ODBCDB::ColumnExists(const std::string &table_name, const std::string &column_name) {
    SQLHSTMT hstmt;
    SQLRETURN ret = SQLAllocHandle(SQL_HANDLE_STMT, hdbc, &hstmt);
    if (ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO) {
        throw std::runtime_error("Failed to allocate statement handle for column check");
    }
    
    // Get column information
    ret = SQLColumns(hstmt, NULL, 0, NULL, 0, (SQLCHAR*)table_name.c_str(), SQL_NTS, 
                     (SQLCHAR*)column_name.c_str(), SQL_NTS);
    
    if (ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO) {
        SQLFreeHandle(SQL_HANDLE_STMT, hstmt);
        return false;
    }
    
    // Check if any columns were returned
    ret = SQLFetch(hstmt);
    bool exists = (ret == SQL_SUCCESS);
    
    SQLFreeHandle(SQL_HANDLE_STMT, hstmt);
    return exists;
}

CatalogType ODBCDB::GetEntryType(const std::string &name) {
    SQLHSTMT hstmt;
    SQLRETURN ret = SQLAllocHandle(SQL_HANDLE_STMT, hdbc, &hstmt);
    if (ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO) {
        throw std::runtime_error("Failed to allocate statement handle for entry type check");
    }
    
    // Check if it's a table
    ret = SQLTables(hstmt, NULL, 0, NULL, 0, (SQLCHAR*)name.c_str(), SQL_NTS, (SQLCHAR*)"TABLE", SQL_NTS);
    if (ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO) {
        SQLFreeHandle(SQL_HANDLE_STMT, hstmt);
        return CatalogType::INVALID;
    }
    
    ret = SQLFetch(hstmt);
    if (ret == SQL_SUCCESS) {
        SQLFreeHandle(SQL_HANDLE_STMT, hstmt);
        return CatalogType::TABLE_ENTRY;
    }
    
    SQLFreeStmt(hstmt, SQL_CLOSE);
    
    // Check if it's a view
    ret = SQLTables(hstmt, NULL, 0, NULL, 0, (SQLCHAR*)name.c_str(), SQL_NTS, (SQLCHAR*)"VIEW", SQL_NTS);
    if (ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO) {
        SQLFreeHandle(SQL_HANDLE_STMT, hstmt);
        return CatalogType::INVALID;
    }
    
    ret = SQLFetch(hstmt);
    if (ret == SQL_SUCCESS) {
        SQLFreeHandle(SQL_HANDLE_STMT, hstmt);
        return CatalogType::VIEW_ENTRY;
    }
    
    SQLFreeHandle(SQL_HANDLE_STMT, hstmt);
    return CatalogType::INVALID;
}

void ODBCDB::CheckError(SQLRETURN ret, SQLSMALLINT handle_type, SQLHANDLE handle, const std::string &operation) {
    if (ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO) {
        std::string error_message = ODBCUtils::GetErrorMessage(handle_type, handle);
        throw std::runtime_error("ODBC Error in " + operation + ": " + error_message);
    }
}

void ODBCDB::DebugSetPrintQueries(bool print) {
    debug_odbc_print_queries = print;
}

} // namespace duckdb