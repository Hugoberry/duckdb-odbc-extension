#include "storage/odbc_transaction.hpp"
#include "storage/odbc_catalog.hpp"
#include "storage/odbc_table_entry.hpp"
#include "storage/odbc_schema_entry.hpp" 
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/catalog/catalog_entry/view_catalog_entry.hpp"
#include "odbc_utils.hpp"

namespace duckdb {

OdbcTransaction::OdbcTransaction(OdbcCatalog &odbc_catalog, TransactionManager &manager, ClientContext &context)
    : Transaction(manager, context), odbc_catalog(odbc_catalog), connection(nullptr) {
    
    // Create connection using catalog's connection parameters
    owned_connection = OdbcConnection::Connect(odbc_catalog.GetConnectionParams());
    connection = owned_connection.get();
}

OdbcTransaction::~OdbcTransaction() {
    // Connection cleanup handled by unique_ptr
}

void OdbcTransaction::Start() {
    // ODBC auto-commit is typically enabled by default
    // For transactions, we need to disable auto-commit
    try {
        auto native_handle = connection->GetNativeConnection().native_dbc_handle();
        SQLSetConnectAttr(native_handle, SQL_ATTR_AUTOCOMMIT, (SQLPOINTER)SQL_AUTOCOMMIT_OFF, 0);
    } catch (const nanodbc::database_error &e) {
        OdbcUtils::ThrowException("start transaction", e);
    }
}

void OdbcTransaction::Commit() {
    try {
        auto native_handle = connection->GetNativeConnection().native_dbc_handle();
        SQLEndTran(SQL_HANDLE_DBC, native_handle, SQL_COMMIT);
        // Re-enable auto-commit
        SQLSetConnectAttr(native_handle, SQL_ATTR_AUTOCOMMIT, (SQLPOINTER)SQL_AUTOCOMMIT_ON, 0);
    } catch (const nanodbc::database_error &e) {
        OdbcUtils::ThrowException("commit transaction", e);
    }
}

void OdbcTransaction::Rollback() {
    try {
        auto native_handle = connection->GetNativeConnection().native_dbc_handle();
        SQLEndTran(SQL_HANDLE_DBC, native_handle, SQL_ROLLBACK);
        // Re-enable auto-commit
        SQLSetConnectAttr(native_handle, SQL_ATTR_AUTOCOMMIT, (SQLPOINTER)SQL_AUTOCOMMIT_ON, 0);
    } catch (const nanodbc::database_error &e) {
        // Don't throw on rollback failure - log and continue
    }
}

OdbcConnection &OdbcTransaction::GetConnection() {
    return *connection;
}

OdbcTransaction &OdbcTransaction::Get(ClientContext &context, Catalog &catalog) {
    return Transaction::Get(context, catalog).Cast<OdbcTransaction>();
}

optional_ptr<CatalogEntry> OdbcTransaction::GetCatalogEntry(const string &entry_name) {
    auto entry = catalog_entries.find(entry_name);
    if (entry != catalog_entries.end()) {
        return entry->second.get();
    }

    // Look up table in ODBC database
    try {
        auto tables = connection->GetTables();
        bool found = std::find(tables.begin(), tables.end(), entry_name) != tables.end();
        
        if (found) {
            // Use the 3-parameter constructor: catalog, schema, name
            CreateTableInfo info(odbc_catalog.GetName(), DEFAULT_SCHEMA, entry_name);
            ColumnList columns;
            std::vector<std::unique_ptr<Constraint>> constraints;
            
            connection->GetTableInfo(entry_name, "", columns, constraints, 
                                   odbc_catalog.GetOptions().all_varchar);
            
            if (!columns.empty()) {
                info.columns = std::move(columns);
                
                // Cast GetMainSchema() to SchemaCatalogEntry& explicitly
                auto &main_schema = static_cast<SchemaCatalogEntry&>(odbc_catalog.GetMainSchema());
                
                auto result = make_uniq<OdbcTableEntry>(odbc_catalog, main_schema, 
                                                       info, odbc_catalog.GetOptions().all_varchar);
                auto result_ptr = result.get();
                catalog_entries[entry_name] = std::move(result);
                return result_ptr;
            }
        }
    } catch (const nanodbc::database_error &e) {
        // Table doesn't exist or access error
        return nullptr;
    }

    return nullptr;
}

void OdbcTransaction::ClearTableEntry(const string &table_name) {
    catalog_entries.erase(table_name);
}

} // namespace duckdb