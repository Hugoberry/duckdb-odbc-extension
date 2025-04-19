#define DUCKDB_EXTENSION_MAIN

#include "odbc_extension.hpp"
#include "duckdb.hpp"
#include "odbc_db.hpp"
#include "odbc_scanner.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/main/extension_util.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"

namespace duckdb {

static void SetODBCDebugQueryPrint(ClientContext &context, SetScope scope, Value &parameter) {
    OdbcDB::DebugSetPrintQueries(BooleanValue::Get(parameter));
}

static void LoadInternal(DatabaseInstance &instance) {
    // Register the ODBC scan function
    ODBCScanFunction odbc_fun;
    ExtensionUtil::RegisterFunction(instance, odbc_fun);

    // Register the ODBC attach function
    ODBCAttachFunction attach_func;
    ExtensionUtil::RegisterFunction(instance, attach_func);

    // Register the ODBC query function
    ODBCQueryFunction query_func;
    ExtensionUtil::RegisterFunction(instance, query_func);

    // Add extension options
    auto &config = DBConfig::GetConfig(instance);
    config.AddExtensionOption("odbc_all_varchar", "Load all ODBC columns as VARCHAR columns", LogicalType(LogicalTypeId::BOOLEAN));
    
    config.AddExtensionOption("odbc_debug_show_queries", "DEBUG SETTING: print all queries sent to ODBC to stdout",
                            LogicalType(LogicalTypeId::BOOLEAN), false, SetODBCDebugQueryPrint);
}

void OdbcExtension::Load(DuckDB &db) {
    LoadInternal(*db.instance);
}

} // namespace duckdb

extern "C" {

DUCKDB_EXTENSION_API void odbc_init(duckdb::DatabaseInstance &db) {
    duckdb::DuckDB db_wrapper(db);
    db_wrapper.LoadExtension<duckdb::OdbcExtension>();
}

DUCKDB_EXTENSION_API const char *odbc_extension_version() {
    return duckdb::DuckDB::LibraryVersion();
}

}