#include "storage/odbc_table_entry.hpp"
#include "storage/odbc_catalog.hpp"
#include "storage/odbc_transaction.hpp"
#include "duckdb/storage/statistics/base_statistics.hpp"
#include "odbc_scanner.hpp"
#include "duckdb/storage/table_storage_info.hpp"

namespace duckdb {

OdbcTableEntry::OdbcTableEntry(Catalog &catalog, SchemaCatalogEntry &schema, CreateTableInfo &info, bool all_varchar)
    : TableCatalogEntry(catalog, schema, info), all_varchar(all_varchar) {
}

unique_ptr<BaseStatistics> OdbcTableEntry::GetStatistics(ClientContext &context, column_t column_id) {
    return nullptr; // ODBC doesn't provide detailed statistics
}

void OdbcTableEntry::BindUpdateConstraints(Binder &, LogicalGet &, LogicalProjection &, LogicalUpdate &, ClientContext &) {
    // ODBC tables don't have special update constraint handling
}

TableFunction OdbcTableEntry::GetScanFunction(ClientContext &context, unique_ptr<FunctionData> &bind_data) {
    auto result = make_uniq<OdbcScannerState>();
    
    // Set up connection parameters from catalog
    auto &odbc_catalog = catalog.Cast<OdbcCatalog>();
    result->connection_params = odbc_catalog.GetConnectionParams();
    result->options = odbc_catalog.GetOptions();
    
    // Set up table information
    result->table_name = name;
    result->column_names.clear();
    result->column_types.clear();
    
    for (auto &col : columns.Logical()) {
        result->column_names.push_back(col.GetName());
        result->column_types.push_back(col.GetType());
    }
    
    bind_data = std::move(result);
    return OdbcScanFunction();
}

TableStorageInfo OdbcTableEntry::GetStorageInfo(ClientContext &context) {
    TableStorageInfo result;
    
    // ODBC doesn't provide detailed storage information
    // Provide reasonable defaults
    result.cardinality = 10000; // Estimate
    
    return result;
}

} // namespace duckdb