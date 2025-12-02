#pragma once

#include "duckdb.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"

namespace duckdb {

class OdbcTableEntry : public TableCatalogEntry {
public:
    OdbcTableEntry(Catalog &catalog, SchemaCatalogEntry &schema, CreateTableInfo &info, bool all_varchar);

    unique_ptr<BaseStatistics> GetStatistics(ClientContext &context, column_t column_id) override;
    void BindUpdateConstraints(Binder &binder, LogicalGet &get, LogicalProjection &proj, 
                              LogicalUpdate &update, ClientContext &context) override;
    TableFunction GetScanFunction(ClientContext &context, unique_ptr<FunctionData> &bind_data) override;
    TableStorageInfo GetStorageInfo(ClientContext &context) override;

private:
    bool all_varchar;
};

} // namespace duckdb