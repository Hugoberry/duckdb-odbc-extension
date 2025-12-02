#pragma once

#include "duckdb.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/planner/operator/logical_insert.hpp"
#include "duckdb/planner/operator/logical_delete.hpp"
#include "duckdb/planner/operator/logical_update.hpp"
#include "duckdb/planner/operator/logical_create_table.hpp"
#include "odbc_connection.hpp"
#include "odbc_parameters.hpp"

namespace duckdb {

class OdbcSchemaEntry;

class OdbcCatalog : public Catalog {
public:
    OdbcCatalog(AttachedDatabase &db_p, const string &path, 
                ConnectionParams connection_params, OdbcOptions options);
    ~OdbcCatalog();

    void Initialize(bool load_builtin) override;
    
    optional_ptr<CatalogEntry> CreateSchema(CatalogTransaction transaction, 
                                          CreateSchemaInfo &info) override;
    void ScanSchemas(ClientContext &context, 
                    std::function<void(SchemaCatalogEntry &)> callback) override;
    optional_ptr<SchemaCatalogEntry> LookupSchema(CatalogTransaction transaction,
                                                 const EntryLookupInfo &schema_lookup,
                                                 OnEntryNotFound if_not_found) override;
    
    void DropSchema(ClientContext &context, DropInfo &info) override;
    DatabaseSize GetDatabaseSize(ClientContext &context) override;

    // Pure virtual methods from Catalog base class
    string GetCatalogType() override;
    bool InMemory() override;
    string GetDBPath() override;
    
    // Planning methods for DML operations
    PhysicalOperator &PlanCreateTableAs(ClientContext &context, PhysicalPlanGenerator &planner,
                                       LogicalCreateTable &op, PhysicalOperator &plan) override;
    PhysicalOperator &PlanInsert(ClientContext &context, PhysicalPlanGenerator &planner, LogicalInsert &op,
                                optional_ptr<PhysicalOperator> plan) override;
    PhysicalOperator &PlanDelete(ClientContext &context, PhysicalPlanGenerator &planner, LogicalDelete &op,
                                PhysicalOperator &plan) override;
    PhysicalOperator &PlanUpdate(ClientContext &context, PhysicalPlanGenerator &planner, LogicalUpdate &op,
                                PhysicalOperator &plan) override;

    // Fixed return type for GetMainSchema
    OdbcSchemaEntry& GetMainSchema() const { return *main_schema; }

    // ODBC-specific methods
    const ConnectionParams& GetConnectionParams() const { return connection_params; }
    const OdbcOptions& GetOptions() const { return options; }

private:
    string path;
    ConnectionParams connection_params;
    OdbcOptions options;
    unique_ptr<OdbcSchemaEntry> main_schema;
};

} // namespace duckdb