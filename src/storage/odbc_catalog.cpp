#include "storage/odbc_catalog.hpp"
#include "storage/odbc_schema_entry.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"
#include "duckdb/storage/database_size.hpp"
#include "duckdb/planner/planner.hpp"

namespace duckdb {

OdbcCatalog::OdbcCatalog(AttachedDatabase &db_p, const string &path, 
                         ConnectionParams connection_params, OdbcOptions options)
    : Catalog(db_p), path(path), connection_params(std::move(connection_params)), 
      options(std::move(options)) {
}

OdbcCatalog::~OdbcCatalog() {
}

void OdbcCatalog::Initialize(bool load_builtin) {
    CreateSchemaInfo info;
    main_schema = make_uniq<OdbcSchemaEntry>(*this, info);
}

optional_ptr<CatalogEntry> OdbcCatalog::CreateSchema(CatalogTransaction transaction, 
                                                    CreateSchemaInfo &info) {
    throw BinderException("ODBC databases do not support creating new schemas");
}

void OdbcCatalog::ScanSchemas(ClientContext &context, 
                             std::function<void(SchemaCatalogEntry &)> callback) {
    callback(*main_schema);
}

optional_ptr<SchemaCatalogEntry> OdbcCatalog::LookupSchema(CatalogTransaction transaction,
                                                          const EntryLookupInfo &schema_lookup,
                                                          OnEntryNotFound if_not_found) {
    auto &schema_name = schema_lookup.GetEntryName();
    if (schema_name == DEFAULT_SCHEMA || schema_name == INVALID_SCHEMA) {
        return main_schema.get();
    }
    if (if_not_found == OnEntryNotFound::RETURN_NULL) {
        return nullptr;
    }
    throw BinderException("ODBC databases only have a single schema - \"%s\"", DEFAULT_SCHEMA);
}

void OdbcCatalog::DropSchema(ClientContext &context, DropInfo &info) {
    throw BinderException("ODBC databases do not support dropping schemas");
}

DatabaseSize OdbcCatalog::GetDatabaseSize(ClientContext &context) {
    DatabaseSize result;
    // ODBC doesn't have a standard way to get database size
    result.total_blocks = 0;
    result.block_size = 0;
    result.free_blocks = 0;
    result.used_blocks = 0;
    result.bytes = 0;
    result.wal_size = idx_t(-1);
    return result;
}

string OdbcCatalog::GetCatalogType() {
    return "odbc";
}

bool OdbcCatalog::InMemory() {
    return false; // ODBC databases are typically not in-memory
}

string OdbcCatalog::GetDBPath() {
    return path;
}

PhysicalOperator &OdbcCatalog::PlanCreateTableAs(ClientContext &context, PhysicalPlanGenerator &planner,
                                                 LogicalCreateTable &op, PhysicalOperator &plan) {
    throw BinderException("ODBC databases do not support CREATE TABLE AS through DuckDB");
}

PhysicalOperator &OdbcCatalog::PlanInsert(ClientContext &context, PhysicalPlanGenerator &planner, LogicalInsert &op,
                                         optional_ptr<PhysicalOperator> plan) {
    throw BinderException("ODBC databases do not support INSERT through DuckDB");
}

PhysicalOperator &OdbcCatalog::PlanDelete(ClientContext &context, PhysicalPlanGenerator &planner, LogicalDelete &op,
                                         PhysicalOperator &plan) {
    throw BinderException("ODBC databases do not support DELETE through DuckDB");
}

PhysicalOperator &OdbcCatalog::PlanUpdate(ClientContext &context, PhysicalPlanGenerator &planner, LogicalUpdate &op,
                                         PhysicalOperator &plan) {
    throw BinderException("ODBC databases do not support UPDATE through DuckDB");
}

} // namespace duckdb