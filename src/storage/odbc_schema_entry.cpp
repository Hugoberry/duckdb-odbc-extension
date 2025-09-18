#include "storage/odbc_schema_entry.hpp"
#include "storage/odbc_catalog.hpp"
#include "storage/odbc_transaction.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"

namespace duckdb {

OdbcSchemaEntry::OdbcSchemaEntry(Catalog &catalog, CreateSchemaInfo &info) 
    : SchemaCatalogEntry(catalog, info) {
}

optional_ptr<CatalogEntry> OdbcSchemaEntry::CreateTable(CatalogTransaction transaction, BoundCreateTableInfo &info) {
    throw BinderException("ODBC databases do not support creating tables through DuckDB");
}

optional_ptr<CatalogEntry> OdbcSchemaEntry::CreateFunction(CatalogTransaction transaction, CreateFunctionInfo &info) {
    throw BinderException("ODBC databases do not support creating functions");
}

optional_ptr<CatalogEntry> OdbcSchemaEntry::CreateIndex(CatalogTransaction transaction, CreateIndexInfo &info, TableCatalogEntry &table) {
    throw BinderException("ODBC databases do not support creating indexes through DuckDB");
}

optional_ptr<CatalogEntry> OdbcSchemaEntry::CreateView(CatalogTransaction transaction, CreateViewInfo &info) {
    throw BinderException("ODBC databases do not support creating views through DuckDB");
}

optional_ptr<CatalogEntry> OdbcSchemaEntry::CreateSequence(CatalogTransaction transaction, CreateSequenceInfo &info) {
    throw BinderException("ODBC databases do not support creating sequences");
}

optional_ptr<CatalogEntry> OdbcSchemaEntry::CreateTableFunction(CatalogTransaction transaction, CreateTableFunctionInfo &info) {
    throw BinderException("ODBC databases do not support creating table functions");
}

optional_ptr<CatalogEntry> OdbcSchemaEntry::CreateCopyFunction(CatalogTransaction transaction, CreateCopyFunctionInfo &info) {
    throw BinderException("ODBC databases do not support creating copy functions");
}

optional_ptr<CatalogEntry> OdbcSchemaEntry::CreatePragmaFunction(CatalogTransaction transaction, CreatePragmaFunctionInfo &info) {
    throw BinderException("ODBC databases do not support creating pragma functions");
}

optional_ptr<CatalogEntry> OdbcSchemaEntry::CreateCollation(CatalogTransaction transaction, CreateCollationInfo &info) {
    throw BinderException("ODBC databases do not support creating collations");
}

optional_ptr<CatalogEntry> OdbcSchemaEntry::CreateType(CatalogTransaction transaction, CreateTypeInfo &info) {
    throw BinderException("ODBC databases do not support creating types");
}

void OdbcSchemaEntry::Alter(CatalogTransaction catalog_transaction, AlterInfo &info) {
    throw BinderException("ODBC databases do not support altering tables through DuckDB");
}

void OdbcSchemaEntry::Scan(ClientContext &context, CatalogType type, const std::function<void(CatalogEntry &)> &callback) {
    auto &transaction = OdbcTransaction::Get(context, catalog);
    vector<string> entries;
    
    switch (type) {
    case CatalogType::TABLE_ENTRY:
        try {
            entries = transaction.GetConnection().GetTables();
        } catch (...) {
            // If we can't get tables, return empty
            return;
        }
        break;
    case CatalogType::VIEW_ENTRY:
        try {
            entries = transaction.GetConnection().GetViews();
        } catch (...) {
            // If we can't get views, return empty
            return;
        }
        break;
    default:
        // No other catalog types supported
        return;
    }
    
    for (auto &entry_name : entries) {
        auto entry = transaction.GetCatalogEntry(entry_name);
        if (entry) {
            callback(*entry);
        }
    }
}

void OdbcSchemaEntry::Scan(CatalogType type, const std::function<void(CatalogEntry &)> &callback) {
    throw InternalException("OdbcSchemaEntry::Scan without context not supported");
}

void OdbcSchemaEntry::DropEntry(ClientContext &context, DropInfo &info) {
    throw BinderException("ODBC databases do not support dropping entries through DuckDB");
}

optional_ptr<CatalogEntry> OdbcSchemaEntry::LookupEntry(CatalogTransaction transaction, const EntryLookupInfo &lookup_info) {
    auto &odbc_transaction = transaction.transaction->Cast<OdbcTransaction>();
    
    switch (lookup_info.GetCatalogType()) {
    case CatalogType::TABLE_ENTRY:
    case CatalogType::VIEW_ENTRY:
        return odbc_transaction.GetCatalogEntry(lookup_info.GetEntryName());
    default:
        return nullptr;
    }
}

} // namespace duckdb