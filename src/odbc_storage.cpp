#include "odbc_storage.hpp"
#include "storage/odbc_catalog.hpp"
#include "storage/odbc_transaction_manager.hpp"
#include "duckdb/parser/parsed_data/attach_info.hpp"
#include "duckdb/transaction/transaction_manager.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"

namespace duckdb {

static unique_ptr<Catalog> OdbcAttach(optional_ptr<StorageExtensionInfo> storage_info, 
                                      ClientContext &context,
                                      AttachedDatabase &db, const string &name, 
                                      AttachInfo &info,
                                      AttachOptions &attach_options) {
    ConnectionParams params;
    OdbcOptions options;
    
    // Parse connection info from attach_options
    for (auto &entry : attach_options.options) {
        if (StringUtil::CIEquals(entry.first, "connection_string")) {
            params = ConnectionParams(entry.second.ToString());
        } else if (StringUtil::CIEquals(entry.first, "dsn")) {
            params = ConnectionParams(entry.second.ToString());
        } else if (StringUtil::CIEquals(entry.first, "username")) {
            // Handle username/password in ConnectionParams constructor
        } else if (StringUtil::CIEquals(entry.first, "all_varchar")) {
            options.all_varchar = entry.second.GetValue<bool>();
        } else if (StringUtil::CIEquals(entry.first, "encoding")) {
            options.encoding = entry.second.ToString();
        } else {
            throw NotImplementedException("Unsupported parameter for ODBC Attach: %s", entry.first);
        }
    }
    
    return make_uniq<OdbcCatalog>(db, info.path, std::move(params), std::move(options));
}

static unique_ptr<TransactionManager> OdbcCreateTransactionManager(
    optional_ptr<StorageExtensionInfo> storage_info,
    AttachedDatabase &db, Catalog &catalog) {
    auto &odbc_catalog = catalog.Cast<OdbcCatalog>();
    return make_uniq<OdbcTransactionManager>(db, odbc_catalog);
}

OdbcStorageExtension::OdbcStorageExtension() {
    attach = OdbcAttach;
    create_transaction_manager = OdbcCreateTransactionManager;
}

} // namespace duckdb