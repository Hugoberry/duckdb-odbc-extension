#pragma once

#include "duckdb.hpp"
#include "duckdb/transaction/transaction.hpp"
#include "odbc_connection.hpp"
#include <unordered_map>

namespace duckdb {

class OdbcCatalog;
class CatalogEntry;

class OdbcTransaction : public Transaction {
public:
    OdbcTransaction(OdbcCatalog &odbc_catalog, TransactionManager &manager, ClientContext &context);
    ~OdbcTransaction();

    void Start();
    void Commit();
    void Rollback();

    OdbcConnection &GetConnection();
    
    static OdbcTransaction &Get(ClientContext &context, Catalog &catalog);

    // Catalog entry management
    optional_ptr<CatalogEntry> GetCatalogEntry(const string &entry_name);
    void ClearTableEntry(const string &table_name);

private:
    OdbcCatalog &odbc_catalog;
    unique_ptr<OdbcConnection> owned_connection;
    OdbcConnection *connection;
    
    // Cache for catalog entries
    std::unordered_map<string, unique_ptr<CatalogEntry>> catalog_entries;
};

} // namespace duckdb