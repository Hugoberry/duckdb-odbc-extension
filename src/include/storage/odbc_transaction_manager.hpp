#pragma once

#include "duckdb.hpp"
#include "duckdb/transaction/transaction_manager.hpp"
#include "storage/odbc_catalog.hpp"
#include <unordered_map>

namespace duckdb {

class OdbcTransaction;

class OdbcTransactionManager : public TransactionManager {
public:
    OdbcTransactionManager(AttachedDatabase &db_p, OdbcCatalog &odbc_catalog);

    Transaction &StartTransaction(ClientContext &context) override;
    ErrorData CommitTransaction(ClientContext &context, Transaction &transaction) override;
    void RollbackTransaction(Transaction &transaction) override;
    void Checkpoint(ClientContext &context, bool force) override;

private:
    OdbcCatalog &odbc_catalog;
    mutex transaction_lock;
    // Use Transaction* as key instead of reference_wrapper
    std::unordered_map<Transaction*, unique_ptr<Transaction>> transactions;
};

} // namespace duckdb