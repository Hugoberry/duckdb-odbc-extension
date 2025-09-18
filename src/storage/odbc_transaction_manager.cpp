#include "storage/odbc_transaction_manager.hpp"
#include "storage/odbc_transaction.hpp"
#include "duckdb/main/attached_database.hpp"

namespace duckdb {

OdbcTransactionManager::OdbcTransactionManager(AttachedDatabase &db_p, OdbcCatalog &odbc_catalog)
    : TransactionManager(db_p), odbc_catalog(odbc_catalog) {
}

Transaction &OdbcTransactionManager::StartTransaction(ClientContext &context) {
    auto transaction = make_uniq<OdbcTransaction>(odbc_catalog, *this, context);
    transaction->Start();
    auto &result = *transaction;
    lock_guard<mutex> l(transaction_lock);
    // Use pointer as key
    transactions[&result] = std::move(transaction);
    return result;
}

ErrorData OdbcTransactionManager::CommitTransaction(ClientContext &context, Transaction &transaction) {
    auto &odbc_transaction = transaction.Cast<OdbcTransaction>();
    odbc_transaction.Commit();
    lock_guard<mutex> l(transaction_lock);
    // Use pointer as key
    transactions.erase(&transaction);
    return ErrorData();
}

void OdbcTransactionManager::RollbackTransaction(Transaction &transaction) {
    auto &odbc_transaction = transaction.Cast<OdbcTransaction>();
    odbc_transaction.Rollback();
    lock_guard<mutex> l(transaction_lock);
    // Use pointer as key
    transactions.erase(&transaction);
}

void OdbcTransactionManager::Checkpoint(ClientContext &context, bool force) {
    // ODBC doesn't have explicit checkpoint functionality
    // This is a no-op for ODBC databases
}

} // namespace duckdb