#pragma once

#include "duckdb.hpp"
#include "odbc_connection.hpp" 
#include "odbc_statement.hpp"
#include "odbc_parameters.hpp"
#include <cmath>

namespace duckdb {

/**
 * @brief Operation types for ODBC functions
 */
enum class OdbcOperation {
    SCAN,    // Read a table
    ATTACH,  // Attach database
    QUERY,   // Execute custom query
    EXEC     // Execute statement without results
};

/**
 * @brief Scanner state for ODBC functions
 */
struct OdbcScannerState : public TableFunctionData {
    // Connection parameters
    ConnectionParams connection_params;
    
    // Operation details
    std::string table_name;
    std::string schema_name;
    std::string sql;
    
    // Schema information
    std::vector<std::string> column_names;
    std::vector<LogicalType> column_types;
    
    // Options
    OdbcOptions options;
    
    // Global shared connection (optional)
    std::shared_ptr<OdbcConnection> global_connection;
};

/**
 * @brief Local scanner state for parallel execution
 */
struct OdbcLocalScanState : public LocalTableFunctionState {
    // Connection (owned or borrowed)
    std::shared_ptr<OdbcConnection> connection;
    std::unique_ptr<OdbcStatement> statement;
    
    // Scan state
    bool done = false;
    std::vector<column_t> column_ids;
    idx_t scan_count = 0;
};

/**
 * @brief Global scanner state for parallel execution
 */
struct OdbcGlobalScanState : public GlobalTableFunctionState {
    explicit OdbcGlobalScanState(idx_t max_threads) : max_thread_count(max_threads) {}
    
    std::mutex lock;
    idx_t position = 0;
    idx_t max_thread_count;
    
    idx_t MaxThreads() const override {
        return max_thread_count;
    }
};

/**
 * @brief Exec function data
 */
struct OdbcExecFunctionData : public TableFunctionData {
    ConnectionParams connection_params;
    std::string sql;
    OdbcOptions options;
    bool finished = false;
};

/**
 * @brief Attach function data
 */
struct OdbcAttachFunctionData : public TableFunctionData {
    ConnectionParams connection_params;
    OdbcOptions options;
    bool finished = false;
};

/**
 * @brief Creates standard ODBC table function instances
 */
class OdbcTableFunction {
public:
    static TableFunction CreateScanFunction();
    static TableFunction CreateAttachFunction();
    static TableFunction CreateQueryFunction();
    static TableFunction CreateExecFunction();
};

// Main binding function for all ODBC operations
unique_ptr<FunctionData> BindOdbcFunction(ClientContext &context, 
                                        TableFunctionBindInput &input,
                                        vector<LogicalType> &return_types, 
                                        vector<string> &names,
                                        OdbcOperation operation);

// Main scan function for reading data
void ScanOdbcSource(ClientContext &context, TableFunctionInput &data, DataChunk &output);

// State initialization functions
unique_ptr<GlobalTableFunctionState> InitOdbcGlobalState(ClientContext &context, 
                                                       TableFunctionInitInput &input);
                                                       
unique_ptr<LocalTableFunctionState> InitOdbcLocalState(ExecutionContext &context, 
                                                     TableFunctionInitInput &input,
                                                     GlobalTableFunctionState *global_state);

// Attach function for creating database views
void AttachOdbcDatabase(ClientContext &context, TableFunctionInput &data, DataChunk &output);
void ExecuteOdbcStatement(ClientContext &context, TableFunctionInput &data, DataChunk &output);

// Function declarations for public API
TableFunction OdbcScanFunction();
TableFunction OdbcAttachFunction();
TableFunction OdbcQueryFunction();
TableFunction OdbcExecFunction();

} // namespace duckdb