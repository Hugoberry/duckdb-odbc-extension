#pragma once

#include "duckdb.hpp"
#include "odbc_connection.hpp"

namespace duckdb {

// Forward declarations
struct OdbcScannerState;
struct OdbcExecFunctionData;
struct OdbcAttachFunctionData;

// Common options for all ODBC functions
struct OdbcOptions {
    bool all_varchar = false;
    std::string encoding = "UTF-8";  // Default to UTF-8
    bool overwrite = false;
    // Add other common options as needed
};

/**
 * @brief Factory class for creating ODBC function data structures
 * Implements the Factory Pattern to eliminate intermediate parameter structs
 */
class OdbcFunctionDataFactory {
public:
    // Factory methods that directly populate the target data structures
    static std::unique_ptr<OdbcScannerState> CreateScannerState(const TableFunctionBindInput& input);
    static std::unique_ptr<OdbcScannerState> CreateQueryState(const TableFunctionBindInput& input);
    static std::unique_ptr<OdbcExecFunctionData> CreateExecData(const TableFunctionBindInput& input);
    static std::unique_ptr<OdbcAttachFunctionData> CreateAttachData(const TableFunctionBindInput& input);

    // Helper methods for parsing common components
    static ConnectionParams ParseConnectionParams(const TableFunctionBindInput& input);
    static OdbcOptions ParseOptions(const TableFunctionBindInput& input);
    
private:
    // Helper to get a string parameter with error checking
    static std::string GetRequiredString(const TableFunctionBindInput& input, 
                                       const std::string& param_name);
    
    // Helper to get an optional string parameter
    static std::string GetOptionalString(const TableFunctionBindInput& input, 
                                       const std::string& param_name, 
                                       const std::string& default_value = "");
    
    // Helper to get an optional boolean parameter
    static bool GetOptionalBoolean(const TableFunctionBindInput& input, 
                                  const std::string& param_name, 
                                  bool default_value = false);
};

} // namespace duckdb