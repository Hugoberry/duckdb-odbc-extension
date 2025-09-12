#pragma once

#include "duckdb.hpp"
#include "odbc_connection.hpp"

namespace duckdb {

// Common options for all ODBC functions
struct OdbcOptions {
    bool all_varchar = false;
    std::string encoding = "UTF-8";  // Default to UTF-8
    bool overwrite = false;
    // Add other common options as needed
};

// Scan-specific parameters
struct OdbcScanParameters {
    ConnectionParams connection;
    std::string table_name;
    std::string schema_name;
    OdbcOptions options;
};

// Query-specific parameters
struct OdbcQueryParameters {
    ConnectionParams connection;
    std::string query;
    OdbcOptions options;
};

// Exec-specific parameters
struct OdbcExecParameters {
    ConnectionParams connection;
    std::string sql;
    OdbcOptions options;
};

// Attach-specific parameters
struct OdbcAttachParameters {
    ConnectionParams connection;
    OdbcOptions options;
};

// Parameter parsing utility class
class OdbcParameterParser {
public:
    // Parse connection parameters from named parameters
    static ConnectionParams ParseConnectionParams(const TableFunctionBindInput& input);
    
    // Parse common options from named parameters
    static OdbcOptions ParseCommonOptions(const TableFunctionBindInput& input);
    
    // Parse scan-specific parameters
    static OdbcScanParameters ParseScanParameters(const TableFunctionBindInput& input);
    
    // Parse query-specific parameters
    static OdbcQueryParameters ParseQueryParameters(const TableFunctionBindInput& input);
    
    // Parse exec-specific parameters
    static OdbcExecParameters ParseExecParameters(const TableFunctionBindInput& input);
    
    // Parse attach-specific parameters
    static OdbcAttachParameters ParseAttachParameters(const TableFunctionBindInput& input);
    
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