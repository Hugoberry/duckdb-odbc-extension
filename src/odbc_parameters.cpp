#include "odbc_parameters.hpp"
#include "odbc_scanner.hpp"
#include "duckdb/common/string_util.hpp"

namespace duckdb {

std::unique_ptr<OdbcScannerState> OdbcFunctionDataFactory::CreateScannerState(const TableFunctionBindInput& input) {
    auto result = std::make_unique<OdbcScannerState>();
    
    // Parse and set connection parameters
    result->connection_params = ParseConnectionParams(input);
    
    // Parse scan-specific parameters
    result->table_name = GetRequiredString(input, "table_name");
    result->schema_name = GetOptionalString(input, "schema_name");
    
    // Parse options
    result->options = ParseOptions(input);
    
    return result;
}

std::unique_ptr<OdbcScannerState> OdbcFunctionDataFactory::CreateQueryState(const TableFunctionBindInput& input) {
    auto result = std::make_unique<OdbcScannerState>();
    
    // Parse and set connection parameters
    result->connection_params = ParseConnectionParams(input);
    
    // Parse query-specific parameters
    result->sql = GetRequiredString(input, "query");
    
    // Parse options
    result->options = ParseOptions(input);
    
    return result;
}

std::unique_ptr<OdbcExecFunctionData> OdbcFunctionDataFactory::CreateExecData(const TableFunctionBindInput& input) {
    auto result = std::make_unique<OdbcExecFunctionData>();
    
    // Parse and set connection parameters
    result->connection_params = ParseConnectionParams(input);
    
    // Parse exec-specific parameters
    result->sql = GetRequiredString(input, "sql");
    
    // Parse options
    result->options = ParseOptions(input);
    
    return result;
}

std::unique_ptr<OdbcAttachFunctionData> OdbcFunctionDataFactory::CreateAttachData(const TableFunctionBindInput& input) {
    auto result = std::make_unique<OdbcAttachFunctionData>();
    
    // Parse and set connection parameters
    result->connection_params = ParseConnectionParams(input);
    
    // Parse options
    result->options = ParseOptions(input);
    
    return result;
}

ConnectionParams OdbcFunctionDataFactory::ParseConnectionParams(const TableFunctionBindInput& input) {
    std::string connection = GetRequiredString(input, "connection");
    std::string username = GetOptionalString(input, "username");
    std::string password = GetOptionalString(input, "password");
    
    // Parse additional connection options
    int timeout = 60;  // Default timeout
    bool read_only = true;  // Default to read-only
    
    auto timeout_param = input.named_parameters.find("timeout");
    if (timeout_param != input.named_parameters.end()) {
        timeout = timeout_param->second.GetValue<int>();
    }
    
    auto read_only_param = input.named_parameters.find("read_only");
    if (read_only_param != input.named_parameters.end()) {
        read_only = read_only_param->second.GetValue<bool>();
    }
    
    return ConnectionParams(connection, username, password, timeout, read_only);
}

OdbcOptions OdbcFunctionDataFactory::ParseOptions(const TableFunctionBindInput& input) {
    OdbcOptions options;
    
    options.all_varchar = GetOptionalBoolean(input, "all_varchar", false);
    options.encoding = GetOptionalString(input, "encoding", "UTF-8");
    options.overwrite = GetOptionalBoolean(input, "overwrite", false);
    
    return options;
}

std::string OdbcFunctionDataFactory::GetRequiredString(const TableFunctionBindInput& input, 
                                                 const std::string& param_name) {
    auto it = input.named_parameters.find(param_name);
    if (it == input.named_parameters.end()) {
        throw BinderException("Missing required parameter '%s'", param_name);
    }
    
    if (it->second.type().id() != LogicalTypeId::VARCHAR) {
        throw BinderException("Parameter '%s' must be a string", param_name);
    }
    
    return it->second.GetValue<string>();
}

std::string OdbcFunctionDataFactory::GetOptionalString(const TableFunctionBindInput& input, 
                                                  const std::string& param_name, 
                                                  const std::string& default_value) {
    auto it = input.named_parameters.find(param_name);
    if (it == input.named_parameters.end()) {
        return default_value;
    }
    
    if (it->second.type().id() != LogicalTypeId::VARCHAR) {
        throw BinderException("Parameter '%s' must be a string", param_name);
    }
    
    return it->second.GetValue<string>();
}

bool OdbcFunctionDataFactory::GetOptionalBoolean(const TableFunctionBindInput& input, 
                                            const std::string& param_name, 
                                            bool default_value) {
    auto it = input.named_parameters.find(param_name);
    if (it == input.named_parameters.end()) {
        return default_value;
    }
    
    if (it->second.type().id() != LogicalTypeId::BOOLEAN) {
        throw BinderException("Parameter '%s' must be a boolean", param_name);
    }
    
    return it->second.GetValue<bool>();
}

} // namespace duckdb