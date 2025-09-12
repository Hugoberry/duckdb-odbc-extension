#include "odbc_parameters.hpp"
#include "duckdb/common/string_util.hpp"

namespace duckdb {

ConnectionParams OdbcParameterParser::ParseConnectionParams(const TableFunctionBindInput& input) {
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

OdbcOptions OdbcParameterParser::ParseCommonOptions(const TableFunctionBindInput& input) {
    OdbcOptions options;
    
    options.all_varchar = GetOptionalBoolean(input, "all_varchar", false);
    options.encoding = GetOptionalString(input, "encoding", "UTF-8");
    options.overwrite = GetOptionalBoolean(input, "overwrite", false);
    
    return options;
}

OdbcScanParameters OdbcParameterParser::ParseScanParameters(const TableFunctionBindInput& input) {
    OdbcScanParameters params;
    
    params.connection = ParseConnectionParams(input);
    params.table_name = GetRequiredString(input, "table_name");
    params.schema_name = GetOptionalString(input, "schema_name");
    params.options = ParseCommonOptions(input);
    
    return params;
}

OdbcQueryParameters OdbcParameterParser::ParseQueryParameters(const TableFunctionBindInput& input) {
    OdbcQueryParameters params;
    
    params.connection = ParseConnectionParams(input);
    params.query = GetRequiredString(input, "query");
    params.options = ParseCommonOptions(input);
    
    return params;
}

OdbcExecParameters OdbcParameterParser::ParseExecParameters(const TableFunctionBindInput& input) {
    OdbcExecParameters params;
    
    params.connection = ParseConnectionParams(input);
    params.sql = GetRequiredString(input, "sql");
    params.options = ParseCommonOptions(input);
    
    return params;
}

OdbcAttachParameters OdbcParameterParser::ParseAttachParameters(const TableFunctionBindInput& input) {
    OdbcAttachParameters params;
    
    params.connection = ParseConnectionParams(input);
    params.options = ParseCommonOptions(input);
    
    return params;
}

std::string OdbcParameterParser::GetRequiredString(const TableFunctionBindInput& input, 
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

std::string OdbcParameterParser::GetOptionalString(const TableFunctionBindInput& input, 
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

bool OdbcParameterParser::GetOptionalBoolean(const TableFunctionBindInput& input, 
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