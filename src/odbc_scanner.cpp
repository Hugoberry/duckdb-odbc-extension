#include "odbc_scanner.hpp"
#include "odbc_utils.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/common/types/date.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/common/types/time.hpp"
#include "duckdb/storage/statistics/base_statistics.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/main/attached_database.hpp"
#include <cmath>
#include <algorithm>

namespace duckdb {

LogicalType GetDuckDBType(SQLSMALLINT odbc_type, SQLULEN column_size, SQLSMALLINT decimal_digits) {
    return ODBCUtils::TypeToLogicalType(odbc_type, column_size, decimal_digits);
}

int GetODBCSQLType(const LogicalType &type) {
    return ODBCUtils::ToODBCType(type);
}

static unique_ptr<FunctionData> ODBCBind(ClientContext &context, TableFunctionBindInput &input,
                                       vector<LogicalType> &return_types, vector<string> &names) {
    auto result = make_uniq<ODBCBindData>();
    
    // Check which connection method to use
    if (input.inputs[0].type().id() == LogicalTypeId::VARCHAR) {
        // First argument is table name
        result->table_name = input.inputs[0].GetValue<string>();
        
        if (input.inputs.size() < 2) {
            throw BinderException("ODBC scan requires at least a table name and either a DSN or connection string");
        }
        
        if (input.inputs[1].type().id() == LogicalTypeId::VARCHAR) {
            // Second argument can be either DSN or connection string
            auto conn_str = input.inputs[1].GetValue<string>();
            
            // Check if it's a DSN or connection string
            if (conn_str.find('=') == string::npos) {
                // Likely a DSN
                result->dsn = conn_str;
                
                // Check for optional username and password
                if (input.inputs.size() >= 3) {
                    result->username = input.inputs[2].GetValue<string>();
                }
                
                if (input.inputs.size() >= 4) {
                    result->password = input.inputs[3].GetValue<string>();
                }
            } else {
                // Connection string
                result->connection_string = conn_str;
            }
        } else {
            throw BinderException("Second argument to ODBC scan must be a VARCHAR (DSN or connection string)");
        }
    } else {
        throw BinderException("First argument to ODBC scan must be a VARCHAR (table name)");
    }
    
    // Process additional parameters
    for (auto &kv : input.named_parameters) {
        if (kv.first == "all_varchar") {
            result->all_varchar = BooleanValue::Get(kv.second);
        }
    }
    
    // Connect to data source and get table schema
    ODBCDB db;
    if (!result->dsn.empty()) {
        db = ODBCDB::OpenWithDSN(result->dsn, result->username, result->password);
    } else if (!result->connection_string.empty()) {
        db = ODBCDB::OpenWithConnectionString(result->connection_string);
    } else {
        throw BinderException("Either DSN or connection string must be provided for ODBC scan");
    }
    
    // Get table information
    ColumnList columns;
    std::vector<std::unique_ptr<Constraint>> constraints;
    db.GetTableInfo(result->table_name, columns, constraints, result->all_varchar);
    
    // Map column types and names
    for (auto &column : columns.Logical()) {
        names.push_back(column.GetName());
        return_types.push_back(column.GetType());
    }
    
    if (names.empty()) {
        throw BinderException("No columns found for table " + result->table_name);
    }
    
    result->names = names;
    result->types = return_types;
    
    return std::move(result);
}

static unique_ptr<LocalTableFunctionState>
ODBCInitLocalState(ExecutionContext &context, TableFunctionInitInput &input, GlobalTableFunctionState *global_state) {
    auto &bind_data = input.bind_data->Cast<ODBCBindData>();
    auto &gstate = global_state->Cast<ODBCGlobalState>();
    auto result = make_uniq<ODBCLocalState>();
    
    result->column_ids = input.column_ids;
    
    // If we have a global database connection, use it
    result->db = bind_data.global_db;
    
    if (!result->db) {
        // Otherwise create a new connection
        if (!bind_data.dsn.empty()) {
            result->owned_db = ODBCDB::OpenWithDSN(bind_data.dsn, bind_data.username, bind_data.password);
        } else if (!bind_data.connection_string.empty()) {
            result->owned_db = ODBCDB::OpenWithConnectionString(bind_data.connection_string);
        } else {
            throw std::runtime_error("No connection information available");
        }
        result->db = &result->owned_db;
    }
    
    // Prepare the query
    string sql;
    if (bind_data.sql.empty()) {
        // Build query based on column IDs
        auto col_names = StringUtil::Join(
            result->column_ids.data(), result->column_ids.size(), ", ", [&](const idx_t column_id) {
                return column_id == (column_t)-1 ? "NULL"
                                                : '"' + ODBCUtils::SanitizeString(bind_data.names[column_id]) + '"';
            });
            
        sql = StringUtil::Format("SELECT %s FROM \"%s\"", col_names, 
                                ODBCUtils::SanitizeString(bind_data.table_name));
    } else {
        sql = bind_data.sql;
    }
    
    result->stmt = result->db->Prepare(sql.c_str());
    result->done = false;
    
    return std::move(result);
}

static unique_ptr<GlobalTableFunctionState> ODBCInitGlobalState(ClientContext &context,
                                                                TableFunctionInitInput &input) {
    return make_uniq<ODBCGlobalState>(1); // Single-threaded scan for now
}

static void ODBCScan(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
    auto &state = data.local_state->Cast<ODBCLocalState>();
    if (state.done) {
        return;
    }
    
    // Initialize binding buffers for columns - if needed
    // This would be for more complex types that need special handling
    
    // Fetch rows and populate the DataChunk
    idx_t out_idx = 0;
    while (true) {
        if (out_idx == STANDARD_VECTOR_SIZE) {
            output.SetCardinality(out_idx);
            return;
        }
        
        auto &stmt = state.stmt;
        bool has_more;
        
        // For the first row, we need to call Step which executes the statement
        if (out_idx == 0) {
            has_more = stmt.Step();
        } else {
            // For subsequent rows, we call SQLFetch
            SQLRETURN ret = SQLFetch(stmt.hstmt);
            has_more = (ret == SQL_SUCCESS || ret == SQL_SUCCESS_WITH_INFO);
        }
        
        if (!has_more) {
            state.done = true;
            output.SetCardinality(out_idx);
            break;
        }
        
        state.scan_count++;
        
        // Process each column
        for (idx_t col_idx = 0; col_idx < output.ColumnCount(); col_idx++) {
            auto &out_vec = output.data[col_idx];
            SQLSMALLINT odbc_type = stmt.GetODBCType(col_idx);
            
            // Get the NULL indicator
            SQLLEN indicator;
            SQLGetData(stmt.hstmt, col_idx + 1, SQL_C_DEFAULT, NULL, 0, &indicator);
            
            if (indicator == SQL_NULL_DATA) {
                auto &mask = FlatVector::Validity(out_vec);
                mask.Set(out_idx, false);
                continue;
            }
            
            // Based on the output vector type, convert and fetch the data
            switch (out_vec.GetType().id()) {
                case LogicalTypeId::BOOLEAN: {
                    char value;
                    SQLGetData(stmt.hstmt, col_idx + 1, SQL_C_BIT, &value, sizeof(value), &indicator);
                    FlatVector::GetData<bool>(out_vec)[out_idx] = value != 0;
                    break;
                }
                case LogicalTypeId::TINYINT: {
                    int8_t value;
                    SQLGetData(stmt.hstmt, col_idx + 1, SQL_C_STINYINT, &value, sizeof(value), &indicator);
                    FlatVector::GetData<int8_t>(out_vec)[out_idx] = value;
                    break;
                }
                case LogicalTypeId::SMALLINT: {
                    int16_t value;
                    SQLGetData(stmt.hstmt, col_idx + 1, SQL_C_SSHORT, &value, sizeof(value), &indicator);
                    FlatVector::GetData<int16_t>(out_vec)[out_idx] = value;
                    break;
                }
                case LogicalTypeId::INTEGER: {
                    int32_t value;
                    SQLGetData(stmt.hstmt, col_idx + 1, SQL_C_SLONG, &value, sizeof(value), &indicator);
                    FlatVector::GetData<int32_t>(out_vec)[out_idx] = value;
                    break;
                }
                case LogicalTypeId::BIGINT: {
                    int64_t value;
                    SQLGetData(stmt.hstmt, col_idx + 1, SQL_C_SBIGINT, &value, sizeof(value), &indicator);
                    FlatVector::GetData<int64_t>(out_vec)[out_idx] = value;
                    break;
                }
                case LogicalTypeId::FLOAT: {
                    float value;
                    SQLGetData(stmt.hstmt, col_idx + 1, SQL_C_FLOAT, &value, sizeof(value), &indicator);
                    FlatVector::GetData<float>(out_vec)[out_idx] = value;
                    break;
                }
                case LogicalTypeId::DOUBLE: {
                    double value;
                    SQLGetData(stmt.hstmt, col_idx + 1, SQL_C_DOUBLE, &value, sizeof(value), &indicator);
                    FlatVector::GetData<double>(out_vec)[out_idx] = value;
                    break;
                }
                case LogicalTypeId::VARCHAR: {
                    // For string data, we need to handle potentially large strings
                    char buffer[8192];
                    SQLLEN bytes_read;
                    SQLGetData(stmt.hstmt, col_idx + 1, SQL_C_CHAR, buffer, sizeof(buffer), &bytes_read);
                    
                    if (bytes_read > 0 && bytes_read < sizeof(buffer)) {
                        // String fits in the buffer
                        FlatVector::GetData<string_t>(out_vec)[out_idx] = 
                            StringVector::AddString(out_vec, buffer, bytes_read);
                    } else if (bytes_read >= sizeof(buffer)) {
                        // String is larger than buffer, need to handle in chunks
                        std::string large_string(buffer, sizeof(buffer) - 1);
                        
                        while (bytes_read >= sizeof(buffer) - 1) {
                            SQLGetData(stmt.hstmt, col_idx + 1, SQL_C_CHAR, buffer, sizeof(buffer), &bytes_read);
                            if (bytes_read > 0) {
                                size_t append_size = bytes_read < sizeof(buffer) ? bytes_read : sizeof(buffer) - 1;
                                large_string.append(buffer, append_size);
                            }
                        }
                        
                        FlatVector::GetData<string_t>(out_vec)[out_idx] = 
                            StringVector::AddString(out_vec, large_string);
                    } else {
                        // Empty string
                        FlatVector::GetData<string_t>(out_vec)[out_idx] = 
                            StringVector::AddString(out_vec, "");
                    }
                    break;
                }
                case LogicalTypeId::DATE: {
                    SQL_DATE_STRUCT date_val;
                    SQLGetData(stmt.hstmt, col_idx + 1, SQL_C_DATE, &date_val, sizeof(date_val), &indicator);
                    FlatVector::GetData<date_t>(out_vec)[out_idx] = Date::FromDate(date_val.year, date_val.month, date_val.day);
                    break;
                }
                case LogicalTypeId::TIME: {
                    SQL_TIME_STRUCT time_val;
                    SQLGetData(stmt.hstmt, col_idx + 1, SQL_C_TIME, &time_val, sizeof(time_val), &indicator);
                    FlatVector::GetData<dtime_t>(out_vec)[out_idx] = Time::FromTime(time_val.hour, time_val.minute, time_val.second, 0);
                    break;
                }
                case LogicalTypeId::TIMESTAMP: {
                    SQL_TIMESTAMP_STRUCT ts_val;
                    SQLGetData(stmt.hstmt, col_idx + 1, SQL_C_TYPE_TIMESTAMP, &ts_val, sizeof(ts_val), &indicator);
                    
                    date_t date_val = Date::FromDate(ts_val.year, ts_val.month, ts_val.day);
                    dtime_t time_val = Time::FromTime(ts_val.hour, ts_val.minute, ts_val.second, ts_val.fraction / 1000000);
                    
                    FlatVector::GetData<timestamp_t>(out_vec)[out_idx] = Timestamp::FromDatetime(date_val, time_val);
                    break;
                }
                case LogicalTypeId::BLOB: {
                    // For BLOB data, we need to handle potentially large binary data
                    char buffer[8192];
                    SQLLEN bytes_read;
                    SQLGetData(stmt.hstmt, col_idx + 1, SQL_C_BINARY, buffer, sizeof(buffer), &bytes_read);
                    
                    if (bytes_read > 0 && bytes_read < sizeof(buffer)) {
                        // Binary data fits in the buffer
                        FlatVector::GetData<string_t>(out_vec)[out_idx] = 
                            StringVector::AddStringOrBlob(out_vec, buffer, bytes_read);
                    } else if (bytes_read >= sizeof(buffer)) {
                        // Binary data is larger than buffer, need to handle in chunks
                        std::vector<char> large_blob;
                        large_blob.insert(large_blob.end(), buffer, buffer + sizeof(buffer) - 1);
                        
                        while (bytes_read >= sizeof(buffer) - 1) {
                            SQLGetData(stmt.hstmt, col_idx + 1, SQL_C_BINARY, buffer, sizeof(buffer), &bytes_read);
                            if (bytes_read > 0) {
                                size_t append_size = bytes_read < sizeof(buffer) ? bytes_read : sizeof(buffer) - 1;
                                large_blob.insert(large_blob.end(), buffer, buffer + append_size);
                            }
                        }
                        
                        FlatVector::GetData<string_t>(out_vec)[out_idx] = 
                            StringVector::AddStringOrBlob(out_vec, large_blob.data(), large_blob.size());
                    } else {
                        // Empty BLOB
                        FlatVector::GetData<string_t>(out_vec)[out_idx] = 
                            StringVector::AddStringOrBlob(out_vec, "", 0);
                    }
                    break;
                }
                default:
                    throw std::runtime_error("Unsupported ODBC to DuckDB type conversion: " + 
                                           out_vec.GetType().ToString());
            }
        }
        
        out_idx++;
    }
}

static InsertionOrderPreservingMap<string> ODBCToString(TableFunctionToStringInput &input) {
    D_ASSERT(input.bind_data);
    InsertionOrderPreservingMap<string> result;
    auto &bind_data = input.bind_data->Cast<ODBCBindData>();
    result["Table"] = bind_data.table_name;
    if (!bind_data.dsn.empty()) {
        result["DSN"] = bind_data.dsn;
    } else if (!bind_data.connection_string.empty()) {
        // Don't show the full connection string as it might contain credentials
        result["Connection"] = "Connection String";
    }
    return result;
}

static BindInfo ODBCBindInfo(const optional_ptr<FunctionData> bind_data_p) {
    BindInfo info(ScanType::EXTERNAL);
    auto &bind_data = bind_data_p->Cast<ODBCBindData>();
    info.table = bind_data.table;
    return info;
}

ODBCScanFunction::ODBCScanFunction()
    : TableFunction("odbc_scan", {LogicalType::VARCHAR, LogicalType::VARCHAR}, ODBCScan, ODBCBind,
                    ODBCInitGlobalState, ODBCInitLocalState) {
    to_string = ODBCToString;
    get_bind_info = ODBCBindInfo;
    projection_pushdown = true;
    named_parameters["all_varchar"] = LogicalType::BOOLEAN;
}

struct AttachFunctionData : public TableFunctionData {
    AttachFunctionData() {}

    bool finished = false;
    bool overwrite = false;
    string dsn;
    string connection_string;
    string username;
    string password;
};

static unique_ptr<FunctionData> AttachBind(ClientContext &context, TableFunctionBindInput &input,
                                           vector<LogicalType> &return_types, vector<string> &names) {
    auto result = make_uniq<AttachFunctionData>();
    
    // Check which connection method to use
    if (input.inputs[0].type().id() == LogicalTypeId::VARCHAR) {
        auto conn_str = input.inputs[0].GetValue<string>();
        
        // Check if it's a DSN or connection string
        if (conn_str.find('=') == string::npos) {
            // Likely a DSN
            result->dsn = conn_str;
            
            // Check for optional username and password
            if (input.inputs.size() >= 2) {
                result->username = input.inputs[1].GetValue<string>();
            }
            
            if (input.inputs.size() >= 3) {
                result->password = input.inputs[2].GetValue<string>();
            }
        } else {
            // Connection string
            result->connection_string = conn_str;
        }
    } else {
        throw BinderException("First argument to ODBC attach must be a VARCHAR (DSN or connection string)");
    }
    
    // Process additional parameters
    for (auto &kv : input.named_parameters) {
        if (kv.first == "overwrite") {
            result->overwrite = BooleanValue::Get(kv.second);
        }
    }
    
    return_types.emplace_back(LogicalType::BOOLEAN);
    names.emplace_back("Success");
    return std::move(result);
}

static void AttachFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
    auto &data = data_p.bind_data->CastNoConst<AttachFunctionData>();
    if (data.finished) {
        return;
    }
    
    // Connect to the ODBC data source
    ODBCDB db;
    if (!data.dsn.empty()) {
        db = ODBCDB::OpenWithDSN(data.dsn, data.username, data.password);
    } else if (!data.connection_string.empty()) {
        db = ODBCDB::OpenWithConnectionString(data.connection_string);
    } else {
        throw std::runtime_error("No connection information provided");
    }
    
    // Get list of tables
    auto tables = db.GetTables();
    
    // Create connection to DuckDB
    auto dconn = Connection(context.db->GetDatabase(context));
    
    // Create views for each table
    for (auto &table_name : tables) {
        if (!data.dsn.empty()) {
            dconn.TableFunction("odbc_scan", {Value(table_name), Value(data.dsn), 
                                Value(data.username), Value(data.password)})
                ->CreateView(table_name, data.overwrite, false);
        } else {
            dconn.TableFunction("odbc_scan", {Value(table_name), Value(data.connection_string)})
                ->CreateView(table_name, data.overwrite, false);
        }
    }
    
    data.finished = true;
    
    // Set output
    output.SetCardinality(1);
    output.SetValue(0, 0, Value::BOOLEAN(true));
}

ODBCAttachFunction::ODBCAttachFunction()
    : TableFunction("odbc_attach", {LogicalType::VARCHAR}, AttachFunction, AttachBind) {
    named_parameters["overwrite"] = LogicalType::BOOLEAN;
}

static unique_ptr<FunctionData> QueryBind(ClientContext &context, TableFunctionBindInput &input,
                                       vector<LogicalType> &return_types, vector<string> &names) {
    auto result = make_uniq<ODBCBindData>();
    
    if (input.inputs[0].IsNull() || input.inputs[1].IsNull()) {
        throw BinderException("Parameters to odbc_query cannot be NULL");
    }
    
    // First parameter is either DSN or connection string
    auto conn_str = input.inputs[0].GetValue<string>();
    
    // Check if it's a DSN or connection string
    if (conn_str.find('=') == string::npos) {
        // Likely a DSN
        result->dsn = conn_str;
        
        // Check for optional username and password
        if (input.inputs.size() >= 3) {
            result->username = input.inputs[2].GetValue<string>();
        }
        
        if (input.inputs.size() >= 4) {
            result->password = input.inputs[3].GetValue<string>();
        }
    } else {
        // Connection string
        result->connection_string = conn_str;
    }
    
    // Second parameter is the SQL query
    result->sql = input.inputs[1].GetValue<string>();
    
    // Process additional parameters
    for (auto &kv : input.named_parameters) {
        if (kv.first == "all_varchar") {
            result->all_varchar = BooleanValue::Get(kv.second);
        }
    }
    
    // Connect to ODBC data source
    ODBCDB db;
    if (!result->dsn.empty()) {
        db = ODBCDB::OpenWithDSN(result->dsn, result->username, result->password);
    } else if (!result->connection_string.empty()) {
        db = ODBCDB::OpenWithConnectionString(result->connection_string);
    } else {
        throw BinderException("No connection information provided");
    }
    
    // Prepare statement to get column info
    auto stmt = db.Prepare(result->sql);
    if (!stmt.IsOpen()) {
        throw BinderException("Failed to prepare query");
    }
    
    // Get column information
    auto column_count = stmt.GetColumnCount();
    for (idx_t i = 0; i < column_count; i++) {
        auto column_name = stmt.GetName(i);
        auto column_type = result->all_varchar ? LogicalType::VARCHAR : 
                           GetDuckDBType(stmt.GetODBCType(i), 0, 0);
        
        names.push_back(column_name);
        return_types.push_back(column_type);
    }
    
    if (names.empty()) {
        throw BinderException("Query must return at least one column");
    }
    
    result->names = names;
    result->types = return_types;
    
    return std::move(result);
}

ODBCQueryFunction::ODBCQueryFunction()
    : TableFunction("odbc_query", {LogicalType::VARCHAR, LogicalType::VARCHAR}, ODBCScan, QueryBind,
                    ODBCInitGlobalState, ODBCInitLocalState) {
    projection_pushdown = false;  // Can't push projection to arbitrary queries
    named_parameters["all_varchar"] = LogicalType::BOOLEAN;
}

struct ODBCDriversBindData : public TableFunctionData {
    // Store drivers information at bind time
    std::vector<std::pair<std::string, std::string>> drivers; // (name, attributes)
};

struct ODBCDriversLocalState : public LocalTableFunctionState {
    idx_t offset = 0;  // Track current position in the drivers list
    bool done = false;
};

static unique_ptr<FunctionData> ODBCDriversBind(ClientContext &context, TableFunctionBindInput &input,
                                          vector<LogicalType> &return_types, vector<string> &names) {
    auto result = make_uniq<ODBCDriversBindData>();
    
    // Setup return types and names
    names.push_back("driver_name");
    return_types.push_back(LogicalType::VARCHAR);
    
    names.push_back("attributes");
    return_types.push_back(LogicalType::VARCHAR);
    
#ifdef _WIN32
    // On Windows, retrieve the list of drivers
    SQLHENV env;
    SQLRETURN ret;
    
    // Allocate environment handle
    ret = SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &env);
    if (ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO) {
        return result; // Return empty result on error
    }
    
    // Set ODBC version
    ret = SQLSetEnvAttr(env, SQL_ATTR_ODBC_VERSION, (SQLPOINTER)SQL_OV_ODBC3, 0);
    if (ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO) {
        SQLFreeHandle(SQL_HANDLE_ENV, env);
        return result; // Return empty result on error
    }
    
    // Retrieve driver information
    SQLCHAR driver_desc[256];
    SQLCHAR driver_attr[256];
    SQLSMALLINT desc_len, attr_len;
    
    ret = SQLDrivers(env, SQL_FETCH_FIRST, driver_desc, sizeof(driver_desc), &desc_len,
                    driver_attr, sizeof(driver_attr), &attr_len);
                    
    while (ret == SQL_SUCCESS || ret == SQL_SUCCESS_WITH_INFO) {
        // Add driver to the list
        std::string name((char*)driver_desc, desc_len);
        std::string attributes((char*)driver_attr, attr_len);
        
        result->drivers.push_back({name, attributes});
        
        // Get next driver
        ret = SQLDrivers(env, SQL_FETCH_NEXT, driver_desc, sizeof(driver_desc), &desc_len,
                       driver_attr, sizeof(driver_attr), &attr_len);
    }
    
    // Free environment handle
    SQLFreeHandle(SQL_HANDLE_ENV, env);
#endif
    
    return std::move(result);
}

static unique_ptr<GlobalTableFunctionState> ODBCDriversInitGlobalState(ClientContext &context,
                                                               TableFunctionInitInput &input) {
    return make_uniq<ODBCGlobalState>(1); // Single-threaded scan
}

static unique_ptr<LocalTableFunctionState>
ODBCDriversInitLocalState(ExecutionContext &context, TableFunctionInitInput &input, GlobalTableFunctionState *global_state) {
    return make_uniq<ODBCDriversLocalState>();
}

static void ODBCDriversScan(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
    auto &bind_data = data.bind_data->Cast<ODBCDriversBindData>();
    auto &local_state = data.local_state->Cast<ODBCDriversLocalState>();
    
    if (local_state.done) {
        return;
    }
    
    const auto &drivers = bind_data.drivers;
    idx_t remaining = drivers.size() - local_state.offset;
    
    // If no more data, return empty result
    if (remaining == 0) {
        local_state.done = true;
        output.SetCardinality(0);
        return;
    }
    
    // Calculate how many rows to return in this chunk
    idx_t count =  remaining;
    
    // Set up the vectors
    auto &name_vec = output.data[0];
    auto &attr_vec = output.data[1];
    
    // Fill the vectors with data
    for (idx_t i = 0; i < count; i++) {
        idx_t idx = local_state.offset + i;
        const auto &driver = drivers[idx];
        
        // Driver name
        FlatVector::GetData<string_t>(name_vec)[i] = StringVector::AddString(name_vec, driver.first);
        
        // Driver attributes
        FlatVector::GetData<string_t>(attr_vec)[i] = StringVector::AddString(attr_vec, driver.second);
    }
    
    // Update local state
    local_state.offset += count;
    if (local_state.offset >= drivers.size()) {
        local_state.done = true;
    }
    
    // Set the output cardinality
    output.SetCardinality(count);
}

ODBCDriversFunction::ODBCDriversFunction()
    : TableFunction("odbc_drivers", {}, ODBCDriversScan, ODBCDriversBind,
                    ODBCDriversInitGlobalState, ODBCDriversInitLocalState) {
    // No special parameters needed
}

} // namespace duckdb