#include "odbc_scanner.hpp"
#include "odbc_utils.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/common/types/date.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/common/types/time.hpp"
#include "duckdb/common/types/uuid.hpp"
#include "duckdb/storage/statistics/base_statistics.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/main/attached_database.hpp"
#include <cmath>

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

            SQLULEN column_size = 0;
            SQLSMALLINT decimal_digits = 0;
            SQLSMALLINT odbc_type = stmt.GetODBCType(col_idx, &column_size, &decimal_digits);
            
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
                case LogicalTypeId::DECIMAL: {
                    auto &decimal_type = out_vec.GetType();
                    // Get width and scale from both sources
                    uint8_t bind_width = DecimalType::GetWidth(decimal_type);
                    uint8_t bind_scale = DecimalType::GetScale(decimal_type);
                    
                    // Use scan-time information as backup if binding didn't capture it correctly
                    uint8_t width = (bind_width > 0) ? bind_width : column_size;
                    uint8_t scale = (bind_scale > 0) ? bind_scale : decimal_digits;
                    
                    // Still apply defaults if necessary
                    if (width == 0) width = 38;  // Default width
                    if (scale == 0) scale = 2;   // Default scale

                    // Always use string representation for decimals to preserve exact values
                    std::vector<char> decimalData;
                    bool isNull = false;
                    
                    if (!ODBCUtils::ReadVarColumn(stmt.hstmt, col_idx + 1, SQL_C_CHAR, isNull, decimalData)) {
                        FlatVector::Validity(out_vec).Set(out_idx, false);
                        break;
                    }
                    
                    if (isNull) {
                        FlatVector::Validity(out_vec).Set(out_idx, false);
                        break;
                    }
                    
                    // Ensure null-termination for string processing
                    if (!decimalData.empty() && decimalData.back() != '\0') {
                        decimalData.push_back('\0');
                    }
                    
                    // Process based on target storage type
                    bool success = false;
                    switch (decimal_type.InternalType()) {
                        case PhysicalType::INT16: {
                            int16_t result;
                            success = TryDecimalStringCast<int16_t>(decimalData.data(), decimalData.size() - 1, result, width, scale);
                            if (success) {
                                FlatVector::GetData<int16_t>(out_vec)[out_idx] = result;
                            }
                            break;
                        }
                        case PhysicalType::INT32: {
                            int32_t result;
                            success = TryDecimalStringCast<int32_t>(decimalData.data(), decimalData.size() - 1, result, width, scale);
                            if (success) {
                                FlatVector::GetData<int32_t>(out_vec)[out_idx] = result;
                            }
                            break;
                        }
                        case PhysicalType::INT64: {
                            int64_t result;
                            success = TryDecimalStringCast<int64_t>(decimalData.data(), decimalData.size() - 1, result, width, scale);
                            if (success) {
                                FlatVector::GetData<int64_t>(out_vec)[out_idx] = result;
                            }
                            break;
                        }
                        case PhysicalType::INT128: {
                            hugeint_t result;
                            success = TryDecimalStringCast<hugeint_t>(decimalData.data(), decimalData.size() - 1, result, width, scale);
                            if (success) {
                                FlatVector::GetData<hugeint_t>(out_vec)[out_idx] = result;
                            }
                            break;
                        }
                        default:
                            throw InternalException("Unsupported decimal storage type");
                    }
                    
                    if (!success) {
                        FlatVector::Validity(out_vec).Set(out_idx, false);
                    }
                    break;
                }
                case LogicalTypeId::VARCHAR: {
                    std::vector<char> strData;
                    bool isNull = false;
                    
                    SQLSMALLINT ctype = ODBCUtils::IsWideType(odbc_type) ? SQL_C_WCHAR : SQL_C_CHAR;
                    if (!ODBCUtils::ReadVarColumn(stmt.hstmt, col_idx + 1, ctype, isNull, strData)) {
                        auto &mask = FlatVector::Validity(out_vec);
                        mask.Set(out_idx, false);
                        break;
                    }
                    
                    if (isNull) {
                        auto &mask = FlatVector::Validity(out_vec);
                        mask.Set(out_idx, false);
                    } else if (strData.empty()) {
                        FlatVector::GetData<string_t>(out_vec)[out_idx] = 
                            StringVector::AddString(out_vec, "");
                    } else {
                        FlatVector::GetData<string_t>(out_vec)[out_idx] = 
                            StringVector::AddString(out_vec, strData.data(), strData.size());
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
                case LogicalTypeId::UUID: {
                    // UUIDs in ODBC are typically returned as strings in standard format
                    char buffer[37]; // 36 chars for UUID string + null terminator
                    SQLLEN bytes_read;
                    
                    // Fetch the UUID as a string
                    SQLGetData(stmt.hstmt, col_idx + 1, SQL_C_CHAR, buffer, sizeof(buffer), &bytes_read);
                    
                    if (bytes_read > 0 && bytes_read < sizeof(buffer)) {
                        // Convert string UUID to DuckDB UUID format
                        try {
                            // Ensure null termination
                            buffer[bytes_read] = '\0';
                            
                            // Parse the UUID string
                            hugeint_t uuid_value;
                            if (UUID::FromString(string(buffer), uuid_value)) {
                                FlatVector::GetData<hugeint_t>(out_vec)[out_idx] = uuid_value;
                            } else {
                                // If parsing fails, set to NULL
                                auto &mask = FlatVector::Validity(out_vec);
                                mask.Set(out_idx, false);
                            }
                        } catch (...) {
                            // If any error occurs during conversion, set to NULL
                            auto &mask = FlatVector::Validity(out_vec);
                            mask.Set(out_idx, false);
                        }
                    } else if (bytes_read >= sizeof(buffer)) {
                        // UUID string is too long (should never happen for valid UUIDs)
                        auto &mask = FlatVector::Validity(out_vec);
                        mask.Set(out_idx, false);
                    }
                    break;
                }
                case LogicalTypeId::BLOB: {
                    std::vector<char> blobData;
                    bool isNull = false;
                    
                    if (!ODBCUtils::ReadVarColumn(stmt.hstmt, col_idx + 1, SQL_C_BINARY, isNull, blobData)) {
                        auto &mask = FlatVector::Validity(out_vec);
                        mask.Set(out_idx, false);
                        break;
                    }
                    
                    if (isNull) {
                        auto &mask = FlatVector::Validity(out_vec);
                        mask.Set(out_idx, false);
                    } else if (blobData.empty()) {
                        FlatVector::GetData<string_t>(out_vec)[out_idx] = 
                            StringVector::AddStringOrBlob(out_vec, "", 0);
                    } else {
                        FlatVector::GetData<string_t>(out_vec)[out_idx] = 
                            StringVector::AddStringOrBlob(out_vec, blobData.data(), blobData.size());
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
        // Get full type information
        SQLULEN column_size = 0;
        SQLSMALLINT decimal_digits = 0;
        SQLSMALLINT odbc_type = stmt.GetODBCType(i, &column_size, &decimal_digits);
        
        auto column_type = result->all_varchar ? LogicalType::VARCHAR : 
                        GetDuckDBType(odbc_type, column_size, decimal_digits);
        
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

} // namespace duckdb