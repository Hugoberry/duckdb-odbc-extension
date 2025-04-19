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
    return OdbcUtils::TypeToLogicalType(odbc_type, column_size, decimal_digits);
}

int GetODBCSQLType(const LogicalType &type) {
    return OdbcUtils::ToODBCType(type);
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
    OdbcDB db;
    try {
        if (!result->dsn.empty()) {
            db = OdbcDB::OpenWithDSN(result->dsn, result->username, result->password);
        } else if (!result->connection_string.empty()) {
            db = OdbcDB::OpenWithConnectionString(result->connection_string);
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
    } catch (const nanodbc::database_error& e) {
        throw BinderException("ODBC error during binding: " + OdbcUtils::HandleException(e));
    } catch (const std::exception& e) {
        throw BinderException(e.what());
    }
    
    return std::move(result);
}

static unique_ptr<LocalTableFunctionState>
ODBCInitLocalState(ExecutionContext &context, TableFunctionInitInput &input, GlobalTableFunctionState *global_state) {
    auto &bind_data = input.bind_data->Cast<ODBCBindData>();
    auto result = make_uniq<ODBCLocalState>();
    
    result->column_ids = input.column_ids;
    
    // If we have a global database connection, use it
    result->db = bind_data.global_db;
    
    try {
        if (!result->db) {
            // Otherwise create a new connection
            if (!bind_data.dsn.empty()) {
                result->owned_db = OdbcDB::OpenWithDSN(bind_data.dsn, bind_data.username, bind_data.password);
            } else if (!bind_data.connection_string.empty()) {
                result->owned_db = OdbcDB::OpenWithConnectionString(bind_data.connection_string);
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
                                                    : '"' + OdbcUtils::SanitizeString(bind_data.names[column_id]) + '"';
                });
                
            sql = StringUtil::Format("SELECT %s FROM \"%s\"", col_names, 
                                    OdbcUtils::SanitizeString(bind_data.table_name));
        } else {
            sql = bind_data.sql;
        }
        
        result->stmt = result->db->Prepare(sql.c_str());
        result->done = false;
    } catch (const nanodbc::database_error& e) {
        throw std::runtime_error("ODBC error during initialization: " + OdbcUtils::HandleException(e));
    } catch (const std::exception& e) {
        throw std::runtime_error(e.what());
    }
    
    return std::move(result);
}

static unique_ptr<GlobalTableFunctionState> ODBCInitGlobalState(ClientContext &context,
                                                                TableFunctionInitInput &input) {
    return make_uniq<ODBCGlobalState>(1); // Single-threaded scan for now
}

static void ODBCScan(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
    auto &state = data.local_state->Cast<ODBCLocalState>();
    if (state.done) {
        output.SetCardinality(0);
        return;
    }
    
    // Fetch rows and populate the DataChunk
    idx_t out_idx = 0;
    while (out_idx < STANDARD_VECTOR_SIZE) {
        if (!state.stmt.Step()) {
            // No more rows to fetch
            state.done = true;
            break;
        }
        state.scan_count++;
        
        try {
            // Process each column
            for (idx_t col_idx = 0; col_idx < output.ColumnCount(); col_idx++) {
                auto &out_vec = output.data[col_idx];
                
                // Using nanodbc result to get data
                auto &result = state.stmt.result;
                
                // Check for NULL
                if (result.is_null(col_idx)) {
                    FlatVector::Validity(out_vec).Set(out_idx, false);
                    continue;
                }
                
                // Based on the output vector type, convert and fetch the data
                switch (out_vec.GetType().id()) {
                    case LogicalTypeId::BOOLEAN: {
                        FlatVector::GetData<bool>(out_vec)[out_idx] = (result.get<int>(col_idx) != 0);
                        break;
                    }
                    case LogicalTypeId::TINYINT: {
                        FlatVector::GetData<int8_t>(out_vec)[out_idx] = static_cast<int8_t>(result.get<int>(col_idx));
                        break;
                    }
                    case LogicalTypeId::SMALLINT: {
                        FlatVector::GetData<int16_t>(out_vec)[out_idx] = result.get<int16_t>(col_idx);
                        break;
                    }
                    case LogicalTypeId::INTEGER: {
                        FlatVector::GetData<int32_t>(out_vec)[out_idx] = result.get<int32_t>(col_idx);
                        break;
                    }
                    case LogicalTypeId::BIGINT: {
                        FlatVector::GetData<int64_t>(out_vec)[out_idx] = result.get<int64_t>(col_idx);
                        break;
                    }
                    case LogicalTypeId::FLOAT: {
                        FlatVector::GetData<float>(out_vec)[out_idx] = result.get<float>(col_idx);
                        break;
                    }
                    case LogicalTypeId::DOUBLE: {
                        FlatVector::GetData<double>(out_vec)[out_idx] = result.get<double>(col_idx);
                        break;
                    }
                    case LogicalTypeId::DECIMAL: {
                        auto &decimal_type = out_vec.GetType();
                        
                        // Get precision and scale
                        uint8_t width = DecimalType::GetWidth(decimal_type);
                        uint8_t scale = DecimalType::GetScale(decimal_type);
                        
                        // Get column metadata for backup precision/scale
                        SQLSMALLINT odbc_type;
                        SQLULEN column_size;
                        SQLSMALLINT decimal_digits;
                        OdbcUtils::GetColumnMetadata(result, col_idx, odbc_type, column_size, decimal_digits);
                        
                        // Use backup values if needed
                        if (width == 0) width = column_size;
                        if (scale == 0) scale = decimal_digits;
                        if (width == 0) width = 38;  // Default width
                        if (scale == 0) scale = 2;   // Default scale
                        
                        // Get value as string to preserve precision
                        std::string decimal_str = result.get<std::string>(col_idx);
                        
                        // Process based on target storage type
                        bool success = false;
                        switch (decimal_type.InternalType()) {
                            case PhysicalType::INT16: {
                                int16_t result_val;
                                success = TryDecimalStringCast<int16_t>(decimal_str.c_str(), decimal_str.length(), result_val, width, scale);
                                if (success) {
                                    FlatVector::GetData<int16_t>(out_vec)[out_idx] = result_val;
                                }
                                break;
                            }
                            case PhysicalType::INT32: {
                                int32_t result_val;
                                success = TryDecimalStringCast<int32_t>(decimal_str.c_str(), decimal_str.length(), result_val, width, scale);
                                if (success) {
                                    FlatVector::GetData<int32_t>(out_vec)[out_idx] = result_val;
                                }
                                break;
                            }
                            case PhysicalType::INT64: {
                                int64_t result_val;
                                success = TryDecimalStringCast<int64_t>(decimal_str.c_str(), decimal_str.length(), result_val, width, scale);
                                if (success) {
                                    FlatVector::GetData<int64_t>(out_vec)[out_idx] = result_val;
                                }
                                break;
                            }
                            case PhysicalType::INT128: {
                                hugeint_t result_val;
                                success = TryDecimalStringCast<hugeint_t>(decimal_str.c_str(), decimal_str.length(), result_val, width, scale);
                                if (success) {
                                    FlatVector::GetData<hugeint_t>(out_vec)[out_idx] = result_val;
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
                        std::string str_val = result.get<std::string>(col_idx);
                        FlatVector::GetData<string_t>(out_vec)[out_idx] = 
                            StringVector::AddString(out_vec, str_val);
                        break;
                    }
                    case LogicalTypeId::DATE: {
                        nanodbc::date date_val = result.get<nanodbc::date>(col_idx);
                        FlatVector::GetData<date_t>(out_vec)[out_idx] = Date::FromDate(date_val.year, date_val.month, date_val.day);
                        break;
                    }
                    case LogicalTypeId::TIME: {
                        nanodbc::time time_val = result.get<nanodbc::time>(col_idx);
                        FlatVector::GetData<dtime_t>(out_vec)[out_idx] = Time::FromTime(time_val.hour, time_val.min, time_val.sec, 0);
                        break;
                    }
                    case LogicalTypeId::TIMESTAMP: {
                        nanodbc::timestamp ts_val = result.get<nanodbc::timestamp>(col_idx);
                        date_t date_val = Date::FromDate(ts_val.year, ts_val.month, ts_val.day);
                        dtime_t time_val = Time::FromTime(ts_val.hour, ts_val.min, ts_val.sec, ts_val.fract / 1000000);
                        
                        FlatVector::GetData<timestamp_t>(out_vec)[out_idx] = Timestamp::FromDatetime(date_val, time_val);
                        break;
                    }
                    case LogicalTypeId::UUID: {
                        // UUIDs are typically handled as strings in ODBC
                        std::string uuid_str = result.get<std::string>(col_idx);
                        
                        // Convert string UUID to DuckDB UUID format
                        try {
                            hugeint_t uuid_value;
                            if (UUID::FromString(uuid_str, uuid_value)) {
                                FlatVector::GetData<hugeint_t>(out_vec)[out_idx] = uuid_value;
                            } else {
                                // If parsing fails, set to NULL
                                FlatVector::Validity(out_vec).Set(out_idx, false);
                            }
                        } catch (...) {
                            // If any error occurs during conversion, set to NULL
                            FlatVector::Validity(out_vec).Set(out_idx, false);
                        }
                        break;
                    }
                    case LogicalTypeId::BLOB: {
                        std::vector<char> blob_data;
                        bool isNull = false;
                        
                        if (OdbcUtils::ReadVarColumn(result, col_idx, isNull, blob_data)) {
                            if (isNull) {
                                FlatVector::Validity(out_vec).Set(out_idx, false);
                            } else if (blob_data.empty()) {
                                FlatVector::GetData<string_t>(out_vec)[out_idx] = 
                                    StringVector::AddStringOrBlob(out_vec, "", 0);
                            } else {
                                FlatVector::GetData<string_t>(out_vec)[out_idx] = 
                                    StringVector::AddStringOrBlob(out_vec, blob_data.data(), blob_data.size());
                            }
                        } else {
                            FlatVector::Validity(out_vec).Set(out_idx, false);
                        }
                        break;
                    }
                    default:
                        throw std::runtime_error("Unsupported ODBC to DuckDB type conversion: " + 
                                               out_vec.GetType().ToString());
                }
            }
        } catch (const nanodbc::database_error& e) {
            throw std::runtime_error("ODBC error during data scan: " + OdbcUtils::HandleException(e));
        } catch (const std::exception& e) {
            throw std::runtime_error(e.what());
        }
        
        out_idx++;
    }
    // Set the cardinality of the output chunk
    output.SetCardinality(out_idx);
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
    named_parameters["all_varchar"] = LogicalType(LogicalTypeId::BOOLEAN);
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
    
    return_types.emplace_back(LogicalType(LogicalTypeId::BOOLEAN));
    names.emplace_back("Success");
    return std::move(result);
}

static void AttachFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
    auto &data = data_p.bind_data->CastNoConst<AttachFunctionData>();
    if (data.finished) {
        return;
    }
    
    try {
        // Connect to the ODBC data source
        OdbcDB db;
        if (!data.dsn.empty()) {
            db = OdbcDB::OpenWithDSN(data.dsn, data.username, data.password);
        } else if (!data.connection_string.empty()) {
            db = OdbcDB::OpenWithConnectionString(data.connection_string);
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
    } catch (const nanodbc::database_error& e) {
        throw std::runtime_error("ODBC error during attach: " + OdbcUtils::HandleException(e));
    } catch (const std::exception& e) {
        throw std::runtime_error(e.what());
    }
}

ODBCAttachFunction::ODBCAttachFunction()
    : TableFunction("odbc_attach", {LogicalType::VARCHAR}, AttachFunction, AttachBind) {
    named_parameters["overwrite"] = LogicalType(LogicalTypeId::BOOLEAN);
}

static unique_ptr<FunctionData> QueryBind(ClientContext &context, TableFunctionBindInput &input,
                                          vector<LogicalType> &return_types, vector<string> &names) {
    auto result = make_uniq<ODBCBindData>();

    // NULL checks
    if (input.inputs[0].IsNull() || input.inputs[1].IsNull()) {
        throw BinderException("Parameters to odbc_query cannot be NULL");
    }

    // Parse connection info
    auto conn_str = input.inputs[0].GetValue<string>();
    if (conn_str.find('=') == string::npos) {
        result->dsn = conn_str;
        if (input.inputs.size() >= 3) {
            result->username = input.inputs[2].GetValue<string>();
        }
        if (input.inputs.size() >= 4) {
            result->password = input.inputs[3].GetValue<string>();
        }
    } else {
        result->connection_string = conn_str;
    }
    result->sql = input.inputs[1].GetValue<string>();

    // all_varchar flag
    for (auto &kv : input.named_parameters) {
        if (kv.first == "all_varchar") {
            result->all_varchar = BooleanValue::Get(kv.second);
        }
    }

    try {
        // 1) Connect
        OdbcDB db;
        if (!result->dsn.empty()) {
            db = OdbcDB::OpenWithDSN(result->dsn, result->username, result->password);
        } else {
            db = OdbcDB::OpenWithConnectionString(result->connection_string);
        }

        // 2) Prepare
        auto stmt = db.Prepare(result->sql);
        if (!stmt.IsOpen()) {
            throw BinderException("Failed to prepare query");
        }

        // 3) Step once to populate metadata
        bool has_data = stmt.Step();
        auto column_count = stmt.GetColumnCount();

        // 4) Pull column names/types
        for (idx_t i = 0; i < column_count; i++) {
            auto col_name = stmt.GetName(i);
            SQLULEN size = 0;
            SQLSMALLINT digits = 0;
            SQLSMALLINT odbc_type = stmt.GetODBCType(i, &size, &digits);

            auto duck_type = result->all_varchar
                             ? LogicalType::VARCHAR
                             : GetDuckDBType(odbc_type, size, digits);

            names.push_back(col_name);
            return_types.push_back(duck_type);
        }

    } catch (const nanodbc::database_error &e) {
        throw BinderException("ODBC error during query bind: " + OdbcUtils::HandleException(e));
    } catch (const std::exception &e) {
        throw BinderException(e.what());
    }

    result->names = names;
    result->types = return_types;
    return std::move(result);
}

ODBCQueryFunction::ODBCQueryFunction()
    : TableFunction("odbc_query", {LogicalType::VARCHAR, LogicalType::VARCHAR}, ODBCScan, QueryBind,
                    ODBCInitGlobalState, ODBCInitLocalState) {
    projection_pushdown = false;  // Can't push projection to arbitrary queries
    named_parameters["all_varchar"] = LogicalType(LogicalTypeId::BOOLEAN);
}

} // namespace duckdb