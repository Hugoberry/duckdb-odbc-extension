#include "odbc_scanner.hpp"
#include "odbc_parameters.hpp"
#include "odbc_utils.hpp"
#include "odbc_encoding.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/common/types/date.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/common/types/time.hpp"
#include "duckdb/common/types/uuid.hpp"
#include "duckdb/storage/statistics/base_statistics.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/common/string_util.hpp"

namespace duckdb {

//------------------------------------------------------------------------------
// OdbcTableFunction implementations
//------------------------------------------------------------------------------

TableFunction OdbcTableFunction::CreateScanFunction() {
    TableFunction result("odbc_scan", {}, ScanOdbcSource);
    
    // Setup binding function
    result.bind = [](ClientContext &context, TableFunctionBindInput &input,
                     vector<LogicalType> &return_types, vector<string> &names) -> unique_ptr<FunctionData> {
        return BindOdbcFunction(context, input, return_types, names, OdbcOperation::SCAN);
    };
    
    // Setup state initialization
    result.init_global = InitOdbcGlobalState;
    result.init_local = InitOdbcLocalState;
    result.projection_pushdown = true;
    
    // Add named parameters
    result.named_parameters["connection"] = LogicalType(LogicalTypeId::VARCHAR);
    result.named_parameters["table_name"] = LogicalType(LogicalTypeId::VARCHAR);
    result.named_parameters["schema_name"] = LogicalType(LogicalTypeId::VARCHAR);
    result.named_parameters["username"] = LogicalType(LogicalTypeId::VARCHAR);
    result.named_parameters["password"] = LogicalType(LogicalTypeId::VARCHAR);
    result.named_parameters["all_varchar"] = LogicalType(LogicalTypeId::BOOLEAN);
    result.named_parameters["encoding"] = LogicalType(LogicalTypeId::VARCHAR);
    result.named_parameters["timeout"] = LogicalType(LogicalTypeId::INTEGER);
    result.named_parameters["read_only"] = LogicalType(LogicalTypeId::BOOLEAN);
    
    return result;
}

TableFunction OdbcTableFunction::CreateAttachFunction() {
    TableFunction result("odbc_attach", {}, AttachOdbcDatabase);
    
    // Setup binding function
    result.bind = [](ClientContext &context, TableFunctionBindInput &input,
                     vector<LogicalType> &return_types, vector<string> &names) -> unique_ptr<FunctionData> {
        return BindOdbcFunction(context, input, return_types, names, OdbcOperation::ATTACH);
    };
    
    // Add named parameters
    result.named_parameters["connection"] = LogicalType(LogicalTypeId::VARCHAR);
    result.named_parameters["username"] = LogicalType(LogicalTypeId::VARCHAR);
    result.named_parameters["password"] = LogicalType(LogicalTypeId::VARCHAR);
    result.named_parameters["all_varchar"] = LogicalType(LogicalTypeId::BOOLEAN);
    result.named_parameters["overwrite"] = LogicalType(LogicalTypeId::BOOLEAN);
    result.named_parameters["encoding"] = LogicalType(LogicalTypeId::VARCHAR);
    result.named_parameters["timeout"] = LogicalType(LogicalTypeId::INTEGER);
    result.named_parameters["read_only"] = LogicalType(LogicalTypeId::BOOLEAN);
    
    return result;
}

TableFunction OdbcTableFunction::CreateQueryFunction() {
    TableFunction result("odbc_query", {}, ScanOdbcSource);
    
    // Setup binding function
    result.bind = [](ClientContext &context, TableFunctionBindInput &input,
                     vector<LogicalType> &return_types, vector<string> &names) -> unique_ptr<FunctionData> {
        return BindOdbcFunction(context, input, return_types, names, OdbcOperation::QUERY);
    };
    
    // Setup state initialization
    result.init_global = InitOdbcGlobalState;
    result.init_local = InitOdbcLocalState;
    result.projection_pushdown = false;
    
    // Add named parameters
    result.named_parameters["connection"] = LogicalType(LogicalTypeId::VARCHAR);
    result.named_parameters["query"] = LogicalType(LogicalTypeId::VARCHAR);
    result.named_parameters["username"] = LogicalType(LogicalTypeId::VARCHAR);
    result.named_parameters["password"] = LogicalType(LogicalTypeId::VARCHAR);
    result.named_parameters["all_varchar"] = LogicalType(LogicalTypeId::BOOLEAN);
    result.named_parameters["encoding"] = LogicalType(LogicalTypeId::VARCHAR);
    result.named_parameters["timeout"] = LogicalType(LogicalTypeId::INTEGER);
    result.named_parameters["read_only"] = LogicalType(LogicalTypeId::BOOLEAN);
    
    return result;
}

TableFunction OdbcTableFunction::CreateExecFunction() {
    TableFunction result("odbc_exec", {}, ExecuteOdbcStatement);
    
    // Setup binding function
    result.bind = [](ClientContext &context, TableFunctionBindInput &input,
                     vector<LogicalType> &return_types, vector<string> &names) -> unique_ptr<FunctionData> {
        return BindOdbcFunction(context, input, return_types, names, OdbcOperation::EXEC);
    };
    
    // Add named parameters
    result.named_parameters["connection"] = LogicalType(LogicalTypeId::VARCHAR);
    result.named_parameters["sql"] = LogicalType(LogicalTypeId::VARCHAR);
    result.named_parameters["username"] = LogicalType(LogicalTypeId::VARCHAR);
    result.named_parameters["password"] = LogicalType(LogicalTypeId::VARCHAR);
    result.named_parameters["encoding"] = LogicalType(LogicalTypeId::VARCHAR);
    result.named_parameters["timeout"] = LogicalType(LogicalTypeId::INTEGER);
    result.named_parameters["read_only"] = LogicalType(LogicalTypeId::BOOLEAN);
    
    return result;
}

//------------------------------------------------------------------------------
// Binding Functions
//------------------------------------------------------------------------------

unique_ptr<FunctionData> BindOdbcFunction(ClientContext &context, TableFunctionBindInput &input,
                                        vector<LogicalType> &return_types, vector<string> &names,
                                        OdbcOperation operation) {
    switch (operation) {
        case OdbcOperation::SCAN: {
            auto result = OdbcFunctionDataFactory::CreateScannerState(input);
            
            // Connect to data source and get schema
            try {
                auto db = OdbcConnection::Connect(result->connection_params);
                
                // Get table information
                ColumnList columns;
                std::vector<std::unique_ptr<Constraint>> constraints;
                db->GetTableInfo(result->table_name, result->schema_name, columns, constraints, result->options.all_varchar);
                
                // Map column types and names
                for (auto &column : columns.Logical()) {
                    names.push_back(column.GetName());
                    return_types.push_back(column.GetType());
                }
                
                if (names.empty()) {
                    throw BinderException("No columns found for table " + result->table_name);
                }
                
                result->column_names = names;
                result->column_types = return_types;
                
            } catch (const nanodbc::database_error& e) {
                OdbcUtils::ThrowException("bind scan function", e);
            }
            
            return std::move(result);
        }
        
        case OdbcOperation::QUERY: {
            auto result = OdbcFunctionDataFactory::CreateQueryState(input);
            
            // Connect to data source and get schema
            try {
                auto db = OdbcConnection::Connect(result->connection_params);
                auto stmt = db->Prepare(result->sql);
                
                // Get column information
                auto columnCount = stmt->GetColumnCount();
                
                if (columnCount == 0) {
                    // DDL statement - add success column
                    names.push_back("Success");
                    return_types.push_back(LogicalType(LogicalTypeId::BOOLEAN));
                } else {
                    // Regular query with results
                    for (idx_t i = 0; i < columnCount; i++) {
                        auto colName = stmt->GetName(i);
                        SQLULEN size = 0;
                        SQLSMALLINT digits = 0;
                        SQLSMALLINT odbcType = stmt->GetOdbcType(i, &size, &digits);

                        auto duckType = result->options.all_varchar ? 
                                      LogicalType::VARCHAR : 
                                      OdbcUtils::OdbcTypeToLogicalType(odbcType, size, digits);
                        
                        names.push_back(colName);
                        return_types.push_back(duckType);
                    }
                }
                
                result->column_names = names;
                result->column_types = return_types;
                
            } catch (const nanodbc::database_error& e) {
                OdbcUtils::ThrowException("bind query function", e);
            }
            
            return std::move(result);
        }
        
        case OdbcOperation::EXEC: {
            auto result = OdbcFunctionDataFactory::CreateExecData(input);
            
            // Set up return types (single boolean column)
            return_types.emplace_back(LogicalTypeId::BOOLEAN);
            names.emplace_back("Success");
            
            return std::move(result);
        }
        
        case OdbcOperation::ATTACH: {
            auto result = OdbcFunctionDataFactory::CreateAttachData(input);
            
            // Set up return types (single boolean column)
            return_types.emplace_back(LogicalTypeId::BOOLEAN);
            names.emplace_back("Success");
            
            return std::move(result);
        }
        
        default:
            throw NotImplementedException("Unsupported ODBC operation");
    }
}

//------------------------------------------------------------------------------
// State Initialization
//------------------------------------------------------------------------------

unique_ptr<GlobalTableFunctionState> InitOdbcGlobalState(ClientContext &context, TableFunctionInitInput &input) {
    return make_uniq<OdbcGlobalScanState>(1); // Single-threaded scan for now
}

unique_ptr<LocalTableFunctionState> InitOdbcLocalState(ExecutionContext &context, TableFunctionInitInput &input, 
                                                     GlobalTableFunctionState *global_state) {
    auto &bind_data = input.bind_data->Cast<OdbcScannerState>();
    auto result = make_uniq<OdbcLocalScanState>();
    
    // Store column IDs from input
    result->column_ids = input.column_ids;
    
    try {
        // Create connection
        result->connection = OdbcConnection::Connect(bind_data.connection_params);
        
        // Special handling for DDL statements
        if (bind_data.column_names.size() == 1 && bind_data.column_names[0] == "Success") {
            try {
                result->connection->Execute(bind_data.sql);
                result->done = false;
            } catch (const nanodbc::database_error& e) {
                OdbcUtils::ThrowException("execute statement", e);
            }
            return std::move(result);
        }
        
        // Prepare the query
        std::string sql;
        if (bind_data.sql.empty()) {
            // Build query based on column IDs
            auto colNames = StringUtil::Join(
                result->column_ids.data(), result->column_ids.size(), ", ", [&](const idx_t columnId) {
                    return columnId == (column_t)-1 ? "NULL"
                                                  : '"' + OdbcUtils::SanitizeString(bind_data.column_names[columnId]) + '"';
                });

            // Simple schema check - format with or without schema
            if (!bind_data.schema_name.empty()) {
                sql = StringUtil::Format("SELECT %s FROM \"%s\".\"%s\"", 
                                       colNames,
                                       OdbcUtils::SanitizeString(bind_data.schema_name),
                                       OdbcUtils::SanitizeString(bind_data.table_name));
            } else {
                sql = StringUtil::Format("SELECT %s FROM \"%s\"", 
                                       colNames,
                                       OdbcUtils::SanitizeString(bind_data.table_name));
            }
        } else {
            sql = bind_data.sql;
        }
        
        // Prepare the statement
        result->statement = result->connection->Prepare(sql);
        result->done = false;
    } catch (const nanodbc::database_error& e) {
        OdbcUtils::ThrowException("initialize scanner", e);
    }
    
    return std::move(result);
}

//------------------------------------------------------------------------------
// Scan Function
//------------------------------------------------------------------------------

void ScanOdbcSource(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
    auto &state = data.local_state->Cast<OdbcLocalScanState>();
    auto &bind_data = data.bind_data->Cast<OdbcScannerState>();
    
    if (state.done) {
        output.SetCardinality(0);
        return;
    }

    // Handle DDL statements
    if (output.ColumnCount() == 1 && 
        bind_data.column_names.size() == 1 && 
        bind_data.column_names[0] == "Success") {
        
        FlatVector::GetData<bool>(output.data[0])[0] = true;
        output.SetCardinality(1);
        state.done = true;
        return;
    }
    
    // Fetch rows and populate the DataChunk
    idx_t out_idx = 0;
    while (out_idx < STANDARD_VECTOR_SIZE) {
        if (!state.statement->Step()) {
            state.done = true;
            break;
        }
        state.scan_count++;
        
        // Process each column
        for (idx_t col_idx = 0; col_idx < output.ColumnCount(); col_idx++) {
            auto &out_vec = output.data[col_idx];
            
            // Check for NULL
            if (state.statement->result.is_null(col_idx)) {
                FlatVector::Validity(out_vec).Set(out_idx, false);
                continue;
            }
            
            // Based on the output vector type, convert and fetch the data
            switch (out_vec.GetType().id()) {
                case LogicalTypeId::VARCHAR: {
                    std::string str_val = state.statement->result.get<std::string>((short)col_idx);
                    // Apply encoding conversion if needed
                    if (OdbcEncoding::NeedsConversion(bind_data.options.encoding)) {
                        str_val = OdbcEncoding::ConvertToUTF8(str_val, bind_data.options.encoding);
                    }
                    FlatVector::GetData<string_t>(out_vec)[out_idx] = 
                        StringVector::AddString(out_vec, str_val);
                    break;
                }
                
                case LogicalTypeId::BOOLEAN:
                    FlatVector::GetData<bool>(out_vec)[out_idx] = (state.statement->result.get<int32_t>((short)col_idx) != 0);
                    break;
                    
                case LogicalTypeId::TINYINT:
                    FlatVector::GetData<int8_t>(out_vec)[out_idx] = static_cast<int8_t>(state.statement->result.get<int32_t>((short)col_idx));
                    break;
                    
                case LogicalTypeId::SMALLINT:
                    FlatVector::GetData<int16_t>(out_vec)[out_idx] = static_cast<int16_t>(state.statement->result.get<int32_t>((short)col_idx));
                    break;
                    
                case LogicalTypeId::INTEGER:
                    FlatVector::GetData<int32_t>(out_vec)[out_idx] = state.statement->result.get<int32_t>((short)col_idx);
                    break;
                    
                case LogicalTypeId::BIGINT:
                    FlatVector::GetData<int64_t>(out_vec)[out_idx] = state.statement->result.get<int64_t>((short)col_idx);
                    break;
                    
                case LogicalTypeId::FLOAT:
                    FlatVector::GetData<float>(out_vec)[out_idx] = state.statement->result.get<float>((short)col_idx);
                    break;
                    
                case LogicalTypeId::DOUBLE:
                    FlatVector::GetData<double>(out_vec)[out_idx] = state.statement->result.get<double>((short)col_idx);
                    break;
                    
                case LogicalTypeId::DECIMAL: {
                    auto &decimal_type = out_vec.GetType();
                    uint8_t width = DecimalType::GetWidth(decimal_type);
                    uint8_t scale = DecimalType::GetScale(decimal_type);
                    
                    double decimal_val = state.statement->result.get<double>((short)col_idx);
                    double scaled_val = decimal_val * pow(10, scale);
                    
                    switch (decimal_type.InternalType()) {
                        case PhysicalType::INT16:
                            FlatVector::GetData<int16_t>(out_vec)[out_idx] = (int16_t)round(scaled_val);
                            break;
                        case PhysicalType::INT32:
                            FlatVector::GetData<int32_t>(out_vec)[out_idx] = (int32_t)round(scaled_val);
                            break;
                        case PhysicalType::INT64:
                            FlatVector::GetData<int64_t>(out_vec)[out_idx] = (int64_t)round(scaled_val);
                            break;
                        case PhysicalType::INT128:
                            FlatVector::GetData<hugeint_t>(out_vec)[out_idx] = (hugeint_t)round(scaled_val);
                            break;
                        default:
                            FlatVector::Validity(out_vec).Set(out_idx, false);
                            break;
                    }
                    break;
                }
                
                case LogicalTypeId::DATE: {
                    nanodbc::date dt = state.statement->result.get<nanodbc::date>((short)col_idx);
                    FlatVector::GetData<date_t>(out_vec)[out_idx] = Date::FromDate(dt.year,dt.month,dt.day);
                    break;
                }
                
                case LogicalTypeId::TIME: {
                    nanodbc::time ti = state.statement->result.get<nanodbc::time>((short)col_idx);
                    FlatVector::GetData<dtime_t>(out_vec)[out_idx] = Time::FromTime(ti.hour,ti.min,ti.sec);
                    break;
                }
                
                case LogicalTypeId::TIMESTAMP: {
                    nanodbc::timestamp ts = state.statement->result.get<nanodbc::timestamp>((short)col_idx);
                    // Convert to DuckDB timestamp
                    date_t date = Date::FromDate(ts.year, ts.month, ts.day);
                    dtime_t time = Time::FromTime(ts.hour, ts.min, ts.sec, ts.fract / 1000000);
                    FlatVector::GetData<timestamp_t>(out_vec)[out_idx] = Timestamp::FromDatetime(date, time);
                    break;
                }
                
                case LogicalTypeId::UUID: {
                    std::string uuidStr = state.statement->result.get<std::string>((short)col_idx);
                    try {
                        hugeint_t uuidValue;
                        if (UUID::FromString(uuidStr, uuidValue)) {
                            FlatVector::GetData<hugeint_t>(out_vec)[out_idx] = uuidValue;
                        } else {
                            FlatVector::Validity(out_vec).Set(out_idx, false);
                        }
                    } catch (...) {
                        FlatVector::Validity(out_vec).Set(out_idx, false);
                    }
                    break;
                }
                
                case LogicalTypeId::BLOB: {
                    std::string blob_data = state.statement->result.get<std::string>((short)col_idx);
                    FlatVector::GetData<string_t>(out_vec)[out_idx] = 
                        StringVector::AddStringOrBlob(out_vec, blob_data.data(), blob_data.size());
                    break;
                }
                
                default:
                    throw BinderException("Unsupported ODBC to DuckDB type conversion: " + 
                                       out_vec.GetType().ToString());
            }
        }
        
        out_idx++;
    }
    
    output.SetCardinality(out_idx);
}

//------------------------------------------------------------------------------
// Attach Function
//------------------------------------------------------------------------------

void AttachOdbcDatabase(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
    auto &attach_data = data.bind_data->CastNoConst<OdbcAttachFunctionData>();
    
    if (attach_data.finished) {
        output.SetCardinality(0);
        return;
    }
    
    try {
        auto db = OdbcConnection::Connect(attach_data.connection_params);
        auto dconn = Connection(context.db->GetDatabase(context));
        
        // Handle tables
        auto tables = db->GetTables();
        for (auto &table_name : tables) {
            // Build named parameters map
            duckdb::named_parameter_map_t params;
            params["connection"] = Value(attach_data.connection_params.GetDsn().empty() ? 
                                       attach_data.connection_params.GetConnectionString() : 
                                       attach_data.connection_params.GetDsn());
            params["table_name"] = Value(table_name);
            
            if (!attach_data.connection_params.GetUsername().empty()) {
                params["username"] = Value(attach_data.connection_params.GetUsername());
                
                if (!attach_data.connection_params.GetPassword().empty()) {
                    params["password"] = Value(attach_data.connection_params.GetPassword());
                }
            }
            
            if (attach_data.options.all_varchar) {
                params["all_varchar"] = Value::BOOLEAN(true);
            }
            
            if (OdbcEncoding::NeedsConversion(attach_data.options.encoding)) {
                params["encoding"] = Value(attach_data.options.encoding);
            }
            
            auto table_func_relation = dconn.TableFunction("odbc_scan", {}, params);
            table_func_relation->CreateView(table_name, attach_data.options.overwrite, false);
        }
        
        // Handle views
        auto views = db->GetViews();
        for (auto &view_name : views) {
            duckdb::named_parameter_map_t params;
            params["connection"] = Value(attach_data.connection_params.GetDsn().empty() ? 
                                       attach_data.connection_params.GetConnectionString() : 
                                       attach_data.connection_params.GetDsn());
            params["query"] = Value("SELECT * FROM \"" + OdbcUtils::SanitizeString(view_name) + "\"");
            
            if (!attach_data.connection_params.GetUsername().empty()) {
                params["username"] = Value(attach_data.connection_params.GetUsername());
                
                if (!attach_data.connection_params.GetPassword().empty()) {
                    params["password"] = Value(attach_data.connection_params.GetPassword());
                }
            }
            
            if (attach_data.options.all_varchar) {
                params["all_varchar"] = Value::BOOLEAN(true);
            }
            
            if (OdbcEncoding::NeedsConversion(attach_data.options.encoding)) {
                params["encoding"] = Value(attach_data.options.encoding);
            }
            
            auto query_func_relation = dconn.TableFunction("odbc_query", {}, params);
            query_func_relation->CreateView(view_name, attach_data.options.overwrite, false);
        }
        
        // Set output
        output.SetCardinality(1);
        output.SetValue(0, 0, Value::BOOLEAN(true));
        
        attach_data.finished = true;
        
    } catch (const nanodbc::database_error& e) {
        OdbcUtils::ThrowException("attach database", e);
    }
}

//------------------------------------------------------------------------------
// Exec Function
//------------------------------------------------------------------------------

void ExecuteOdbcStatement(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
    auto &exec_data = data.bind_data->CastNoConst<OdbcExecFunctionData>();
    
    if (exec_data.finished) {
        output.SetCardinality(0);
        return;
    }
    
    try {
        auto db = OdbcConnection::Connect(exec_data.connection_params);
        db->Execute(exec_data.sql);
        
        output.SetCardinality(1);
        FlatVector::GetData<bool>(output.data[0])[0] = true;
        
        exec_data.finished = true;
        
    } catch (const nanodbc::database_error& e) {
        OdbcUtils::ThrowException("execute statement", e);
    }
}

//------------------------------------------------------------------------------
// Function Registration
//------------------------------------------------------------------------------

TableFunction OdbcScanFunction() {
    return OdbcTableFunction::CreateScanFunction();
}

TableFunction OdbcAttachFunction() {
    return OdbcTableFunction::CreateAttachFunction();
}

TableFunction OdbcQueryFunction() {
    return OdbcTableFunction::CreateQueryFunction();
}

TableFunction OdbcExecFunction() {
    return OdbcTableFunction::CreateExecFunction();
}

} // namespace duckdb