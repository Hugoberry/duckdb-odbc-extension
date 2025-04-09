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

namespace duckdb {

#define ODBC_CHUNK_SIZE 1024 // Default chunk size for large text/binary fields

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
        } else if (kv.first == "row_group_size") {
            result->rows_per_group = kv.second.GetValue<idx_t>();
        } else if (kv.first == "chunk_size") {
            result->chunk_size = kv.second.GetValue<idx_t>();
        }
    }
    
    // Set default values if not specified
    if (result->chunk_size == 0) {
        result->chunk_size = ODBC_CHUNK_SIZE;
    }
    if (result->rows_per_group == 0) {
        result->rows_per_group = 122880; // Default to the same as SQLite scanner
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
                              
        // Add row bounds if available
        if (!gstate.bounds.empty()) {
            // Get current bounds
            auto &bounds = gstate.bounds[gstate.current_bound_index];
            sql += StringUtil::Format(" LIMIT %d OFFSET %d", 
                                      (int)(bounds.end_row - bounds.start_row), 
                                      (int)bounds.start_row);
        }
    } else {
        sql = bind_data.sql;
    }
    
    result->stmt = result->db->Prepare(sql.c_str());
    result->done = false;
    result->chunk_size = bind_data.chunk_size;
    
    return std::move(result);
}

static unique_ptr<GlobalTableFunctionState> ODBCInitGlobalState(ClientContext &context,
                                                                TableFunctionInitInput &input) {
    auto &bind_data = input.bind_data->Cast<ODBCBindData>();
    
    // Get the max threads available from context
    idx_t max_threads = TaskScheduler::GetScheduler(context).NumberOfThreads();
    
    // Create the global state
    auto result = make_uniq<ODBCGlobalState>(max_threads);
    
    // If we have row groups, set up the parallelism
    if (bind_data.rows_per_group != 0) {
        // Estimate total rows for the table
        idx_t estimated_rows = 0;
        
        try {
            // Try to get row count if possible
            if (!bind_data.sql.empty()) {
                // For custom SQL, use a COUNT query
                ODBCDB db;
                if (!bind_data.dsn.empty()) {
                    db = ODBCDB::OpenWithDSN(bind_data.dsn, bind_data.username, bind_data.password);
                } else {
                    db = ODBCDB::OpenWithConnectionString(bind_data.connection_string);
                }
                
                auto count_sql = "SELECT COUNT(*) FROM (" + bind_data.sql + ") AS t";
                auto stmt = db.Prepare(count_sql);
                
                if (stmt.Step()) {
                    // Get count from first column
                    auto count_val = stmt.GetValueLength(0);
                    if (count_val != SQL_NULL_DATA) {
                        estimated_rows = stmt.GetValue<int64_t>(0);
                    }
                }
            } else {
                // For table scans, query the table directly
                ODBCDB db;
                if (!bind_data.dsn.empty()) {
                    db = ODBCDB::OpenWithDSN(bind_data.dsn, bind_data.username, bind_data.password);
                } else {
                    db = ODBCDB::OpenWithConnectionString(bind_data.connection_string);
                }
                
                auto count_sql = "SELECT COUNT(*) FROM \"" + 
                              ODBCUtils::SanitizeString(bind_data.table_name) + "\"";
                auto stmt = db.Prepare(count_sql);
                
                if (stmt.Step()) {
                    auto count_val = stmt.GetValueLength(0);
                    if (count_val != SQL_NULL_DATA) {
                        estimated_rows = stmt.GetValue<int64_t>(0);
                    }
                }
            }
        } catch (...) {
            // If we can't get row count, use an educated guess
            estimated_rows = 1000000; // Default to 1 million rows
        }
        
        if (estimated_rows > 0) {
            // Set up row boundaries for parallel scanning
            idx_t row_group_size = bind_data.rows_per_group;
            idx_t num_groups = (estimated_rows + row_group_size - 1) / row_group_size;
            
            // Limit the number of groups to max_threads
            num_groups = MinValue<idx_t>(num_groups, result->max_threads);
            
            if (num_groups > 1) {
                // Create row groups for parallel scanning
                idx_t rows_per_thread = (estimated_rows + num_groups - 1) / num_groups;
                
                for (idx_t i = 0; i < num_groups; i++) {
                    ODBCRowBounds bounds;
                    bounds.start_row = i * rows_per_thread;
                    bounds.end_row = MinValue<idx_t>((i + 1) * rows_per_thread, estimated_rows);
                    result->bounds.push_back(bounds);
                }
            }
        }
    }
    
    return result;
}

static void ODBCScan(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
    auto &state = data.local_state->Cast<ODBCLocalState>();
    auto &gstate = data.global_state->Cast<ODBCGlobalState>();
    auto &bind_data = data.bind_data->Cast<ODBCBindData>();
    
    if (state.done) {
        bool more_work = false;
        
        // Check if there are more row bounds to process
        if (!gstate.bounds.empty()) {
            lock_guard<mutex> lock(gstate.lock);
            
            if (gstate.current_bound_index < gstate.bounds.size() - 1) {
                gstate.current_bound_index++;
                more_work = true;
            }
        }
        
        if (more_work) {
            // Reinitialize with new bounds
            state.stmt.Close();
            
            string sql;
            if (bind_data.sql.empty()) {
                // Build query based on column IDs
                auto col_names = StringUtil::Join(
                    state.column_ids.data(), state.column_ids.size(), ", ", [&](const idx_t column_id) {
                        return column_id == (column_t)-1 ? "NULL"
                                                       : '"' + ODBCUtils::SanitizeString(bind_data.names[column_id]) + '"';
                    });
                    
                sql = StringUtil::Format("SELECT %s FROM \"%s\"", col_names, 
                                        ODBCUtils::SanitizeString(bind_data.table_name));
                                      
                // Add row bounds
                auto &bounds = gstate.bounds[gstate.current_bound_index];
                sql += StringUtil::Format(" LIMIT %d OFFSET %d", 
                                         (int)(bounds.end_row - bounds.start_row), 
                                         (int)bounds.start_row);
            } else {
                sql = bind_data.sql;
            }
            
            state.stmt = state.db->Prepare(sql.c_str());
            state.done = false;
        } else {
            return;
        }
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
                case LogicalTypeId::HUGEINT: {
                    // For HUGEINT, we fetch as string and parse
                    char buffer[100]; // Should be enough for any numeric representation
                    SQLGetData(stmt.hstmt, col_idx + 1, SQL_C_CHAR, buffer, sizeof(buffer), &indicator);
                    
                    if (indicator > 0 && indicator < sizeof(buffer)) {
                        hugeint_t value;
                        std::string str_val(buffer, indicator);
                        if (Hugeint::TryConvert(str_val, value)) {
                            FlatVector::GetData<hugeint_t>(out_vec)[out_idx] = value;
                        } else {
                            // Failed to parse as HUGEINT, set to NULL
                            auto &mask = FlatVector::Validity(out_vec);
                            mask.Set(out_idx, false);
                        }
                    } else {
                        // Invalid data, set to NULL
                        auto &mask = FlatVector::Validity(out_vec);
                        mask.Set(out_idx, false);
                    }
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
                    // For DECIMAL, we need to handle it based on precision and scale
                    auto dec_type = out_vec.GetType();
                    auto dec_width = DecimalType::GetWidth(dec_type);
                    auto dec_scale = DecimalType::GetScale(dec_type);
                    
                    if (dec_width <= 4) {
                        // Tiny decimal
                        int16_t value;
                        SQLGetData(stmt.hstmt, col_idx + 1, SQL_C_SSHORT, &value, sizeof(value), &indicator);
                        FlatVector::GetData<int16_t>(out_vec)[out_idx] = value;
                    } else if (dec_width <= 9) {
                        // Small decimal
                        int32_t value;
                        SQLGetData(stmt.hstmt, col_idx + 1, SQL_C_SLONG, &value, sizeof(value), &indicator);
                        FlatVector::GetData<int32_t>(out_vec)[out_idx] = value;
                    } else if (dec_width <= 18) {
                        // Medium decimal
                        int64_t value;
                        SQLGetData(stmt.hstmt, col_idx + 1, SQL_C_SBIGINT, &value, sizeof(value), &indicator);
                        FlatVector::GetData<int64_t>(out_vec)[out_idx] = value;
                    } else {
                        // For larger decimals, use string parsing and HUGEINT
                        char buffer[100]; // Should be enough for any decimal representation
                        SQLGetData(stmt.hstmt, col_idx + 1, SQL_C_CHAR, buffer, sizeof(buffer), &indicator);
                        
                        if (indicator > 0 && indicator < sizeof(buffer)) {
                            hugeint_t value;
                            std::string str_val(buffer, indicator);
                            
                            // Check for decimal point and adjust value based on scale
                            auto decimal_point = str_val.find('.');
                            if (decimal_point != std::string::npos) {
                                // Remove decimal point and adjust
                                str_val.erase(decimal_point, 1);
                                
                                // Pad with zeros if needed to get correct scale
                                auto decimal_places = str_val.length() - decimal_point;
                                if (decimal_places < dec_scale) {
                                    str_val.append(dec_scale - decimal_places, '0');
                                }
                                
                                // Convert to HUGEINT
                                if (Hugeint::TryConvert(str_val, value)) {
                                    FlatVector::GetData<hugeint_t>(out_vec)[out_idx] = value;
                                } else {
                                    auto &mask = FlatVector::Validity(out_vec);
                                    mask.Set(out_idx, false);
                                }
                            } else {
                                // No decimal point, multiply by 10^scale
                                if (Hugeint::TryConvert(str_val, value)) {
                                    // Scale the value
                                    for (int i = 0; i < dec_scale; i++) {
                                        value *= 10;
                                    }
                                    FlatVector::GetData<hugeint_t>(out_vec)[out_idx] = value;
                                } else {
                                    auto &mask = FlatVector::Validity(out_vec);
                                    mask.Set(out_idx, false);
                                }
                            }
                        } else {
                            // Invalid data
                            auto &mask = FlatVector::Validity(out_vec);
                            mask.Set(out_idx, false);
                        }
                    }
                    break;
                }
                case LogicalTypeId::VARCHAR: {
                    // For string data, we need to handle potentially large strings
                    // First try with a small buffer to check the size
                    char init_buffer[256];
                    SQLLEN bytes_needed;
                    SQLGetData(stmt.hstmt, col_idx + 1, SQL_C_CHAR, init_buffer, sizeof(init_buffer), &bytes_needed);
                    
                    if (bytes_needed == SQL_NULL_DATA) {
                        // NULL value, already handled above
                        continue;
                    }
                    
                    if (bytes_needed >= 0 && bytes_needed < sizeof(init_buffer)) {
                        // String fits in the initial buffer
                        FlatVector::GetData<string_t>(out_vec)[out_idx] = 
                            StringVector::AddString(out_vec, init_buffer, bytes_needed);
                    } else {
                        // Need to allocate a larger buffer
                        // For very large strings, use chunked fetching
                        if (bytes_needed > (SQLLEN)state.chunk_size) {
                            std::string large_string;
                            large_string.reserve(bytes_needed > 0 ? bytes_needed : state.chunk_size);
                            
                            // First chunk partially read into init_buffer
                            large_string.append(init_buffer, sizeof(init_buffer) - 1);
                            
                            // Continue reading chunks
                            char chunk_buffer[ODBC_CHUNK_SIZE];
                            SQLLEN chunk_bytes_read;
                            bool more_data = true;
                            
                            while (more_data) {
                                auto ret = SQLGetData(stmt.hstmt, col_idx + 1, SQL_C_CHAR, 
                                                    chunk_buffer, sizeof(chunk_buffer), &chunk_bytes_read);
                                
                                if (ret == SQL_SUCCESS || ret == SQL_SUCCESS_WITH_INFO) {
                                    if (chunk_bytes_read > 0) {
                                        size_t to_copy = (chunk_bytes_read < sizeof(chunk_buffer)) ? 
                                                        chunk_bytes_read : sizeof(chunk_buffer) - 1;
                                        large_string.append(chunk_buffer, to_copy);
                                    }
                                    
                                    more_data = (ret == SQL_SUCCESS_WITH_INFO);
                                } else {
                                    more_data = false;
                                }
                            }
                            
                            FlatVector::GetData<string_t>(out_vec)[out_idx] = 
                                StringVector::AddString(out_vec, large_string);
                        } else {
                            // String is larger than init_buffer but small enough for a single allocation
                            size_t buffer_size = bytes_needed + 1; // +1 for null terminator
                            auto large_buffer = std::unique_ptr<char[]>(new char[buffer_size]);
                            
                            // Read the entire string in one go
                            SQLGetData(stmt.hstmt, col_idx + 1, SQL_C_CHAR, large_buffer.get(), buffer_size, &bytes_needed);
                            
                            FlatVector::GetData<string_t>(out_vec)[out_idx] = 
                                StringVector::AddString(out_vec, large_buffer.get(), bytes_needed);
                        }
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
                    // For BLOB data, handle potentially large binary data
                    // First try with a small buffer to check the size
                    char init_buffer[256];
                    SQLLEN bytes_needed;
                    SQLGetData(stmt.hstmt, col_idx + 1, SQL_C_BINARY, init_buffer, sizeof(init_buffer), &bytes_needed);
                    
                    if (bytes_needed == SQL_NULL_DATA) {
                        // NULL value, already handled above
                        continue;
                    }
                    
                    if (bytes_needed >= 0 && bytes_needed < sizeof(init_buffer)) {
                        // Binary data fits in the initial buffer
                        FlatVector::GetData<string_t>(out_vec)[out_idx] = 
                            StringVector::AddStringOrBlob(out_vec, init_buffer, bytes_needed);
                    } else {
                        // Need to allocate a larger buffer
                        // For very large blobs, use chunked fetching
                        if (bytes_needed > (SQLLEN)state.chunk_size) {
                            std::vector<char> large_blob;
                            large_blob.reserve(bytes_needed > 0 ? bytes_needed : state.chunk_size);
                            
                            // First chunk partially read into init_buffer
                            large_blob.insert(large_blob.end(), init_buffer, init_buffer + (sizeof(init_buffer) - 1));
                            
                            // Continue reading chunks
                            char chunk_buffer[ODBC_CHUNK_SIZE];
                            SQLLEN chunk_bytes_read;
                            bool more_data = true;
                            
                            while (more_data) {
                                auto ret = SQLGetData(stmt.hstmt, col_idx + 1, SQL_C_BINARY, 
                                                    chunk_buffer, sizeof(chunk_buffer), &chunk_bytes_read);
                                
                                if (ret == SQL_SUCCESS || ret == SQL_SUCCESS_WITH_INFO) {
                                    if (chunk_bytes_read > 0) {
                                        size_t to_copy = (chunk_bytes_read < sizeof(chunk_buffer)) ? 
                                                        chunk_bytes_read : sizeof(chunk_buffer);
                                        large_blob.insert(large_blob.end(), chunk_buffer, chunk_buffer + to_copy);
                                    }
                                    
                                    more_data = (ret == SQL_SUCCESS_WITH_INFO);
                                } else {
                                    more_data = false;
                                }
                            }
                            
                            FlatVector::GetData<string_t>(out_vec)[out_idx] = 
                                StringVector::AddStringOrBlob(out_vec, large_blob.data(), large_blob.size());
                        } else {
                            // Blob is larger than init_buffer but small enough for a single allocation
                            size_t buffer_size = bytes_needed;
                            auto large_buffer = std::unique_ptr<char[]>(new char[buffer_size]);
                            
                            // Read the entire blob in one go
                            SQLGetData(stmt.hstmt, col_idx + 1, SQL_C_BINARY, large_buffer.get(), buffer_size, &bytes_needed);
                            
                            FlatVector::GetData<string_t>(out_vec)[out_idx] = 
                                StringVector::AddStringOrBlob(out_vec, large_buffer.get(), bytes_needed);
                        }
                    }
                    break;
                }
                case LogicalTypeId::UUID: {
                    // For UUID, usually a 16-byte fixed value
                    unsigned char uuid_bytes[16];
                    SQLGetData(stmt.hstmt, col_idx + 1, SQL_C_BINARY, uuid_bytes, sizeof(uuid_bytes), &indicator);
                    
                    if (indicator == 16) {
                        // Valid UUID size
                        hugeint_t uuid_value;
                        memcpy(&uuid_value, uuid_bytes, 16);
                        FlatVector::GetData<hugeint_t>(out_vec)[out_idx] = uuid_value;
                    } else {
                        // Try to parse as string
                        char uuid_str[40]; // More than enough for UUID string
                        SQLGetData(stmt.hstmt, col_idx + 1, SQL_C_CHAR, uuid_str, sizeof(uuid_str), &indicator);
                        
                        if (indicator > 0 && indicator < sizeof(uuid_str)) {
                            string_t uuid_string(uuid_str, indicator);
                            hugeint_t uuid_value;
                            
                            // Parse UUID string into hugeint_t
                            std::string str_uuid(uuid_str, indicator);
                            // Remove hyphens
                            str_uuid.erase(std::remove(str_uuid.begin(), str_uuid.end(), '-'), str_uuid.end());
                            
                            if (Hugeint::TryConvert(str_uuid, uuid_value)) {
                                FlatVector::GetData<hugeint_t>(out_vec)[out_idx] = uuid_value;
                            } else {
                                auto &mask = FlatVector::Validity(out_vec);
                                mask.Set(out_idx, false);
                            }
                        } else {
                            auto &mask = FlatVector::Validity(out_vec);
                            mask.Set(out_idx, false);
                        }
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

static unique_ptr<NodeStatistics> ODBCCardinality(ClientContext &context, const FunctionData *bind_data_p) {
    D_ASSERT(bind_data_p);
    auto &bind_data = bind_data_p->Cast<ODBCBindData>();
    
    // Try to get cardinality estimate
    try {
        // Try to get row count if possible
        if (!bind_data.sql.empty()) {
            // For custom SQL, use a COUNT query
            ODBCDB db;
            if (!bind_data.dsn.empty()) {
                db = ODBCDB::OpenWithDSN(bind_data.dsn, bind_data.username, bind_data.password);
            } else {
                db = ODBCDB::OpenWithConnectionString(bind_data.connection_string);
            }
            
            auto count_sql = "SELECT COUNT(*) FROM (" + bind_data.sql + ") AS t";
            auto stmt = db.Prepare(count_sql);
            
            if (stmt.Step()) {
                // Get count from first column
                auto count_val = stmt.GetValueLength(0);
                if (count_val != SQL_NULL_DATA) {
                    auto row_count = stmt.GetValue<int64_t>(0);
                    return make_uniq<NodeStatistics>(row_count);
                }
            }
        } else {
            // For table scans, query the table directly
            ODBCDB db;
            if (!bind_data.dsn.empty()) {
                db = ODBCDB::OpenWithDSN(bind_data.dsn, bind_data.username, bind_data.password);
            } else {
                db = ODBCDB::OpenWithConnectionString(bind_data.connection_string);
            }
            
            auto count_sql = "SELECT COUNT(*) FROM \"" + 
                          ODBCUtils::SanitizeString(bind_data.table_name) + "\"";
            auto stmt = db.Prepare(count_sql);
            
            if (stmt.Step()) {
                auto count_val = stmt.GetValueLength(0);
                if (count_val != SQL_NULL_DATA) {
                    auto row_count = stmt.GetValue<int64_t>(0);
                    return make_uniq<NodeStatistics>(row_count);
                }
            }
        }
    } catch (...) {
        // If we can't get an accurate count, return nullptr
    }
    
    return nullptr;
}

ODBCScanFunction::ODBCScanFunction()
    : TableFunction("odbc_scan", {LogicalType::VARCHAR, LogicalType::VARCHAR}, ODBCScan, ODBCBind,
                    ODBCInitGlobalState, ODBCInitLocalState) {
    to_string = ODBCToString;
    get_bind_info = ODBCBindInfo;
    cardinality = ODBCCardinality;
    projection_pushdown = true;
    named_parameters["all_varchar"] = LogicalType::BOOLEAN;
    named_parameters["row_group_size"] = LogicalType::UINTEGER;
    named_parameters["chunk_size"] = LogicalType::UINTEGER;
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
        } else if (kv.first == "row_group_size") {
            result->rows_per_group = kv.second.GetValue<idx_t>();
        } else if (kv.first == "chunk_size") {
            result->chunk_size = kv.second.GetValue<idx_t>();
        }
    }
    
    // Set default values if not specified
    if (result->chunk_size == 0) {
        result->chunk_size = ODBC_CHUNK_SIZE;
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
        
        SQLSMALLINT data_type;
        SQLULEN column_size;
        SQLSMALLINT decimal_digits;
        
        // Get detailed column info
        if (stmt.GetColumnAttributes(i, data_type, column_size, decimal_digits)) {
            auto column_type = result->all_varchar ? LogicalType::VARCHAR : 
                               GetDuckDBType(data_type, column_size, decimal_digits);
            
            names.push_back(column_name);
            return_types.push_back(column_type);
        } else {
            // If we can't get attributes, use a simpler approach
            auto column_type = result->all_varchar ? LogicalType::VARCHAR : 
                               LogicalType((LogicalTypeId)stmt.GetType(i));
            
            names.push_back(column_name);
            return_types.push_back(column_type);
        }
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
    named_parameters["row_group_size"] = LogicalType::UINTEGER;
    named_parameters["chunk_size"] = LogicalType::UINTEGER;
}

} // namespace duckdb