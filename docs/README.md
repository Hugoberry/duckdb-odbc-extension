# DuckDB ODBC Extension

> **⚠️ WARNING: This repository is archived and no longer maintained.**
> Please use the official odbc-scanner extension: https://github.com/duckdb/odbc-scanner

The DuckDB ODBC extension allows DuckDB to seamlessly connect to any database system that provides an ODBC driver. This enables you to query and analyze data from a wide variety of data sources without leaving the DuckDB ecosystem.

## Features

- **Simple ODBC Connectivity**: Connect to any ODBC-compatible database using DSNs or direct connection strings
- **Table Scanning**: Import tables from external databases into DuckDB for analysis
- **Cross-Database SQL**: Execute custom SQL queries directly against external databases
- **Automatic Attachment**: Attach all tables from an external database as views in DuckDB
- **Cross-Platform Support**: Works on Windows, macOS, and Linux
- **Type Handling**: Automatic mapping between ODBC and DuckDB data types
- **Character Encoding Support**: Built-in cross-platform encoding conversion with customizable encoding settings
- **Performance Optimization**: Efficient data transfer with binary conversion

## Installation

```sql
INSTALL nanodbc FROM community;
LOAD nanodbc;
```

## Usage Examples

All functions now use named parameters for improved readability and flexibility.

### Query a table using a pre-configured DSN

```sql
-- Basic usage with a DSN
SELECT * FROM odbc_scan(
    table_name='customers',
    connection='MyODBCDSN'
);

-- With username and password
SELECT * FROM odbc_scan(
    table_name='orders',
    connection='MyODBCDSN',
    username='myuser',
    password='mypassword'
);

-- With additional options
SELECT * FROM odbc_scan(
    table_name='products',
    connection='MyODBCDSN',
    encoding='WINDOWS-1252',  -- Specify character encoding
    timeout=30,               -- Set connection timeout
    read_only=true           -- Connect in read-only mode
);
```

### Use a direct connection string

```sql
-- SQL Server example
SELECT * FROM odbc_scan(
    table_name='products',
    connection='Driver={SQL Server};Server=myserver;Database=mydatabase;Trusted_Connection=yes;'
);

-- MySQL example with encoding
SELECT * FROM odbc_scan(
    table_name='customers',
    connection='Driver={MySQL ODBC Driver};Server=localhost;Database=mydb;User=myuser;Password=mypassword;',
    encoding='UTF-8'
);
```

### Handle complex data types

```sql
-- Treat all columns as VARCHAR (useful for complex types)
SELECT * FROM odbc_scan(
    table_name='complex_table',
    connection='MyODBCDSN',
    all_varchar=true
);

-- Specify encoding for non-UTF8 data
SELECT * FROM odbc_scan(
    table_name='legacy_data',
    connection='MyODBCDSN',
    encoding='ISO-8859-1'
);
```

### Execute custom SQL query

```sql
-- Execute a custom SQL query against an ODBC source
SELECT * FROM odbc_query(
    connection='MyODBCDSN',
    query='SELECT id, name, amount FROM sales WHERE amount > 1000'
);

-- With credentials
SELECT * FROM odbc_query(
    connection='MyODBCDSN',
    query='SELECT * FROM sensitive_data',
    username='admin',
    password='secret'
);
```

### Execute DDL or DML statements

```sql
-- Execute DDL/DML statements without returning results
CALL odbc_exec(
    connection='MyODBCDSN',
    sql='CREATE TABLE new_table (id INT, name VARCHAR(100))'
);

-- With credentials
CALL odbc_exec(
    connection='MyODBCDSN',
    sql='INSERT INTO audit_log VALUES (NOW(), ''user_action'')',
    username='admin',
    password='secret'
);
```

### Attach entire database

```sql
-- Attach all tables from an ODBC source as views in DuckDB
CALL odbc_attach(
    connection='MyODBCDSN'
);

-- Attach with options
CALL odbc_attach(
    connection='MyODBCDSN',
    overwrite=true,           -- Replace existing views
    all_varchar=true,         -- Treat all columns as VARCHAR
    encoding='WINDOWS-1252'   -- Specify character encoding
);

-- With credentials
CALL odbc_attach(
    connection='MyODBCDSN',
    username='user',
    password='pass',
    read_only=true
);
```

## Function Reference

### odbc_scan

Query a table from an ODBC data source.

```sql
odbc_scan(
    table_name VARCHAR,       -- Name of the table to scan
    connection VARCHAR,       -- DSN or connection string
    username VARCHAR = '',    -- Optional username
    password VARCHAR = '',    -- Optional password
    all_varchar BOOLEAN = false,  -- Treat all columns as VARCHAR
    encoding VARCHAR = 'UTF-8',   -- Character encoding (default: UTF-8)
    timeout INTEGER = 60,         -- Connection timeout in seconds
    read_only BOOLEAN = true      -- Connect in read-only mode
)
```

### odbc_query

Execute a custom SQL query against an ODBC data source.

```sql
odbc_query(
    connection VARCHAR,       -- DSN or connection string
    query VARCHAR,           -- SQL query to execute
    username VARCHAR = '',    -- Optional username
    password VARCHAR = '',    -- Optional password
    all_varchar BOOLEAN = false,  -- Treat all columns as VARCHAR
    encoding VARCHAR = 'UTF-8',   -- Character encoding (default: UTF-8)
    timeout INTEGER = 60,         -- Connection timeout in seconds
    read_only BOOLEAN = true      -- Connect in read-only mode
)
```

### odbc_exec

Execute SQL statements without returning results (DDL/DML operations).

```sql
odbc_exec(
    connection VARCHAR,       -- DSN or connection string
    sql VARCHAR,             -- SQL statement to execute
    username VARCHAR = '',    -- Optional username
    password VARCHAR = '',    -- Optional password
    encoding VARCHAR = 'UTF-8',   -- Character encoding (default: UTF-8)
    timeout INTEGER = 60,         -- Connection timeout in seconds
    read_only BOOLEAN = false     -- Connect in read-only mode
)
```

### odbc_attach

Attach all tables from an ODBC data source as views in DuckDB.

```sql
odbc_attach(
    connection VARCHAR,       -- DSN or connection string
    username VARCHAR = '',    -- Optional username
    password VARCHAR = '',    -- Optional password
    overwrite BOOLEAN = false,    -- Overwrite existing views
    all_varchar BOOLEAN = false,  -- Treat all columns as VARCHAR
    encoding VARCHAR = 'UTF-8',   -- Character encoding (default: UTF-8)
    timeout INTEGER = 60,         -- Connection timeout in seconds
    read_only BOOLEAN = true      -- Connect in read-only mode
)
```

## Character Encoding Support

The extension now includes comprehensive cross-platform encoding support. By default, all data is expected to be in UTF-8. If your data uses a different encoding, you can specify it using the `encoding` parameter.

Supported encodings include:
- Common Windows Codepages: WINDOWS-1252, CP1252, CP1250, etc.
- ISO standards: ISO-8859-1, ISO-8859-15, etc.
- Asian encodings: SHIFT_JIS, GB2312, BIG5, EUC-KR
- Unicode: UTF-8 (default)

The encoding conversion is handled automatically on all platforms (Windows, macOS, Linux).

## Compatibility Matrix

This extension has been tested with various database systems across different platforms. Below is a matrix showing confirmed compatibility:

| Database System | Windows | macOS | Linux |
|-----------------|:-------:|:-----:|:-----:|
| SQL Server | ✅ | ✅ | ✅ |
| Microsoft Access | ✅ | - | - |
| Snowflake | ✅ | ✅ | ✅ |
| SQLite | ✅ | ✅ | ✅ |
| PostgreSQL | ✅ | ✅ | ✅ |
| MySQL | ✅ | ✅ | ✅ |

_Note: Compatibility depends on the availability of ODBC drivers for your platform. The matrix above shows databases I've verified, but any database with proper ODBC drivers should work._

## Requirements

### Windows
- Windows ODBC drivers are included in the operating system
- Additional database-specific drivers may need to be installed separately

### macOS
- Install unixODBC: `brew install unixodbc`
- Install database-specific drivers
- Ensure iconv library is available (usually pre-installed)

### Linux
- Install unixODBC: `apt-get install unixodbc unixodbc-dev` (Ubuntu/Debian) or `yum install unixODBC unixODBC-devel` (RHEL/CentOS)
- Install database-specific drivers
- Ensure iconv library is available: `apt-get install libiconv` (if not already installed)

## Configuration

### Setting up DSNs

#### Windows
1. Open "ODBC Data Sources" from Administrative Tools
2. Add a new User or System DSN with your connection details

#### macOS/Linux
1. Edit `~/.odbc.ini` to add your DSN:
```ini
[MyODBCDSN]
Driver = PostgreSQL
Server = localhost
Database = mydb
```
2. Edit `~/.odbcinst.ini` to configure drivers:
```ini
[PostgreSQL]
Description = PostgreSQL ODBC Driver
Driver = /usr/local/lib/psqlodbcw.so
```

## Troubleshooting

- **Connection Errors**: Ensure your DSN is properly configured and the database server is accessible
- **Missing Drivers**: Verify that the appropriate ODBC drivers are installed for your database system
- **Type Conversion Issues**: Use the `all_varchar=true` parameter if you encounter type conversion problems
- **Character Encoding Issues**: Specify the correct `encoding` parameter if you see garbled text. Common values include `WINDOWS-1252` for older Windows systems or `ISO-8859-1` for Latin-1 encoded data
- **Parameter Syntax**: Remember that all functions now use named parameters. Use the format `parameter_name=value` when calling functions

## Performance Considerations

- For large datasets, consider using `LIMIT` or filtering conditions in your queries
- The extension performs best when retrieving specific columns rather than `SELECT *`
- Complex joins are better performed within DuckDB after importing the necessary tables
- Use the `timeout` parameter to prevent long-running queries from blocking
- Enable `read_only=true` (default) for better performance when only reading data

## License

This extension is licensed under the MIT License.
