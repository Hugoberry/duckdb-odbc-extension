#!/bin/bash
set -e

echo "===== Installing ODBC Driver Manager ====="
brew install unixodbc

echo "===== Installing DuckDB ODBC Driver ====="
mkdir -p duckdb_odbc
curl -L -o duckdb_odbc-osx-universal.zip https://github.com/duckdb/duckdb-odbc/releases/download/v1.2.2/duckdb_odbc-osx-universal.zip
unzip duckdb_odbc-osx-universal.zip -d duckdb_odbc

# Check file existence and permissions
echo "Checking extracted files:"
ls -la duckdb_odbc/

# Check if dynamic library is valid
echo "Checking dynamic library information:"
otool -L duckdb_odbc/libduckdb_odbc.dylib

# Install unixODBC dependencies properly
echo "Installing unixODBC with proper configuration:"
brew install unixodbc
brew list unixodbc

# Find the unixODBC installation location
UNIXODBC_LIB=$(brew --prefix unixodbc)/lib
echo "unixODBC library location: $UNIXODBC_LIB"
ls -la $UNIXODBC_LIB

# Fix the library dependencies with install_name_tool
echo "Fixing library path dependencies:"
# First let's keep the original in case we need it
cp duckdb_odbc/libduckdb_odbc.dylib duckdb_odbc/libduckdb_odbc.dylib.orig

# Try to fix hard-coded paths if needed 
if otool -L duckdb_odbc/libduckdb_odbc.dylib | grep -q "/Users/runner/work/duckdb-odbc"; then
  echo "Attempting to fix hardcoded paths in the library..."
  # Find the libodbcinst library path in the brew installation
  ODBCINST_LIB=$(find $UNIXODBC_LIB -name "libodbcinst*.dylib" | head -1)
  echo "Found odbcinst library at: $ODBCINST_LIB"
  
  install_name_tool -change "/Users/runner/work/duckdb-odbc/duckdb-odbc/build/unixodbc/build/lib/libodbcinst.2.dylib" \
                    "$ODBCINST_LIB" \
                    duckdb_odbc/libduckdb_odbc.dylib || echo "Failed to update library path"
  
  # Check if the changes were applied
  echo "Checking library dependencies after changes:"
  otool -L duckdb_odbc/libduckdb_odbc.dylib
fi

# Set proper permissions
chmod 755 duckdb_odbc/libduckdb_odbc.dylib

# Determine the correct Homebrew path
if [ -d "/opt/homebrew/etc" ]; then
  # For Apple Silicon Macs
  HOMEBREW_ETC="/opt/homebrew/etc"
else
  # For Intel Macs
  HOMEBREW_ETC="/usr/local/etc"
fi

echo "Using Homebrew config directory: $HOMEBREW_ETC"

# Create ODBC configuration directories if they don't exist
sudo mkdir -p $HOMEBREW_ETC

# Create odbcinst.ini file with DuckDB driver definition
echo "Creating ODBC driver configuration in $HOMEBREW_ETC/odbcinst.ini"
sudo touch $HOMEBREW_ETC/odbcinst.ini
echo "[DuckDB Driver]" | sudo tee $HOMEBREW_ETC/odbcinst.ini
echo "Description=DuckDB ODBC Driver" | sudo tee -a $HOMEBREW_ETC/odbcinst.ini
echo "Driver=$(pwd)/duckdb_odbc/libduckdb_odbc.dylib" | sudo tee -a $HOMEBREW_ETC/odbcinst.ini
echo "Setup=$(pwd)/duckdb_odbc/libduckdb_odbc.dylib" | sudo tee -a $HOMEBREW_ETC/odbcinst.ini
echo "UsageCount=1" | sudo tee -a $HOMEBREW_ETC/odbcinst.ini

# Create a DSN in odbc.ini
echo "Creating ODBC DSN configuration in $HOMEBREW_ETC/odbc.ini"
sudo touch $HOMEBREW_ETC/odbc.ini
echo "[DuckDB]" | sudo tee $HOMEBREW_ETC/odbc.ini
echo "Description=DuckDB Database" | sudo tee -a $HOMEBREW_ETC/odbc.ini
echo "Driver=DuckDB Driver" | sudo tee -a $HOMEBREW_ETC/odbc.ini
echo "Database=:memory:" | sudo tee -a $HOMEBREW_ETC/odbc.ini

# Set permissions
sudo chmod 644 $HOMEBREW_ETC/odbcinst.ini $HOMEBREW_ETC/odbc.ini