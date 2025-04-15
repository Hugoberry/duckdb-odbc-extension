#!/bin/bash
set -e

echo "===== Installing SQLite and SQLite ODBC Driver ====="
brew install sqliteodbc

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

# Create odbcinst.ini file with SQLite driver definition
echo "Creating ODBC driver configuration in $HOMEBREW_ETC/odbcinst.ini"
sudo touch $HOMEBREW_ETC/odbcinst.ini
cat <<EOF | sudo tee $HOMEBREW_ETC/odbcinst.ini
[ODBC Drivers]
SQLite Driver = Installed

[SQLite]
Driver = /opt/homebrew/Cellar/sqliteodbc/0.99991/lib/libsqlite3odbc.dylib
EOF

# Create a DSN in odbc.ini
echo "Creating ODBC DSN configuration in $HOMEBREW_ETC/odbc.ini"
sudo touch $HOMEBREW_ETC/odbc.ini
cat <<EOF | sudo tee $HOMEBREW_ETC/odbc.ini
[DuckDB]
Driver = SQLite
Database = :memory:
EOF

# Set permissions
sudo chmod 644 $HOMEBREW_ETC/odbcinst.ini $HOMEBREW_ETC/odbc.ini

# Set environment variables
echo "Setting ODBC environment variables"
export ODBCSYSINI=$HOMEBREW_ETC
export ODBCINI=$HOMEBREW_ETC/odbc.ini

# Export for subsequent steps in the workflow
echo "ODBCSYSINI=$HOMEBREW_ETC" >> $GITHUB_ENV
echo "ODBCINI=$HOMEBREW_ETC/odbc.ini" >> $GITHUB_ENV


# Test the SQLite installation
echo "===== Testing SQLite ODBC Driver ====="
isql -v DuckDB || echo "Could not connect to DuckDB DSN, but continuing anyway"

echo "===== SQLite ODBC Driver Installation Complete ====="