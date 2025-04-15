#!/bin/bash
set -e

# Default values
DRIVER_PATH=$(pwd)/libduckdb_odbc.so
DATABASE_PATH=":memory:"
LEVEL="user"  # Default to user-level installation

# Parse arguments
while [[ $# -gt 0 ]]; do
  case $1 in
    -s)
      LEVEL="system"
      shift
      ;;
    -u)
      LEVEL="user"
      shift
      ;;
    -db)
      DATABASE_PATH="$2"
      shift 2
      ;;
    -D)
      DRIVER_PATH="$2"
      shift 2
      ;;
    *)
      echo "Unknown option: $1"
      exit 1
      ;;
  esac
done

echo "Installing DuckDB ODBC driver at $LEVEL level"
echo "Driver path: $DRIVER_PATH"
echo "Database path: $DATABASE_PATH"

# Create temporary files for ODBC configuration
ODBC_INI_FILE=$(mktemp)
ODBCINST_FILE=$(mktemp)

# Create odbc.ini content
cat << EOF > $ODBC_INI_FILE
[DuckDB]
Driver = DuckDB Driver
Database=${DATABASE_PATH}
EOF

# Create odbcinst.ini content
cat << EOF > $ODBCINST_FILE
[ODBC]
Trace = yes
TraceFile = /tmp/odbctrace

[DuckDB Driver]
Driver = ${DRIVER_PATH}
EOF

# Install the driver based on level
if [ "$LEVEL" = "system" ]; then
  if [ "$EUID" -ne 0 ]; then
    echo "System-level installation requires root privileges"
    exit 1
  fi
  
  # System-level installation
  odbcinst -i -d -f $ODBCINST_FILE
  odbcinst -i -s -l -f $ODBC_INI_FILE
else
  # User-level installation - create/update ~/.odbcinst.ini directly
  HOME_ODBCINST="$HOME/.odbcinst.ini"
  
  # Create file if it doesn't exist
  touch "$HOME_ODBCINST"
  
  # Remove existing DuckDB Driver section if present
  sed -i '/\[DuckDB Driver\]/,/^\[/d' "$HOME_ODBCINST" || true
  
  # Add our new driver section
  echo "[DuckDB Driver]" >> "$HOME_ODBCINST"
  echo "Driver = $DRIVER_PATH" >> "$HOME_ODBCINST"
  
  # Set up the DSN
  odbcinst -i -s -h -f $ODBC_INI_FILE
fi

# Clean up temporary files
rm $ODBC_INI_FILE
rm $ODBCINST_FILE

echo "DuckDB ODBC driver installation complete"

# Verify installation
echo "Verifying driver installation:"
odbcinst -q -d | grep -A 5 "DuckDB Driver" || echo "Driver not found in ODBC registry"

# Copy the driver to system library locations for better visibility
if [ "$LEVEL" = "system" ] || [ "$EUID" -eq 0 ]; then
  echo "Copying driver to system library locations"
  cp "$DRIVER_PATH" /usr/lib/ || true
  [ -d /usr/lib64 ] && cp "$DRIVER_PATH" /usr/lib64/ || true
  ldconfig
fi