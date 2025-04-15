Write-Host "===== Installing DuckDB ODBC Driver ====="
mkdir -Force duckdb_odbc
Invoke-WebRequest -Uri "https://github.com/duckdb/duckdb-odbc/releases/download/v1.2.2/duckdb_odbc-windows-amd64.zip" -OutFile "duckdb_odbc.zip"
Expand-Archive -Path duckdb_odbc.zip -DestinationPath duckdb_odbc -Force

Write-Host "===== Inspecting Files ====="
Get-ChildItem -Path .\duckdb_odbc -Recurse | ForEach-Object { Write-Host $_.FullName }

# Determine the correct driver filename
$driverFileName = if (Test-Path ".\duckdb_odbc\duckdb_odbc.dll") { "duckdb_odbc.dll" } elseif (Test-Path ".\duckdb_odbc\libduckdb_odbc.dll") { "libduckdb_odbc.dll" } else { throw "DuckDB ODBC DLL not found!" }
$driverPath = "$env:SystemRoot\System32\$driverFileName"

Write-Host "===== Manual Driver Registration ====="
# Copy the driver files to system directory
Copy-Item -Path ".\duckdb_odbc\$driverFileName" -Destination "$env:SystemRoot\System32" -Force
Write-Host "Copied $driverFileName to $env:SystemRoot\System32"

# Base registry paths
$odbcRegPath = "HKLM:\SOFTWARE\ODBC"
$odbcInstPath = "$odbcRegPath\ODBCINST.INI"
$odbcIniPath = "$odbcRegPath\ODBC.INI"

# Create base registry keys if they don't exist
if (!(Test-Path $odbcRegPath)) { New-Item -Path $odbcRegPath -Force | Out-Null }
if (!(Test-Path $odbcInstPath)) { New-Item -Path $odbcInstPath -Force | Out-Null }
if (!(Test-Path $odbcIniPath)) { New-Item -Path $odbcIniPath -Force | Out-Null }

# Create "ODBC Drivers" section in ODBCINST.INI
if (!(Test-Path "$odbcInstPath\ODBC Drivers")) {
  New-Item -Path "$odbcInstPath\ODBC Drivers" -Force | Out-Null
}
New-ItemProperty -Path "$odbcInstPath\ODBC Drivers" -Name "DuckDB Driver" -Value "Installed" -PropertyType String -Force | Out-Null
Write-Host "Registered driver in 'ODBC Drivers' section"

# Create driver details in ODBCINST.INI
if (!(Test-Path "$odbcInstPath\DuckDB Driver")) {
  New-Item -Path "$odbcInstPath\DuckDB Driver" -Force | Out-Null
}
New-ItemProperty -Path "$odbcInstPath\DuckDB Driver" -Name "Driver" -Value $driverPath -PropertyType String -Force | Out-Null
New-ItemProperty -Path "$odbcInstPath\DuckDB Driver" -Name "Setup" -Value $driverPath -PropertyType String -Force | Out-Null
New-ItemProperty -Path "$odbcInstPath\DuckDB Driver" -Name "Description" -Value "DuckDB ODBC Driver" -PropertyType String -Force | Out-Null

Write-Host "Created driver entry in ODBCINST.INI"

# Create DSN entry in ODBC.INI
if (!(Test-Path "$odbcIniPath\DuckDB")) {
  New-Item -Path "$odbcIniPath\DuckDB" -Force | Out-Null
}
New-ItemProperty -Path "$odbcIniPath\DuckDB" -Name "Driver" -Value $driverPath -PropertyType String -Force | Out-Null
New-ItemProperty -Path "$odbcIniPath\DuckDB" -Name "Description" -Value "DuckDB Data Source" -PropertyType String -Force | Out-Null
New-ItemProperty -Path "$odbcIniPath\DuckDB" -Name "Database" -Value ":memory:" -PropertyType String -Force | Out-Null
Write-Host "Created DSN entry in ODBC.INI"

# Also add to ODBC Data Sources
if (!(Test-Path "$odbcIniPath\ODBC Data Sources")) {
  New-Item -Path "$odbcIniPath\ODBC Data Sources" -Force | Out-Null
}
New-ItemProperty -Path "$odbcIniPath\ODBC Data Sources" -Name "DuckDB" -Value "DuckDB Driver" -PropertyType String -Force | Out-Null
Write-Host "Added DSN to 'ODBC Data Sources' section"

# Verify the installation was completed
Write-Host "===== Verifying Installation ====="
if (Test-Path -Path "$odbcInstPath\DuckDB Driver") {
  Write-Host "DuckDB Driver registered successfully in registry"
  Get-ItemProperty -Path "$odbcInstPath\DuckDB Driver" | Format-List
} else {
  Write-Host "Warning: DuckDB Driver not found in registry"
}

if (Test-Path -Path "$odbcIniPath\DuckDB") {
  Write-Host "DuckDB DSN created successfully in registry"
  Get-ItemProperty -Path "$odbcIniPath\DuckDB" | Format-List
} else {
  Write-Host "Warning: DuckDB DSN not found in registry"
}

# List all ODBC drivers
Write-Host "===== Installed ODBC Drivers ====="
Get-OdbcDriver | Format-Table -AutoSize