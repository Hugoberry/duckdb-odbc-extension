Write-Host "===== Installing DuckDB ODBC Driver ====="
mkdir duckdb_odbc
Invoke-WebRequest -Uri "https://github.com/duckdb/duckdb-odbc/releases/download/v1.2.2/duckdb_odbc-windows-amd64.zip" -OutFile "duckdb_odbc.zip"
Expand-Archive -Path duckdb_odbc.zip -DestinationPath duckdb_odbc -Force

Write-Host "===== Inspecting Files ====="
Get-ChildItem -Path .\duckdb_odbc -Recurse | ForEach-Object { Write-Host $_.FullName }

Write-Host "===== Manual Driver Registration ====="
# Instead of using the installer, manually register the driver
# Copy the driver files to system directory
Copy-Item -Path ".\duckdb_odbc\*.dll" -Destination "$env:SystemRoot\System32" -Force

# Create registry entries directly
$regPath = "HKLM:\SOFTWARE\ODBC\ODBCINST.INI"

# Create driver entry in ODBC Drivers section
if (!(Test-Path "$regPath\ODBC Drivers")) {
  New-Item -Path "$regPath\ODBC Drivers" -Force | Out-Null
}
New-ItemProperty -Path "$regPath\ODBC Drivers" -Name "DuckDB Driver" -Value "Installed" -PropertyType String -Force | Out-Null

# Create driver details
if (!(Test-Path "$regPath\DuckDB Driver")) {
  New-Item -Path "$regPath\DuckDB Driver" -Force | Out-Null
}

$driverPath = "$env:SystemRoot\System32\duckdb_odbc.dll"
if (Test-Path ".\duckdb_odbc\duckdb_odbc.dll") {
  $driverPath = "$env:SystemRoot\System32\duckdb_odbc.dll"
} elseif (Test-Path ".\duckdb_odbc\libduckdb_odbc.dll") {
  $driverPath = "$env:SystemRoot\System32\libduckdb_odbc.dll"
}

New-ItemProperty -Path "$regPath\DuckDB Driver" -Name "Driver" -Value $driverPath -PropertyType String -Force | Out-Null
New-ItemProperty -Path "$regPath\DuckDB Driver" -Name "Setup" -Value $driverPath -PropertyType String -Force | Out-Null
New-ItemProperty -Path "$regPath\DuckDB Driver" -Name "Description" -Value "DuckDB ODBC Driver" -PropertyType String -Force | Out-Null

# Verify the installation was completed
Write-Host "===== Verifying Installation ====="
if (Test-Path -Path "$regPath\DuckDB Driver") {
  Write-Host "DuckDB Driver registered successfully in registry"
  Get-ItemProperty -Path "$regPath\DuckDB Driver" | Format-List
} else {
  Write-Host "Warning: DuckDB Driver not found in registry"
}