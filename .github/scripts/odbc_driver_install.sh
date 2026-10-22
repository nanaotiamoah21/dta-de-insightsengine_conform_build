#!/bin/bash
set -e

# Remove old repo files if present
sudo rm -f /etc/apt/sources.list.d/mssql-release.list
sudo rm -f /etc/apt/sources.list.d/microsoft-prod.list

# Add Microsoft GPG key
curl -sSL https://packages.microsoft.com/keys/microsoft.asc | gpg --dearmor | sudo tee /usr/share/keyrings/microsoft.gpg > /dev/null

# Add official Ubuntu 22.04 (jammy) Microsoft repo
echo "deb [arch=amd64 signed-by=/usr/share/keyrings/microsoft.gpg] https://packages.microsoft.com/ubuntu/22.04/prod jammy main" | sudo tee /etc/apt/sources.list.d/mssql-release.list

# Update and install
sudo apt-get update
sudo ACCEPT_EULA=Y apt-get install -y msodbcsql17 mssql-tools unixodbc unixodbc-dev

# Add mssql-tools to PATH for current and future sessions
if ! grep -q "/opt/mssql-tools/bin" ~/.bashrc; then
  echo 'export PATH="$PATH:/opt/mssql-tools/bin"' >> ~/.bashrc
fi
source ~/.bashrc

# Register ODBC driver if not already registered
if ! odbcinst -q -d | grep -q "ODBC Driver 17 for SQL Server"; then
  DRIVER_PATH=$(ls /opt/microsoft/msodbcsql17/lib64/libmsodbcsql-*.so | head -n1)
  echo "Registering ODBC driver..."
  sudo tee -a /etc/odbcinst.ini > /dev/null <<EOF

[ODBC Driver 17 for SQL Server]
Description=Microsoft ODBC Driver 17 for SQL Server
Driver=$DRIVER_PATH
UsageCount=1
EOF
fi

# Install pyodbc for Python
python3 -m pip install --upgrade pip
python3 -m pip install pyodbc

echo "✅ Done! Test with: python3 -c 'import pyodbc; print(\"pyodbc works!\")'"
