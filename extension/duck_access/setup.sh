#!/bin/bash
# Setup script for duck_access DuckDB extension.
# Downloads and configures all dependencies needed for building.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

# DuckDB version to target (v1.4.4 LTS — latest stable)
DUCKDB_VERSION="v1.4.4"

echo "=== duck_access extension setup ==="

# 1. Clone DuckDB (if not already present)
if [ ! -d "duckdb" ]; then
    echo "Cloning DuckDB ${DUCKDB_VERSION}..."
    git clone --depth 1 --branch "$DUCKDB_VERSION" https://github.com/duckdb/duckdb.git
else
    echo "DuckDB already present, skipping clone."
fi

# 2. Clone extension-ci-tools (pinned to same DuckDB version)
if [ ! -d "extension-ci-tools" ]; then
    echo "Cloning extension-ci-tools ${DUCKDB_VERSION}..."
    git clone --depth 1 --branch "$DUCKDB_VERSION" https://github.com/duckdb/extension-ci-tools.git
else
    echo "extension-ci-tools already present, skipping clone."
fi

# 3. Vendor cpp-httplib (single header)
if [ ! -f "src/include/httplib.h" ]; then
    echo "Downloading cpp-httplib..."
    curl -sL "https://raw.githubusercontent.com/yhirose/cpp-httplib/v0.18.3/httplib.h" \
        -o "src/include/httplib.h"
    echo "Downloaded httplib.h ($(wc -c < src/include/httplib.h) bytes)"
else
    echo "httplib.h already present, skipping download."
fi

# 4. Vendor nlohmann/json (single header)
if [ ! -f "src/include/json.hpp" ]; then
    echo "Downloading nlohmann/json..."
    curl -sL "https://github.com/nlohmann/json/releases/download/v3.11.3/json.hpp" \
        -o "src/include/json.hpp"
    echo "Downloaded json.hpp ($(wc -c < src/include/json.hpp) bytes)"
else
    echo "json.hpp already present, skipping download."
fi

echo ""
echo "=== Setup complete ==="
echo ""
echo "To build the extension:"
echo "  cd $SCRIPT_DIR"
echo "  make"
echo ""
echo "To load in DuckDB (unsigned, for development):"
echo "  duckdb -unsigned"
echo "  LOAD 'build/release/extension/duck_access/duck_access.duckdb_extension';"
echo ""
echo "To create a secret:"
echo "  CREATE SECRET my_platform ("
echo "      TYPE duck_access,"
echo "      API_URL 'http://localhost:8080/v1',"
echo "      API_KEY 'your_api_key_here'"
echo "  );"
