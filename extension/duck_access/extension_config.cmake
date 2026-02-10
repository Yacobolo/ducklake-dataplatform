# This file is included by DuckDB's build system to load this extension.
duckdb_extension_load(duck_access
    SOURCE_DIR ${CMAKE_CURRENT_LIST_DIR}
    LOAD_TESTS
)
