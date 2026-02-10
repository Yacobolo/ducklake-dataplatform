#define DUCKDB_EXTENSION_MAIN

#include "duckdb.hpp"
#include "duck_access_extension.hpp"
#include "duck_access_secret.hpp"
#include "duck_access_scan.hpp"

namespace duckdb {

static void LoadInternal(DatabaseInstance &instance) {
	// 1. Register the "duck_access" secret type and provider
	DuckAccessSecret::Register(instance);

	// 2. Register the replacement scan that intercepts unresolved table names
	DuckAccessScan::Register(instance);
}

void DuckAccessExtension::Load(DuckDB &db) {
	LoadInternal(*db.instance);
}

std::string DuckAccessExtension::Name() {
	return "duck_access";
}

std::string DuckAccessExtension::Version() const {
	return "0.1.0";
}

} // namespace duckdb

extern "C" {

DUCKDB_EXTENSION_API void duck_access_init(duckdb::DatabaseInstance &instance) {
	duckdb::LoadInternal(instance);
}

DUCKDB_EXTENSION_API const char *duck_access_version() {
	return duckdb::DuckDB::LibraryVersion();
}

}

#ifndef DUCKDB_EXTENSION_MAIN
#error DUCKDB_EXTENSION_MAIN not defined
#endif
