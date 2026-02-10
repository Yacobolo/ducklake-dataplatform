#define DUCKDB_EXTENSION_MAIN

#include "duckdb.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duck_access_extension.hpp"
#include "duck_access_secret.hpp"
#include "duck_access_scan.hpp"

namespace duckdb {

static void LoadInternal(ExtensionLoader &loader) {
	// 1. Register the "duck_access" secret type and provider
	DuckAccessSecret::Register(loader);

	// 2. Register the replacement scan that intercepts unresolved table names
	DuckAccessScan::Register(loader.GetDatabaseInstance());
}

void DuckAccessExtension::Load(ExtensionLoader &loader) {
	LoadInternal(loader);
}

std::string DuckAccessExtension::Name() {
	return "duck_access";
}

std::string DuckAccessExtension::Version() const {
	return "0.1.0";
}

} // namespace duckdb

#ifdef DUCKDB_BUILD_LOADABLE_EXTENSION
extern "C" {

DUCKDB_CPP_EXTENSION_ENTRY(duck_access, loader) {
	duckdb::LoadInternal(loader);
}

}
#endif

#ifndef DUCKDB_EXTENSION_MAIN
#error DUCKDB_EXTENSION_MAIN not defined
#endif
