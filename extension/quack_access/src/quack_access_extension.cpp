#define DUCKDB_EXTENSION_MAIN

#include "duckdb.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "quack_access_extension.hpp"
#include "quack_access_secret.hpp"
#include "quack_access_scan.hpp"

namespace duckdb {

static void LoadInternal(ExtensionLoader &loader) {
	// 1. Register the "quack_access" secret type and provider
	QuackAccessSecret::Register(loader);

	// 2. Register the replacement scan that intercepts unresolved table names
	QuackAccessScan::Register(loader.GetDatabaseInstance());
}

void QuackAccessExtension::Load(ExtensionLoader &loader) {
	LoadInternal(loader);
}

std::string QuackAccessExtension::Name() {
	return "quack_access";
}

std::string QuackAccessExtension::Version() const {
	return "0.1.0";
}

} // namespace duckdb

#ifdef DUCKDB_BUILD_LOADABLE_EXTENSION
extern "C" {

DUCKDB_CPP_EXTENSION_ENTRY(quack_access, loader) {
	duckdb::LoadInternal(loader);
}

}
#endif

#ifndef DUCKDB_EXTENSION_MAIN
#error DUCKDB_EXTENSION_MAIN not defined
#endif
