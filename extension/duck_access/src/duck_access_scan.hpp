#pragma once

#include "duckdb.hpp"

namespace duckdb {

/// Registers the duck_access replacement scan.
/// When a user queries a table that doesn't exist locally, this scan
/// intercepts the table name, calls the Go API for a manifest, and
/// rewrites the query to read from presigned Parquet URLs with
/// RLS filters and column masks applied.
class DuckAccessScan {
public:
	static void Register(DatabaseInstance &instance);

private:
	/// The replacement scan callback. Returns a SubqueryRef that wraps
	/// read_parquet() with security policies applied, or nullptr if
	/// the table should not be intercepted.
	static unique_ptr<TableRef> ReplacementScanFunction(
		ClientContext &context,
		ReplacementScanInput &input,
		optional_ptr<ReplacementScanData> data
	);
};

} // namespace duckdb
