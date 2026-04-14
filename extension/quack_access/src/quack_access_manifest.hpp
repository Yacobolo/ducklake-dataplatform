#pragma once

#include <chrono>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

namespace duckdb {

/// A single column in the manifest.
struct ManifestColumn {
	std::string name;
	std::string type;
};

/// Parsed manifest from the Go API /v1/manifest endpoint.
struct TableManifest {
	std::string table;
	std::string schema;
	std::vector<ManifestColumn> columns;
	std::vector<std::string> files;           // presigned HTTPS URLs
	std::vector<std::string> row_filters;     // SQL filter expressions
	std::unordered_map<std::string, std::string> column_masks; // col_name -> mask_expr
	std::chrono::system_clock::time_point expires_at;
	std::chrono::system_clock::time_point fetched_at;
};

/// Thread-safe manifest cache with TTL-based expiration.
/// Caches manifest responses keyed by "schema.table" to avoid
/// hitting the Go API for every query in a script.
class ManifestCache {
public:
	/// Fetch manifest from API or return cached if not expired.
	/// On error, sets out_error and returns nullptr.
	static std::shared_ptr<TableManifest> GetOrFetch(
		const std::string &api_url,
		const std::string &api_key,
		const std::string &schema_name,
		const std::string &table_name,
		std::string &out_error
	);

	/// Force invalidation of a cached entry.
	static void Invalidate(const std::string &schema_name, const std::string &table_name);

	/// Parse a JSON manifest response body into a TableManifest.
	static std::shared_ptr<TableManifest> ParseManifest(
		const std::string &json_body,
		std::string &out_error
	);

private:
	static std::mutex cache_mutex_;
	static std::unordered_map<std::string, std::shared_ptr<TableManifest>> cache_;

	static std::string CacheKey(
		const std::string &api_url,
		const std::string &api_key,
		const std::string &schema,
		const std::string &table
	) {
		auto key_hash = std::hash<std::string>{}(api_key);
		return api_url + "|" + std::to_string(key_hash) + "|" + schema + "." + table;
	}
};

} // namespace duckdb
