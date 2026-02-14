#include "duck_access_manifest.hpp"
#include "duck_access_http.hpp"
#include "include/json.hpp"

#include <ctime>
#include <iomanip>
#include <sstream>

using json = nlohmann::json;

namespace duckdb {

// Static member definitions
std::mutex ManifestCache::cache_mutex_;
std::unordered_map<std::string, std::shared_ptr<TableManifest>> ManifestCache::cache_;

std::shared_ptr<TableManifest> ManifestCache::GetOrFetch(
	const std::string &api_url,
	const std::string &api_key,
	const std::string &schema_name,
	const std::string &table_name,
	std::string &out_error
) {
	auto key = CacheKey(schema_name, table_name);
	auto now = std::chrono::system_clock::now();

	// Check cache under lock
	{
		std::lock_guard<std::mutex> lock(cache_mutex_);
		auto it = cache_.find(key);
		if (it != cache_.end()) {
			auto &manifest = it->second;
			// Check if still valid (with 60s safety margin)
			auto safety_margin = std::chrono::seconds(60);
			if (manifest->expires_at - safety_margin > now) {
				return manifest;
			}
			// Expired, remove
			cache_.erase(it);
		}
	}

	// Cache miss — fetch from API
	std::string manifest_url = api_url + "/manifest";

	json request_body;
	request_body["table"] = table_name;
	request_body["schema"] = schema_name;

	auto response = DuckAccessHttp::PostJson(manifest_url, api_key, request_body.dump());

	if (!response.error.empty()) {
		out_error = "cannot reach API server: " + response.error;
		return nullptr;
	}

	if (response.status_code == 401) {
		out_error = "authentication failed — check your API key";
		return nullptr;
	}

	if (response.status_code == 403) {
		// Try to extract error message from JSON
		try {
			auto err_json = json::parse(response.body);
			out_error = err_json.value("message", "access denied");
		} catch (...) {
			out_error = "access denied";
		}
		return nullptr;
	}

	if (response.status_code == 404) {
		try {
			auto err_json = json::parse(response.body);
			out_error = err_json.value("message", "table not found");
		} catch (...) {
			out_error = "table not found on server";
		}
		return nullptr;
	}

	if (!response.Ok()) {
		out_error = "API returned HTTP " + std::to_string(response.status_code);
		if (!response.body.empty()) {
			out_error += ": " + response.body.substr(0, 200);
		}
		return nullptr;
	}

	// Parse successful response
	auto manifest = ParseManifest(response.body, out_error);
	if (!manifest) {
		return nullptr;
	}

	// Store in cache
	{
		std::lock_guard<std::mutex> lock(cache_mutex_);
		cache_[key] = manifest;
	}

	return manifest;
}

void ManifestCache::Invalidate(const std::string &schema_name, const std::string &table_name) {
	auto key = CacheKey(schema_name, table_name);
	std::lock_guard<std::mutex> lock(cache_mutex_);
	cache_.erase(key);
}

std::shared_ptr<TableManifest> ManifestCache::ParseManifest(
	const std::string &json_body,
	std::string &out_error
) {
	try {
		auto j = json::parse(json_body);

		auto manifest = std::make_shared<TableManifest>();
		manifest->table = j.value("table", "");
		manifest->schema = j.value("schema", "main");
		manifest->fetched_at = std::chrono::system_clock::now();

		// Parse expires_at (ISO 8601 format like "2024-01-15T10:30:00Z")
		if (j.contains("expires_at") && j["expires_at"].is_string()) {
			auto expires_str = j["expires_at"].get<std::string>();
			std::tm tm = {};
			std::istringstream ss(expires_str);
			ss >> std::get_time(&tm, "%Y-%m-%dT%H:%M:%S");
			if (!ss.fail()) {
				// std::get_time does not handle timezone; assume UTC.
				// timegm (POSIX) interprets tm as UTC; mktime uses local time.
				#if defined(_WIN32)
				time_t epoch = _mkgmtime(&tm);
				#else
				time_t epoch = timegm(&tm);
				#endif
				manifest->expires_at = std::chrono::system_clock::from_time_t(epoch);
			} else {
				// Fallback: if parsing fails, default to 1 hour from now.
				manifest->expires_at = std::chrono::system_clock::now() + std::chrono::hours(1);
			}
		} else {
			manifest->expires_at = std::chrono::system_clock::now() + std::chrono::hours(1);
		}

		// Parse columns
		if (j.contains("columns") && j["columns"].is_array()) {
			for (auto &col : j["columns"]) {
				ManifestColumn mc;
				mc.name = col.value("name", "");
				mc.type = col.value("type", "");
				manifest->columns.push_back(std::move(mc));
			}
		}

		// Parse files (presigned URLs)
		if (j.contains("files") && j["files"].is_array()) {
			for (auto &f : j["files"]) {
				if (f.is_string()) {
					manifest->files.push_back(f.get<std::string>());
				}
			}
		}

		// Parse row_filters
		if (j.contains("row_filters") && j["row_filters"].is_array()) {
			for (auto &rf : j["row_filters"]) {
				if (rf.is_string()) {
					manifest->row_filters.push_back(rf.get<std::string>());
				}
			}
		}

		// Parse column_masks
		if (j.contains("column_masks") && j["column_masks"].is_object()) {
			for (auto &item : j["column_masks"].items()) {
				if (item.value().is_string()) {
					manifest->column_masks[item.key()] = item.value().get<std::string>();
				}
			}
		}

		if (manifest->files.empty()) {
			out_error = "manifest contains no data files for table '" + manifest->table + "'";
			return nullptr;
		}

		return manifest;

	} catch (const json::parse_error &e) {
		out_error = std::string("failed to parse manifest JSON: ") + e.what();
		return nullptr;
	} catch (const std::exception &e) {
		out_error = std::string("error processing manifest: ") + e.what();
		return nullptr;
	}
}

} // namespace duckdb
