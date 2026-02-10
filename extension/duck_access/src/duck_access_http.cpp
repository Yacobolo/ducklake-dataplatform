#include "duck_access_http.hpp"

// CPPHTTPLIB_OPENSSL_SUPPORT is defined via CMake compile definitions
#include "include/httplib.h"

namespace duckdb {

HttpResponse DuckAccessHttp::PostJson(
	const std::string &url,
	const std::string &api_key,
	const std::string &json_body,
	int timeout_ms
) {
	HttpResponse response;

	// Parse URL: "https://host:port/path"
	auto scheme_end = url.find("://");
	if (scheme_end == std::string::npos) {
		response.error = "Invalid URL: missing scheme in '" + url + "'";
		return response;
	}

	auto path_start = url.find('/', scheme_end + 3);
	std::string base_url = (path_start != std::string::npos)
		? url.substr(0, path_start)
		: url;
	std::string path = (path_start != std::string::npos)
		? url.substr(path_start)
		: "/";

	httplib::Client cli(base_url);
	// set_connection_timeout(seconds, microseconds) - convert milliseconds properly
	int timeout_sec = timeout_ms / 1000;
	int timeout_usec = (timeout_ms % 1000) * 1000;
	cli.set_connection_timeout(timeout_sec, timeout_usec);
	cli.set_read_timeout(timeout_sec, timeout_usec);
	cli.set_write_timeout(timeout_sec, timeout_usec);

	httplib::Headers headers = {
		{"Content-Type", "application/json"},
		{"X-API-Key", api_key}
	};

	auto res = cli.Post(path, headers, json_body, "application/json");

	if (!res) {
		auto err = res.error();
		response.error = "HTTP request failed: " + httplib::to_string(err);
		return response;
	}

	response.status_code = res->status;
	response.body = res->body;
	return response;
}

} // namespace duckdb
