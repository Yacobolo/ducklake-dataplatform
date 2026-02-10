#pragma once

#include <string>

namespace duckdb {

/// HTTP response from the Go API.
struct HttpResponse {
	int status_code = 0;
	std::string body;
	std::string error;

	bool Ok() const { return status_code >= 200 && status_code < 300; }
};

/// Simple HTTP client for calling the duck_access Go API.
/// Uses cpp-httplib (vendored) with OpenSSL for HTTPS support.
class DuckAccessHttp {
public:
	/// POST JSON to the given URL with API key header.
	/// timeout_ms defaults to 10 seconds.
	static HttpResponse PostJson(
		const std::string &url,
		const std::string &api_key,
		const std::string &json_body,
		int timeout_ms = 10000
	);
};

} // namespace duckdb
