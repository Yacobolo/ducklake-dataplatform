#include "quack_access_http.hpp"

// CPPHTTPLIB_OPENSSL_SUPPORT is defined via CMake compile definitions
#include "include/httplib.h"

namespace duckdb {

static httplib::Client BuildClient(const std::string &url, std::string &path, HttpResponse &response, int timeout_ms) {
	// Parse URL: "https://host:port/path"
	auto scheme_end = url.find("://");
	if (scheme_end == std::string::npos) {
		response.error = "Invalid URL: missing scheme in '" + url + "'";
		return httplib::Client("");
	}

	auto path_start = url.find('/', scheme_end + 3);
	std::string base_url = (path_start != std::string::npos)
		? url.substr(0, path_start)
		: url;
	path = (path_start != std::string::npos)
		? url.substr(path_start)
		: "/";

	httplib::Client cli(base_url);
	int timeout_sec = timeout_ms / 1000;
	int timeout_usec = (timeout_ms % 1000) * 1000;
	cli.set_connection_timeout(timeout_sec, timeout_usec);
	cli.set_read_timeout(timeout_sec, timeout_usec);
	cli.set_write_timeout(timeout_sec, timeout_usec);
	return cli;
}

HttpResponse QuackAccessHttp::GetJson(
	const std::string &url,
	const std::string &api_key,
	int timeout_ms
) {
	HttpResponse response;
	std::string path;
	auto cli = BuildClient(url, path, response, timeout_ms);
	if (!response.error.empty()) {
		return response;
	}

	httplib::Headers headers = {
		{"Accept", "application/json"},
		{"X-API-Key", api_key}
	};

	auto res = cli.Get(path, headers);
	if (!res) {
		auto err = res.error();
		response.error = "HTTP request failed: " + httplib::to_string(err);
		return response;
	}

	response.status_code = res->status;
	response.body = res->body;
	return response;
}

HttpResponse QuackAccessHttp::PostJson(
	const std::string &url,
	const std::string &api_key,
	const std::string &json_body,
	int timeout_ms
) {
	HttpResponse response;
	std::string path;
	auto cli = BuildClient(url, path, response, timeout_ms);
	if (!response.error.empty()) {
		return response;
	}

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
