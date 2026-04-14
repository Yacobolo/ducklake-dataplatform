#pragma once

#include "duckdb.hpp"

namespace duckdb {

class ExtensionLoader;

/// Data extracted from a quack_access secret for use by the replacement scan.
struct QuackAccessSecretData {
	string api_url;
	string api_key;
};

/// Registers the "quack_access" secret type and its "config" provider.
/// Users create secrets with:
///   CREATE SECRET my_platform (
///       TYPE quack_access,
///       API_URL 'https://api.example.com/v1',
///       API_KEY 'key_abc123'
///   );
class QuackAccessSecret {
public:
	static void Register(ExtensionLoader &loader);

	/// Look up the first quack_access secret in the secret manager.
	/// Returns nullptr if no secret of this type exists.
	static unique_ptr<QuackAccessSecretData> GetSecret(ClientContext &context);

	static constexpr const char *TYPE_NAME = "quack_access";
};

} // namespace duckdb
