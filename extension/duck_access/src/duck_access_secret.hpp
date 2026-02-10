#pragma once

#include "duckdb.hpp"

namespace duckdb {

class ExtensionLoader;

/// Data extracted from a duck_access secret for use by the replacement scan.
struct DuckAccessSecretData {
	string api_url;
	string api_key;
};

/// Registers the "duck_access" secret type and its "config" provider.
/// Users create secrets with:
///   CREATE SECRET my_platform (
///       TYPE duck_access,
///       API_URL 'https://api.example.com/v1',
///       API_KEY 'key_abc123'
///   );
class DuckAccessSecret {
public:
	static void Register(ExtensionLoader &loader);

	/// Look up the first duck_access secret in the secret manager.
	/// Returns nullptr if no secret of this type exists.
	static unique_ptr<DuckAccessSecretData> GetSecret(ClientContext &context);

	static constexpr const char *TYPE_NAME = "duck_access";
};

} // namespace duckdb
