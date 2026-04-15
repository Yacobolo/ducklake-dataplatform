#include "quack_access_secret.hpp"
#include "duckdb/main/secret/secret.hpp"
#include "duckdb/main/secret/secret_manager.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/common/string_util.hpp"

namespace duckdb {

static unique_ptr<BaseSecret> CreateQuackAccessSecret(ClientContext &context, CreateSecretInput &input) {
	auto secret = make_uniq<KeyValueSecret>(input.scope, input.type, input.provider, input.name);

	for (auto &entry : input.options) {
		auto lower_key = StringUtil::Lower(entry.first);
		if (lower_key == "api_url") {
			secret->secret_map["api_url"] = entry.second.ToString();
		} else if (lower_key == "api_key") {
			// Mark API key as redacted so it won't show in duckdb_secrets()
			secret->redact_keys.insert("api_key");
			secret->secret_map["api_key"] = entry.second.ToString();
		} else if (lower_key == "catalog") {
			secret->secret_map["catalog"] = entry.second.ToString();
		} else {
			throw InvalidInputException("Unknown quack_access secret option: '%s'", entry.first);
		}
	}

	// Validate required fields
	if (secret->secret_map.find("api_url") == secret->secret_map.end()) {
		throw InvalidInputException("quack_access secret requires API_URL parameter");
	}
	if (secret->secret_map.find("api_key") == secret->secret_map.end()) {
		throw InvalidInputException("quack_access secret requires API_KEY parameter");
	}

	return std::move(secret);
}

void QuackAccessSecret::Register(ExtensionLoader &loader) {
	// Register the secret type
	SecretType secret_type;
	secret_type.name = TYPE_NAME;
	secret_type.deserializer = KeyValueSecret::Deserialize<KeyValueSecret>;
	secret_type.default_provider = "config";
	loader.RegisterSecretType(secret_type);

	// Register the "config" provider for this type
	CreateSecretFunction create_func = {TYPE_NAME, "config", CreateQuackAccessSecret};
	create_func.named_parameters["api_url"] = LogicalType::VARCHAR;
	create_func.named_parameters["api_key"] = LogicalType::VARCHAR;
	create_func.named_parameters["catalog"] = LogicalType::VARCHAR;
	loader.RegisterFunction(create_func);
}

unique_ptr<QuackAccessSecretData> QuackAccessSecret::GetSecret(ClientContext &context) {
	auto &secret_manager = SecretManager::Get(context);
	auto transaction = CatalogTransaction::GetSystemCatalogTransaction(context);

	// Look for any secret of type "quack_access"
	auto secret_match = secret_manager.LookupSecret(transaction, "quack_access://", TYPE_NAME);
	if (!secret_match.HasMatch()) {
		return nullptr;
	}

	auto &kv_secret = dynamic_cast<const KeyValueSecret &>(secret_match.GetSecret());
	auto data = make_uniq<QuackAccessSecretData>();

	auto api_url_it = kv_secret.secret_map.find("api_url");
	if (api_url_it != kv_secret.secret_map.end()) {
		data->api_url = api_url_it->second.ToString();
	}

	auto api_key_it = kv_secret.secret_map.find("api_key");
	if (api_key_it != kv_secret.secret_map.end()) {
		data->api_key = api_key_it->second.ToString();
	}
	auto catalog_it = kv_secret.secret_map.find("catalog");
	if (catalog_it != kv_secret.secret_map.end()) {
		data->catalog = catalog_it->second.ToString();
	}

	return data;
}

} // namespace duckdb
