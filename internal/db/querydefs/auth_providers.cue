package querydefs

queries: [
	{
		name: "GetAuthProviderConfig"
		kind: "one"
		result: {
			row: "AuthProvider"
			fields: [
				{name: "ID", type: "int64"},
				{name: "OidcEnabled", type: "int64"},
				{name: "OidcIssuerUrl", type: "sql.NullString"},
				{name: "OidcJwksUrl", type: "sql.NullString"},
				{name: "OidcAudience", type: "sql.NullString"},
				{name: "OidcClientID", type: "sql.NullString"},
				{name: "OidcClientSecretEnc", type: "sql.NullString"},
				{name: "OidcScopes", type: "sql.NullString"},
				{name: "CreatedAt", type: "time.Time"},
				{name: "UpdatedAt", type: "time.Time"},
			]
		}
		select: {
			from: "auth_providers"
			columns: [
				{expr: "id"},
				{expr: "oidc_enabled"},
				{expr: "oidc_issuer_url"},
				{expr: "oidc_jwks_url"},
				{expr: "oidc_audience"},
				{expr: "oidc_client_id"},
				{expr: "oidc_client_secret_enc"},
				{expr: "oidc_scopes"},
				{expr: "created_at"},
				{expr: "updated_at"},
			]
			where: [{column: "id", op: "=", valueSQL: "1"}]
		}
	},
	{
		name: "UpsertAuthProviderConfig"
		kind: "exec"
		paramMode: "struct"
		params: [
			{name: "OidcEnabled", type: "int64"},
			{name: "OidcIssuerUrl", type: "sql.NullString"},
			{name: "OidcJwksUrl", type: "sql.NullString"},
			{name: "OidcAudience", type: "sql.NullString"},
			{name: "OidcClientID", type: "sql.NullString"},
			{name: "OidcClientSecretEnc", type: "sql.NullString"},
			{name: "OidcScopes", type: "sql.NullString"},
		]
		insert: {
			into: "auth_providers"
			columns: [
				"id",
				"oidc_enabled",
				"oidc_issuer_url",
				"oidc_jwks_url",
				"oidc_audience",
				"oidc_client_id",
				"oidc_client_secret_enc",
				"oidc_scopes",
				"updated_at",
			]
			values: [
				{sql: "1"},
				{param: "OidcEnabled"},
				{param: "OidcIssuerUrl"},
				{param: "OidcJwksUrl"},
				{param: "OidcAudience"},
				{param: "OidcClientID"},
				{param: "OidcClientSecretEnc"},
				{param: "OidcScopes"},
				{sql: "CURRENT_TIMESTAMP"},
			]
			conflict: {
				targets: ["id"]
				doUpdate: [
					{column: "oidc_enabled", value: {sql: "excluded.oidc_enabled"}},
					{column: "oidc_issuer_url", value: {sql: "excluded.oidc_issuer_url"}},
					{column: "oidc_jwks_url", value: {sql: "excluded.oidc_jwks_url"}},
					{column: "oidc_audience", value: {sql: "excluded.oidc_audience"}},
					{column: "oidc_client_id", value: {sql: "excluded.oidc_client_id"}},
					{column: "oidc_client_secret_enc", value: {sql: "excluded.oidc_client_secret_enc"}},
					{column: "oidc_scopes", value: {sql: "excluded.oidc_scopes"}},
					{column: "updated_at", value: {sql: "CURRENT_TIMESTAMP"}},
				]
			}
		}
	},
]
