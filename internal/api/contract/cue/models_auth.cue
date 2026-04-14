package api

// Authored auth and identity schemas.

#apiKeyInfoFields: {
	id:           #idProperty
	principal_id: #principalIDProperty
	name:         #nameProperty
	key_prefix:   #stringProperty
	expires_at:   #expiresAtProperty
	created_at:   #createdAtProperty
	...
}

#principalSummaryFields: {
	id:       #idProperty
	name:     #nameProperty
	is_admin: #boolProperty
	...
}

#principalFields: {
	id:         #idProperty
	name:       #nameProperty
	type:       #refProperty & {#ref: "PrincipalType"}
	is_admin:   #boolProperty
	created_at: #createdAtProperty
}

#usernamePasswordFields: {
	password: #stringProperty
	username: #stringProperty
	...
}

#oidcProviderFields: {
	enabled:       #enabledProperty
	issuer_url:    #stringProperty
	jwks_url:      #stringProperty
	audience:      #stringProperty
	client_id:     #stringProperty
	client_secret: #stringProperty
	scopes:        #stringProperty
	...
}

#webSessionStatsFields: {
	absolute_ttl_seconds: #int64Property
	active_sessions:      #int64Property
	created_total:        #int64Property
	idle_ttl_seconds:     #int64Property
	reaped_total:         #int64Property
	resolve_failed_total: #int64Property
	resolved_total:       #int64Property
	revoked_all_total:    #int64Property
	revoked_total:        #int64Property
	...
}

schemas_auth: {
	APIKeyInfo: #objectSchema & {
		title:       "API key metadata."
		description: "Represents a stored API key without returning the full secret value."
		example: {
			id:           "key_01hzyapi7m7p5x4t3"
			principal_id: "user_01hzyadmin8km6w2n"
			name:         "CI deploy key"
			key_prefix:   "quack_prod_"
			expires_at:   "2026-06-30T00:00:00Z"
			created_at:   "2026-04-13T09:30:00Z"
		}
		#fields: #apiKeyInfoFields
		#required: ["id", "principal_id", "name"]
	}

	AuthLoginResponse: #objectSchema & {
		example: {
			token: "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.example"
			principal: {
				id:       "user_01hzyadmin8km6w2n"
				name:     "platform-admin"
				is_admin: true
			}
		}
		#fields: {
			token:     #stringProperty
			principal: #refProperty & {#ref: "AuthPrincipalSummary"}
		}
		#required: ["token", "principal"]
	}

	AuthPrincipalSummary: #objectSchema & {
		#fields:    #principalSummaryFields
		#required: ["id", "name", "is_admin"]
	}

	BootstrapCompleteRequest: #objectSchema & {
		example: {
			username:        "admin"
			password:        "correct-horse-battery-staple"
			principal_name:  "platform-admin"
			bootstrap_token: "bootstrap_01hzytoken"
		}
		#fields: {
			username:        #stringProperty
			password:        #stringProperty
			principal_name:  #principalNameProperty
			bootstrap_token: #stringProperty
		}
		#required: ["username", "password"]
	}

	BootstrapTokenRequest: #objectSchema & {
		example: {
			ttl_seconds: 900
		}
		#fields: {
			ttl_seconds: #int64Property
		}
	}

	BootstrapTokenResponse: #objectSchema & {
		example: {
			bootstrap_token: "bootstrap_01hzytoken"
			ttl_seconds:     900
		}
		#fields: {
			bootstrap_token: #stringProperty
			ttl_seconds:     #int64Property
		}
		#required: ["bootstrap_token", "ttl_seconds"]
	}

	CleanupAPIKeysResponse: #objectSchema & {
		example: {
			deleted_count: 3
		}
		#fields: {
			deleted_count: #int32Property
		}
		#required: ["deleted_count"]
	}

	CreateAPIKeyRequest: #objectSchema & {
		example: {
			principal_id: "user_01hzyreader4b8dm9q"
			name:         "dbt-cloud"
			expires_at:   "2026-12-31T00:00:00Z"
		}
		#fields: {
			principal_id: #principalIDProperty
			name:         #nameProperty
			expires_at:   #expiresAtProperty
		}
		#required: ["principal_id"]
	}

	CreateAPIKeyResponse: #objectSchema & {
		example: {
			id:         "key_01hzyapi7m7p5x4t3"
			key:        "quack_prod_live_2mY5...redacted"
			name:       "dbt-cloud"
			key_prefix: "quack_prod_"
			expires_at: "2026-12-31T00:00:00Z"
			created_at: "2026-04-13T09:30:00Z"
		}
		#fields: {
			id:         #idProperty
			key:        #stringProperty
			name:       #nameProperty
			key_prefix: #stringProperty
			expires_at: #expiresAtProperty
			created_at: #createdAtProperty
		}
		#required: ["id", "key"]
	}

	CreatePrincipalRequest: #objectSchema & {
		#fields: {
			name:     #nameProperty
			type:     #refProperty & {#ref: "PrincipalType"}
			is_admin: #boolProperty
		}
		#required: ["name"]
	}

	LocalLoginRequest: #objectSchema & {
		example: {
			username: "admin"
			password: "correct-horse-battery-staple"
		}
		#fields:    #usernamePasswordFields
		#required: ["username", "password"]
	}

	OIDCProviderRequest: #objectSchema & {
		example: {
			enabled:       true
			issuer_url:    "https://login.example.com"
			jwks_url:      "https://login.example.com/.well-known/jwks.json"
			audience:      "github.com/Yacobolo/quackstack"
			client_id:     "github.com/Yacobolo/quackstack-web"
			client_secret: "client-secret-ref"
			scopes:        "openid profile email"
		}
		#fields: #oidcProviderFields
		#required: ["enabled"]
	}

	OIDCProviderResponse: #objectSchema & {
		example: {
			enabled:       true
			issuer_url:    "https://login.example.com"
			jwks_url:      "https://login.example.com/.well-known/jwks.json"
			audience:      "github.com/Yacobolo/quackstack"
			client_id:     "github.com/Yacobolo/quackstack-web"
			scopes:        "openid profile email"
			updated_at:    "2026-04-13T09:30:00Z"
			secret_stored: true
		}
		#fields: {
			enabled:       #enabledProperty
			issuer_url:    #stringProperty
			jwks_url:      #stringProperty
			audience:      #stringProperty
			client_id:     #stringProperty
			scopes:        #stringProperty
			updated_at:    #updatedAtProperty
			secret_stored: #boolProperty
		}
		#required: ["enabled", "secret_stored"]
	}

	Principal: #objectSchema & {
		title:       "Authenticated principal."
		example: {
			id:         "user_01hzyadmin8km6w2n"
			name:       "platform-admin"
			type:       "user"
			is_admin:   true
			created_at: "2026-04-01T08:00:00Z"
		}
		#fields:    #principalFields
		#required: ["id", "name", "type", "is_admin"]
	}

	PrincipalType: #enumSchema & {
		#values: ["user", "group"]
	}

	RevokeWebSessionsRequest: #objectSchema & {
		example: {
			principal_id: "user_01hzyreader4b8dm9q"
		}
		#fields: {
			principal_id: #principalIDProperty
		}
		#required: ["principal_id"]
	}

	WebSessionStatsResponse: #objectSchema & {
		example: {
			absolute_ttl_seconds: 604800
			active_sessions:      5
			created_total:        42
			idle_ttl_seconds:     86400
			reaped_total:         7
			resolve_failed_total: 1
			resolved_total:       41
			revoked_all_total:    2
			revoked_total:        6
		}
		#fields:    #webSessionStatsFields
		#required: ["created_total", "resolved_total", "resolve_failed_total", "revoked_total", "revoked_all_total", "reaped_total", "active_sessions", "idle_ttl_seconds", "absolute_ttl_seconds"]
	}
}
