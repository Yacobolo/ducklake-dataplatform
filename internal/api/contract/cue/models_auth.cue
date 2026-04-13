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
		#fields: #apiKeyInfoFields
		#required: ["id", "principal_id", "name"]
	}

	AuthLoginResponse: #objectSchema & {
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
		#fields: {
			username:        #stringProperty
			password:        #stringProperty
			principal_name:  #principalNameProperty
			bootstrap_token: #stringProperty
		}
		#required: ["username", "password"]
	}

	BootstrapTokenRequest: #objectSchema & {
		#fields: {
			ttl_seconds: #int64Property
		}
	}

	BootstrapTokenResponse: #objectSchema & {
		#fields: {
			bootstrap_token: #stringProperty
			ttl_seconds:     #int64Property
		}
		#required: ["bootstrap_token", "ttl_seconds"]
	}

	CleanupAPIKeysResponse: #objectSchema & {
		#fields: {
			deleted_count: #int32Property
		}
		#required: ["deleted_count"]
	}

	CreateAPIKeyRequest: #objectSchema & {
		#fields: {
			principal_id: #principalIDProperty
			name:         #nameProperty
			expires_at:   #expiresAtProperty
		}
		#required: ["principal_id"]
	}

	CreateAPIKeyResponse: #objectSchema & {
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
		#fields:    #usernamePasswordFields
		#required: ["username", "password"]
	}

	OIDCProviderRequest: #objectSchema & {
		#fields: #oidcProviderFields
		#required: ["enabled"]
	}

	OIDCProviderResponse: #objectSchema & {
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
		#fields:    #principalFields
		#required: ["id", "name", "type", "is_admin"]
	}

	PrincipalType: #enumSchema & {
		#values: ["user", "group"]
	}

	RevokeWebSessionsRequest: #objectSchema & {
		#fields: {
			principal_id: #principalIDProperty
		}
		#required: ["principal_id"]
	}

	WebSessionStatsResponse: #objectSchema & {
		#fields:    #webSessionStatsFields
		#required: ["created_total", "resolved_total", "resolve_failed_total", "revoked_total", "revoked_all_total", "reaped_total", "active_sessions", "idle_ttl_seconds", "absolute_ttl_seconds"]
	}
}
