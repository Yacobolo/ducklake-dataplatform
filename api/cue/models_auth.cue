package api

// Authored auth and identity schemas.

#apiKeyInfoFields: {
	created_at:   #createdAtProperty
	expires_at:   #expiresAtProperty
	id:           #idProperty
	key_prefix:   #stringProperty
	name:         #nameProperty
	principal_id: #principalIDProperty
	...
}

#principalSummaryFields: {
	id:       #idProperty
	is_admin: #boolProperty
	name:     #nameProperty
	...
}

#principalFields: #principalSummaryFields & {
	created_at: #createdAtProperty
	type:       #refProperty & {#ref: "PrincipalType"}
}

#usernamePasswordFields: {
	password: #stringProperty
	username: #stringProperty
	...
}

#oidcProviderFields: {
	audience:      #stringProperty
	client_id:     #stringProperty
	client_secret: #stringProperty
	enabled:       #enabledProperty
	issuer_url:    #stringProperty
	jwks_url:      #stringProperty
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
		#fields: #apiKeyInfoFields
		#required: ["id", "principal_id", "name"]
	}

	AuthLoginResponse: #objectSchema & {
		#fields: {
			principal: #refProperty & {#ref: "AuthPrincipalSummary"}
			token:     #stringProperty
		}
		#required: ["token", "principal"]
	}

	AuthPrincipalSummary: #objectSchema & {
		#fields:    #principalSummaryFields
		#required: ["id", "name", "is_admin"]
	}

	BootstrapCompleteRequest: #objectSchema & {
		#fields: {
			bootstrap_token: #stringProperty
			password:        #stringProperty
			principal_name:  #principalNameProperty
			username:        #stringProperty
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
			expires_at:   #expiresAtProperty
			name:         #nameProperty
			principal_id: #principalIDProperty
		}
		#required: ["principal_id"]
	}

	CreateAPIKeyResponse: #objectSchema & {
		#fields: {
			created_at: #createdAtProperty
			expires_at: #expiresAtProperty
			id:         #idProperty
			key:        #stringProperty
			key_prefix: #stringProperty
			name:       #nameProperty
		}
		#required: ["id", "key"]
	}

	CreatePrincipalRequest: #objectSchema & {
		#fields: {
			is_admin: #boolProperty
			name:     #nameProperty
			type:     #refProperty & {#ref: "PrincipalType"}
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
			audience:      #stringProperty
			client_id:     #stringProperty
			enabled:       #enabledProperty
			issuer_url:    #stringProperty
			jwks_url:      #stringProperty
			scopes:        #stringProperty
			secret_stored: #boolProperty
			updated_at:    #updatedAtProperty
		}
		#required: ["enabled", "secret_stored"]
	}

	Principal: #objectSchema & {
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
