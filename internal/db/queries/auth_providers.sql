-- name: GetAuthProviderConfig :one
SELECT * FROM auth_providers WHERE id = 1;

-- name: UpsertAuthProviderConfig :exec
INSERT INTO auth_providers (
  id, oidc_enabled, oidc_issuer_url, oidc_jwks_url, oidc_audience,
  oidc_client_id, oidc_client_secret_enc, oidc_scopes, updated_at
)
VALUES (1, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
ON CONFLICT(id) DO UPDATE SET
  oidc_enabled = excluded.oidc_enabled,
  oidc_issuer_url = excluded.oidc_issuer_url,
  oidc_jwks_url = excluded.oidc_jwks_url,
  oidc_audience = excluded.oidc_audience,
  oidc_client_id = excluded.oidc_client_id,
  oidc_client_secret_enc = excluded.oidc_client_secret_enc,
  oidc_scopes = excluded.oidc_scopes,
  updated_at = CURRENT_TIMESTAMP;
