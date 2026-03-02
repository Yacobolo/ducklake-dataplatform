-- +goose Up
CREATE TABLE auth_providers (
  id INTEGER PRIMARY KEY CHECK (id = 1),
  oidc_enabled INTEGER NOT NULL DEFAULT 0,
  oidc_issuer_url TEXT,
  oidc_jwks_url TEXT,
  oidc_audience TEXT,
  oidc_client_id TEXT,
  oidc_client_secret_enc TEXT,
  oidc_scopes TEXT,
  created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);

INSERT INTO auth_providers (id, oidc_enabled) VALUES (1, 0)
ON CONFLICT(id) DO NOTHING;

-- +goose Down
DROP TABLE IF EXISTS auth_providers;
