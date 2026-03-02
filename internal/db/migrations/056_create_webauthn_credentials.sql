-- +goose Up
CREATE TABLE webauthn_credentials (
  id TEXT PRIMARY KEY,
  principal_id TEXT NOT NULL,
  credential_id TEXT NOT NULL UNIQUE,
  public_key TEXT NOT NULL,
  sign_count INTEGER NOT NULL DEFAULT 0,
  transports TEXT,
  backup_eligible INTEGER NOT NULL DEFAULT 0,
  backup_state INTEGER NOT NULL DEFAULT 0,
  created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  last_used_at DATETIME,
  FOREIGN KEY (principal_id) REFERENCES principals(id) ON DELETE CASCADE
);

CREATE INDEX idx_webauthn_credentials_principal_id ON webauthn_credentials(principal_id);

-- +goose Down
DROP INDEX IF EXISTS idx_webauthn_credentials_principal_id;
DROP TABLE IF EXISTS webauthn_credentials;
