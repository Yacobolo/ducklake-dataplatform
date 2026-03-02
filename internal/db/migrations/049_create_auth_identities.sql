-- +goose Up
CREATE TABLE auth_identities (
  id TEXT PRIMARY KEY,
  principal_id TEXT NOT NULL,
  provider TEXT NOT NULL,
  issuer TEXT,
  subject TEXT NOT NULL,
  email TEXT,
  email_verified INTEGER NOT NULL DEFAULT 0,
  created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  UNIQUE (provider, issuer, subject),
  FOREIGN KEY (principal_id) REFERENCES principals(id) ON DELETE CASCADE
);

CREATE INDEX idx_auth_identities_principal_id ON auth_identities(principal_id);

-- +goose Down
DROP INDEX IF EXISTS idx_auth_identities_principal_id;
DROP TABLE IF EXISTS auth_identities;
