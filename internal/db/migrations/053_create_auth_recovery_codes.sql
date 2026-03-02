-- +goose Up
CREATE TABLE auth_recovery_codes (
  id TEXT PRIMARY KEY,
  principal_id TEXT NOT NULL,
  code_hash TEXT NOT NULL UNIQUE,
  used_at DATETIME,
  expires_at DATETIME NOT NULL,
  created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  FOREIGN KEY (principal_id) REFERENCES principals(id) ON DELETE CASCADE
);

CREATE INDEX idx_auth_recovery_codes_principal_id ON auth_recovery_codes(principal_id);

-- +goose Down
DROP INDEX IF EXISTS idx_auth_recovery_codes_principal_id;
DROP TABLE IF EXISTS auth_recovery_codes;
