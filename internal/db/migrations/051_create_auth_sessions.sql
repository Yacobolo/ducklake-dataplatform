-- +goose Up
CREATE TABLE auth_sessions (
  id TEXT PRIMARY KEY,
  principal_id TEXT NOT NULL,
  session_hash TEXT NOT NULL UNIQUE,
  auth_method TEXT NOT NULL,
  user_agent TEXT,
  ip_address TEXT,
  expires_at DATETIME NOT NULL,
  idle_expires_at DATETIME NOT NULL,
  last_seen_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  revoked_at DATETIME,
  created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  FOREIGN KEY (principal_id) REFERENCES principals(id) ON DELETE CASCADE
);

CREATE INDEX idx_auth_sessions_principal_id ON auth_sessions(principal_id);
CREATE INDEX idx_auth_sessions_expires_at ON auth_sessions(expires_at);

-- +goose Down
DROP INDEX IF EXISTS idx_auth_sessions_expires_at;
DROP INDEX IF EXISTS idx_auth_sessions_principal_id;
DROP TABLE IF EXISTS auth_sessions;
