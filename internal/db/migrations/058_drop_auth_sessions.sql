-- +goose Up
DROP INDEX IF EXISTS idx_auth_sessions_principal_active;
DROP INDEX IF EXISTS idx_auth_sessions_reaper;
DROP INDEX IF EXISTS idx_auth_sessions_expires_at;
DROP INDEX IF EXISTS idx_auth_sessions_principal_id;
DROP TABLE IF EXISTS auth_sessions;

-- +goose Down
CREATE TABLE IF NOT EXISTS auth_sessions (
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

CREATE INDEX IF NOT EXISTS idx_auth_sessions_principal_id ON auth_sessions(principal_id);
CREATE INDEX IF NOT EXISTS idx_auth_sessions_expires_at ON auth_sessions(expires_at);
CREATE INDEX IF NOT EXISTS idx_auth_sessions_principal_active
  ON auth_sessions(principal_id, revoked_at, expires_at, idle_expires_at);
CREATE INDEX IF NOT EXISTS idx_auth_sessions_reaper
  ON auth_sessions(revoked_at, expires_at, idle_expires_at);
