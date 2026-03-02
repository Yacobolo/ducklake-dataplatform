-- +goose Up
CREATE TABLE auth_login_attempts (
  id TEXT PRIMARY KEY,
  username TEXT,
  ip_address TEXT,
  success INTEGER NOT NULL,
  reason TEXT,
  created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_auth_login_attempts_username_created_at ON auth_login_attempts(username, created_at DESC);
CREATE INDEX idx_auth_login_attempts_ip_created_at ON auth_login_attempts(ip_address, created_at DESC);

-- +goose Down
DROP INDEX IF EXISTS idx_auth_login_attempts_ip_created_at;
DROP INDEX IF EXISTS idx_auth_login_attempts_username_created_at;
DROP TABLE IF EXISTS auth_login_attempts;
