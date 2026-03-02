-- +goose Up
CREATE TABLE local_credentials (
  principal_id TEXT PRIMARY KEY,
  username TEXT NOT NULL UNIQUE,
  password_hash TEXT NOT NULL,
  password_changed_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  must_change_password INTEGER NOT NULL DEFAULT 0,
  created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  FOREIGN KEY (principal_id) REFERENCES principals(id) ON DELETE CASCADE
);

-- +goose Down
DROP TABLE IF EXISTS local_credentials;
