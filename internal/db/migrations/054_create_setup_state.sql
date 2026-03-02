-- +goose Up
CREATE TABLE setup_state (
  id INTEGER PRIMARY KEY CHECK (id = 1),
  setup_completed INTEGER NOT NULL DEFAULT 0,
  setup_completed_at DATETIME,
  setup_completed_by TEXT,
  bootstrap_token_hash TEXT,
  bootstrap_token_expires_at DATETIME,
  created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  FOREIGN KEY (setup_completed_by) REFERENCES principals(id) ON DELETE SET NULL
);

INSERT INTO setup_state (id, setup_completed) VALUES (1, 0)
ON CONFLICT(id) DO NOTHING;

-- +goose Down
DROP TABLE IF EXISTS setup_state;
