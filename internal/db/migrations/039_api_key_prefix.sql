-- +goose Up
ALTER TABLE api_keys ADD COLUMN key_prefix TEXT;

UPDATE api_keys
SET key_prefix = substr(key_hash, 1, 8)
WHERE key_prefix IS NULL OR key_prefix = '';

-- +goose Down
-- SQLite does not support DROP COLUMN directly in older versions.
-- No-op rollback for additive migration.
