-- +goose Up
INSERT OR IGNORE INTO tags (key, value, created_by, created_at) VALUES
    ('classification', 'pii', 'system', datetime('now')),
    ('classification', 'sensitive', 'system', datetime('now')),
    ('classification', 'confidential', 'system', datetime('now')),
    ('classification', 'public', 'system', datetime('now')),
    ('classification', 'personal_data', 'system', datetime('now')),
    ('sensitivity', 'high', 'system', datetime('now')),
    ('sensitivity', 'medium', 'system', datetime('now')),
    ('sensitivity', 'low', 'system', datetime('now'));

-- +goose Down
DELETE FROM tags WHERE created_by = 'system' AND key IN ('classification', 'sensitivity');
