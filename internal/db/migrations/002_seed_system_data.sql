-- +goose Up
INSERT INTO tags (id, key, value, created_by) VALUES
    ('1', 'classification', 'pii', 'system'),
    ('2', 'classification', 'sensitive', 'system'),
    ('3', 'classification', 'confidential', 'system'),
    ('4', 'classification', 'public', 'system'),
    ('5', 'classification', 'personal_data', 'system'),
    ('6', 'sensitivity', 'high', 'system'),
    ('7', 'sensitivity', 'medium', 'system'),
    ('8', 'sensitivity', 'low', 'system');

INSERT INTO setup_state (id, setup_completed) VALUES (1, 0);
INSERT INTO auth_providers (id, oidc_enabled) VALUES (1, 0);

-- +goose Down
DELETE FROM auth_providers WHERE id = 1;
DELETE FROM setup_state WHERE id = 1;
DELETE FROM tags WHERE id IN ('1', '2', '3', '4', '5', '6', '7', '8');
