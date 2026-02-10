-- +goose Up
CREATE TABLE groups (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL UNIQUE,
    description TEXT,
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE group_members (
    group_id INTEGER NOT NULL REFERENCES groups(id) ON DELETE CASCADE,
    member_type TEXT NOT NULL,
    member_id INTEGER NOT NULL,
    PRIMARY KEY (group_id, member_type, member_id)
);

-- +goose Down
DROP TABLE group_members;
DROP TABLE groups;
