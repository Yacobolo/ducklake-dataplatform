package db

import "embed"

// EmbedMigrations contains the embedded SQL migration files.
//
//go:embed migrations/*.sql
var EmbedMigrations embed.FS
