// Package config handles application configuration and environment loading.
package config

import (
	"bufio"
	"fmt"
	"log/slog"
	"os"
	"strings"
)

// Config holds the configuration for the HTTP API and optional S3/DuckLake storage.
type Config struct {
	// S3 fields are optional — nil when not configured.
	S3KeyID       *string
	S3Secret      *string
	S3Endpoint    *string
	S3Region      *string
	S3Bucket      *string
	MetaDBPath    string // path to SQLite metadata file
	JWTSecret     string // secret key for JWT token validation
	ListenAddr    string // HTTP listen address (default ":8080")
	EncryptionKey string // 64-char hex string (32-byte AES key) for encrypting stored credentials
	LogLevel      string // log level: debug, info, warn, error (default "info")
	CatalogDBType string // "sqlite" (default) or "postgres"
	CatalogDSN    string // PostgreSQL connection string (used when CatalogDBType is "postgres")

	// Warnings collects non-fatal warnings generated during config loading.
	// These are logged by the caller after the logger is initialised.
	Warnings []string
}

// SlogLevel maps the LogLevel string to an slog.Level.
func (c *Config) SlogLevel() slog.Level {
	switch strings.ToLower(c.LogLevel) {
	case "debug":
		return slog.LevelDebug
	case "warn", "warning":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}

// HasS3Config returns true if all required S3 fields are set.
func (c *Config) HasS3Config() bool {
	return c.S3KeyID != nil && c.S3Secret != nil &&
		c.S3Endpoint != nil && c.S3Region != nil
}

// LoadFromEnv loads configuration from environment variables.
// S3 variables are optional — the app can start without them.
func LoadFromEnv() (*Config, error) {
	cfg := &Config{
		MetaDBPath:    os.Getenv("META_DB_PATH"),
		JWTSecret:     os.Getenv("JWT_SECRET"),
		ListenAddr:    os.Getenv("LISTEN_ADDR"),
		EncryptionKey: os.Getenv("ENCRYPTION_KEY"),
		LogLevel:      os.Getenv("LOG_LEVEL"),
		CatalogDBType: os.Getenv("CATALOG_DB_TYPE"),
		CatalogDSN:    os.Getenv("CATALOG_DSN"),
	}

	// S3 fields are optional — only set if present
	if v := os.Getenv("KEY_ID"); v != "" {
		cfg.S3KeyID = &v
	}
	if v := os.Getenv("SECRET"); v != "" {
		cfg.S3Secret = &v
	}
	if v := os.Getenv("ENDPOINT"); v != "" {
		cfg.S3Endpoint = &v
	}
	if v := os.Getenv("REGION"); v != "" {
		cfg.S3Region = &v
	}
	if v := os.Getenv("BUCKET"); v != "" {
		cfg.S3Bucket = &v
	}

	// Defaults
	if cfg.MetaDBPath == "" {
		cfg.MetaDBPath = "ducklake_meta.sqlite"
	}
	if cfg.ListenAddr == "" {
		cfg.ListenAddr = ":8080"
	}
	if cfg.JWTSecret == "" {
		cfg.JWTSecret = "dev-secret-change-in-production"
		cfg.Warnings = append(cfg.Warnings, "JWT_SECRET not set — using insecure default. Set JWT_SECRET in production!")
	}
	if cfg.EncryptionKey == "" {
		cfg.EncryptionKey = "0000000000000000000000000000000000000000000000000000000000000000"
		cfg.Warnings = append(cfg.Warnings, "ENCRYPTION_KEY not set — using insecure default. Set ENCRYPTION_KEY in production!")
	}
	if cfg.LogLevel == "" {
		cfg.LogLevel = "info"
	}
	if cfg.CatalogDBType == "" {
		cfg.CatalogDBType = "sqlite"
	}

	return cfg, nil
}

// LoadDotEnv reads a .env file and sets any variables not already in the environment.
// Lines must be in KEY=VALUE format. Comments (#) and blank lines are skipped.
func LoadDotEnv(path string) error {
	f, err := os.Open(path) //nolint:gosec // path is caller-controlled
	if err != nil {
		if os.IsNotExist(err) {
			return nil // .env not found is not an error
		}
		return fmt.Errorf("open %s: %w", path, err)
	}
	defer f.Close() //nolint:errcheck

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		key, value, ok := strings.Cut(line, "=")
		if !ok {
			continue
		}
		key = strings.TrimSpace(key)
		value = strings.TrimSpace(value)
		// Only set if not already in the environment (env vars take precedence)
		if os.Getenv(key) == "" {
			if err := os.Setenv(key, value); err != nil {
				return fmt.Errorf("setenv %s: %w", key, err)
			}
		}
	}
	return scanner.Err()
}
