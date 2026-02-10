package config

import (
	"bufio"
	"fmt"
	"os"
	"strings"
)

// Config holds the configuration for DuckLake + S3 storage and the HTTP API.
type Config struct {
	S3KeyID     string
	S3Secret    string
	S3Endpoint  string
	S3Region    string
	S3Bucket    string
	MetaDBPath  string // path to SQLite metadata file
	ParquetPath string // path to local parquet file for initial data load
	JWTSecret   string // secret key for JWT token validation
	ListenAddr  string // HTTP listen address (default ":8080")
}

// LoadFromEnv loads configuration from environment variables.
// Expected env vars: KEY_ID, SECRET, ENDPOINT, REGION, BUCKET, META_DB_PATH.
func LoadFromEnv() (*Config, error) {
	cfg := &Config{
		S3KeyID:    os.Getenv("KEY_ID"),
		S3Secret:   os.Getenv("SECRET"),
		S3Endpoint: os.Getenv("ENDPOINT"),
		S3Region:   os.Getenv("REGION"),
		S3Bucket:   os.Getenv("BUCKET"),
		MetaDBPath: os.Getenv("META_DB_PATH"),
		JWTSecret:  os.Getenv("JWT_SECRET"),
		ListenAddr: os.Getenv("LISTEN_ADDR"),
	}

	if cfg.S3Bucket == "" {
		cfg.S3Bucket = "duck-demo"
	}
	if cfg.MetaDBPath == "" {
		cfg.MetaDBPath = "ducklake_meta.sqlite"
	}
	if cfg.ParquetPath == "" {
		cfg.ParquetPath = "titanic.parquet"
	}
	if cfg.ListenAddr == "" {
		cfg.ListenAddr = ":8080"
	}
	if cfg.JWTSecret == "" {
		cfg.JWTSecret = "dev-secret-change-in-production"
	}

	if cfg.S3KeyID == "" {
		return nil, fmt.Errorf("KEY_ID environment variable is required")
	}
	if cfg.S3Secret == "" {
		return nil, fmt.Errorf("SECRET environment variable is required")
	}
	if cfg.S3Endpoint == "" {
		return nil, fmt.Errorf("ENDPOINT environment variable is required")
	}
	if cfg.S3Region == "" {
		return nil, fmt.Errorf("REGION environment variable is required")
	}

	return cfg, nil
}

// LoadDotEnv reads a .env file and sets any variables not already in the environment.
// Lines must be in KEY=VALUE format. Comments (#) and blank lines are skipped.
func LoadDotEnv(path string) error {
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil // .env not found is not an error
		}
		return fmt.Errorf("open %s: %w", path, err)
	}
	defer f.Close()

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
			os.Setenv(key, value)
		}
	}
	return scanner.Err()
}
