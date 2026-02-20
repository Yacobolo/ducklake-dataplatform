// Package config handles application configuration and environment loading.
package config

import (
	"bufio"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"time"
)

// AuthConfig holds authentication and identity provider configuration.
type AuthConfig struct {
	// OIDC / JWKS configuration
	IssuerURL      string        // OIDC issuer URL (e.g., https://login.microsoftonline.com/{tenant}/v2.0)
	JWKSURL        string        // Override JWKS URL (if no .well-known discovery)
	JWTSecret      string        // HS256 shared secret for local/dev JWT auth
	Audience       string        // Required JWT audience claim
	AllowedIssuers []string      // Accepted issuers (defaults to [IssuerURL])
	JWKSCacheTTL   time.Duration // JWKS cache duration (default: 1h)

	// API key settings
	APIKeyEnabled bool   // Enable API key auth (default: true)
	APIKeyHeader  string // Header name for API keys (default: X-API-Key)

	// JIT provisioning
	NameClaim      string // JWT claim for principal name (default: "email")
	BootstrapAdmin string // External ID (sub) of the bootstrap admin user
}

// OIDCEnabled returns true when an external identity provider is configured.
func (a *AuthConfig) OIDCEnabled() bool {
	return a.IssuerURL != "" || a.JWKSURL != ""
}

// Validate checks that the auth configuration is internally consistent.
func (a *AuthConfig) Validate() error {
	if a.IssuerURL == "" && a.JWKSURL == "" {
		return fmt.Errorf("at least one of AUTH_ISSUER_URL or AUTH_JWKS_URL must be set")
	}
	if a.IssuerURL != "" && a.Audience == "" {
		return fmt.Errorf("AUTH_AUDIENCE is required when AUTH_ISSUER_URL is set")
	}
	return nil
}

// Config holds the configuration for the HTTP API and optional S3/DuckLake storage.
type Config struct {
	// S3 fields are optional — nil when not configured.
	S3KeyID           *string
	S3Secret          *string
	S3Endpoint        *string
	S3Region          *string
	S3Bucket          *string
	MetaDBPath        string // path to SQLite metadata file (control plane)
	ListenAddr        string // HTTP listen address (default ":8080")
	TLSCertFile       string // TLS certificate file path (optional)
	TLSKeyFile        string // TLS private key file path (optional)
	AllowInsecureHTTP bool   // allow non-TLS listener in production (for trusted TLS termination)
	EncryptionKey     string // 64-char hex string (32-byte AES key) for encrypting stored credentials
	LogLevel          string // log level: debug, info, warn, error (default "info")
	Env               string // environment: "development" (default) or "production"

	// Rate limiting
	RateLimitRPS   float64 // sustained requests per second (default 100)
	RateLimitBurst int     // burst capacity (default 200)

	// CORS
	CORSAllowedOrigins []string // allowed origins for CORS (default: ["*"])

	// Auth holds identity provider and authentication configuration.
	Auth AuthConfig

	// Distributed execution feature controls.
	FeatureRemoteRouting bool
	FeatureAsyncQueue    bool
	FeatureCursorMode    bool
	RemoteCanaryUsers    []string

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

// IsProduction returns true when the server is running in production mode.
func (c *Config) IsProduction() bool {
	return strings.EqualFold(c.Env, "production")
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
		MetaDBPath:           os.Getenv("META_DB_PATH"),
		ListenAddr:           os.Getenv("LISTEN_ADDR"),
		TLSCertFile:          os.Getenv("TLS_CERT_FILE"),
		TLSKeyFile:           os.Getenv("TLS_KEY_FILE"),
		EncryptionKey:        os.Getenv("ENCRYPTION_KEY"),
		LogLevel:             os.Getenv("LOG_LEVEL"),
		Env:                  os.Getenv("ENV"),
		FeatureRemoteRouting: parseBoolEnvDefault("FEATURE_REMOTE_ROUTING", true),
		FeatureAsyncQueue:    parseBoolEnvDefault("FEATURE_ASYNC_QUEUE", true),
		FeatureCursorMode:    parseBoolEnvDefault("FEATURE_CURSOR_MODE", true),
	}

	// Rate limiting
	if v := os.Getenv("RATE_LIMIT_RPS"); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			cfg.RateLimitRPS = f
		}
	}
	if v := os.Getenv("RATE_LIMIT_BURST"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			cfg.RateLimitBurst = n
		}
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

	// CORS
	if v := os.Getenv("CORS_ALLOWED_ORIGINS"); v != "" {
		origins := strings.Split(v, ",")
		for i := range origins {
			origins[i] = strings.TrimSpace(origins[i])
		}
		cfg.CORSAllowedOrigins = origins
	}
	if strings.EqualFold(os.Getenv("ALLOW_INSECURE_HTTP"), "true") {
		cfg.AllowInsecureHTTP = true
	}
	if v := os.Getenv("REMOTE_CANARY_USERS"); v != "" {
		users := strings.Split(v, ",")
		for i := range users {
			users[i] = strings.TrimSpace(users[i])
		}
		cfg.RemoteCanaryUsers = compactNonEmpty(users)
	}

	// Auth config
	cfg.Auth = AuthConfig{
		IssuerURL:      os.Getenv("AUTH_ISSUER_URL"),
		JWKSURL:        os.Getenv("AUTH_JWKS_URL"),
		JWTSecret:      os.Getenv("JWT_SECRET"),
		Audience:       os.Getenv("AUTH_AUDIENCE"),
		APIKeyEnabled:  true,
		APIKeyHeader:   os.Getenv("AUTH_API_KEY_HEADER"),
		NameClaim:      os.Getenv("AUTH_NAME_CLAIM"),
		BootstrapAdmin: os.Getenv("AUTH_BOOTSTRAP_ADMIN"),
	}

	if v := os.Getenv("AUTH_ALLOWED_ISSUERS"); v != "" {
		cfg.Auth.AllowedIssuers = strings.Split(v, ",")
	}
	if v := os.Getenv("AUTH_JWKS_CACHE_TTL"); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			cfg.Auth.JWKSCacheTTL = d
		}
	}
	if os.Getenv("AUTH_API_KEY_ENABLED") == "false" {
		cfg.Auth.APIKeyEnabled = false
	}

	// Auth config defaults
	if cfg.Auth.JWKSCacheTTL == 0 {
		cfg.Auth.JWKSCacheTTL = time.Hour
	}
	if cfg.Auth.APIKeyHeader == "" {
		cfg.Auth.APIKeyHeader = "X-API-Key"
	}
	if cfg.Auth.NameClaim == "" {
		cfg.Auth.NameClaim = "email"
	}

	// Defaults
	if cfg.MetaDBPath == "" {
		cfg.MetaDBPath = "ducklake_meta.sqlite"
	}
	if cfg.ListenAddr == "" {
		cfg.ListenAddr = ":8080"
	}
	if (cfg.TLSCertFile == "") != (cfg.TLSKeyFile == "") {
		return nil, fmt.Errorf("both TLS_CERT_FILE and TLS_KEY_FILE must be set together")
	}
	if !cfg.Auth.OIDCEnabled() {
		cfg.Warnings = append(cfg.Warnings, "OIDC is not configured — set AUTH_ISSUER_URL or AUTH_JWKS_URL")
	}
	if cfg.EncryptionKey == "" {
		cfg.EncryptionKey = "0000000000000000000000000000000000000000000000000000000000000000"
		cfg.Warnings = append(cfg.Warnings, "ENCRYPTION_KEY not set — using insecure default. Set ENCRYPTION_KEY in production!")
	}
	if cfg.LogLevel == "" {
		cfg.LogLevel = "info"
	}
	if cfg.RateLimitRPS == 0 {
		cfg.RateLimitRPS = 100
	}
	if cfg.RateLimitBurst == 0 {
		cfg.RateLimitBurst = 200
	}
	if len(cfg.CORSAllowedOrigins) == 0 {
		cfg.CORSAllowedOrigins = []string{"*"}
	}

	// Production mode: insecure defaults are fatal errors.
	if cfg.IsProduction() {
		if !cfg.Auth.OIDCEnabled() {
			return nil, fmt.Errorf("OIDC must be configured in production (set AUTH_ISSUER_URL or AUTH_JWKS_URL)")
		}
		if cfg.EncryptionKey == "0000000000000000000000000000000000000000000000000000000000000000" {
			return nil, fmt.Errorf("ENCRYPTION_KEY must be set in production (ENV=production)")
		}
		if len(cfg.CORSAllowedOrigins) == 1 && cfg.CORSAllowedOrigins[0] == "*" {
			return nil, fmt.Errorf("CORS wildcard (*) is not allowed in production (ENV=production)")
		}
		if cfg.TLSCertFile == "" && !cfg.AllowInsecureHTTP {
			return nil, fmt.Errorf("TLS_CERT_FILE/TLS_KEY_FILE must be set in production unless ALLOW_INSECURE_HTTP=true")
		}
	}

	return cfg, nil
}

func parseBoolEnvDefault(key string, defaultVal bool) bool {
	v := strings.TrimSpace(strings.ToLower(os.Getenv(key)))
	if v == "" {
		return defaultVal
	}
	if v == "0" || v == "false" || v == "no" || v == "off" {
		return false
	}
	if v == "1" || v == "true" || v == "yes" || v == "on" {
		return true
	}
	return defaultVal
}

func compactNonEmpty(values []string) []string {
	out := make([]string, 0, len(values))
	for _, v := range values {
		if v != "" {
			out = append(out, v)
		}
	}
	return out
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
		value = stripQuotes(value)
		// Only set if not already in the environment (env vars take precedence)
		if os.Getenv(key) == "" {
			if err := os.Setenv(key, value); err != nil {
				return fmt.Errorf("setenv %s: %w", key, err)
			}
		}
	}
	return scanner.Err()
}

// stripQuotes removes surrounding double or single quotes from a value.
// Only strips if both the first and last characters are matching quotes.
func stripQuotes(s string) string {
	if len(s) >= 2 {
		if (s[0] == '"' && s[len(s)-1] == '"') || (s[0] == '\'' && s[len(s)-1] == '\'') {
			return s[1 : len(s)-1]
		}
	}
	return s
}
