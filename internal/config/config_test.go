package config

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLoadFromEnv_AllVarsSet(t *testing.T) {
	t.Setenv("KEY_ID", "testkey")
	t.Setenv("SECRET", "testsecret")
	t.Setenv("ENDPOINT", "s3.example.com")
	t.Setenv("REGION", "us-east-1")
	t.Setenv("BUCKET", "test-bucket")
	t.Setenv("META_DB_PATH", "/tmp/test.sqlite")

	cfg, err := LoadFromEnv()
	require.NoError(t, err)

	require.NotNil(t, cfg.S3KeyID)
	assert.Equal(t, "testkey", *cfg.S3KeyID)
	require.NotNil(t, cfg.S3Bucket)
	assert.Equal(t, "test-bucket", *cfg.S3Bucket)
	assert.Equal(t, "/tmp/test.sqlite", cfg.MetaDBPath)
}

func TestLoadFromEnv_Defaults(t *testing.T) {
	// Clear all S3 vars
	t.Setenv("KEY_ID", "")
	t.Setenv("SECRET", "")
	t.Setenv("ENDPOINT", "")
	t.Setenv("REGION", "")
	t.Setenv("BUCKET", "")
	t.Setenv("META_DB_PATH", "")

	cfg, err := LoadFromEnv()
	require.NoError(t, err)

	assert.Nil(t, cfg.S3KeyID)
	assert.Nil(t, cfg.S3Bucket)
	assert.Equal(t, "ducklake_meta.sqlite", cfg.MetaDBPath)
	assert.Equal(t, ":8080", cfg.ListenAddr)
	assert.Equal(t, "dev-secret-change-in-production", cfg.JWTSecret)
	assert.Equal(t, "0000000000000000000000000000000000000000000000000000000000000000", cfg.EncryptionKey)
}

func TestLoadFromEnv_NoS3(t *testing.T) {
	t.Setenv("KEY_ID", "")
	t.Setenv("SECRET", "")
	t.Setenv("ENDPOINT", "")
	t.Setenv("REGION", "")

	cfg, err := LoadFromEnv()
	require.NoError(t, err)
	assert.Nil(t, cfg.S3KeyID)
	assert.Nil(t, cfg.S3Secret)
	assert.Nil(t, cfg.S3Endpoint)
	assert.Nil(t, cfg.S3Region)
	assert.False(t, cfg.HasS3Config())
}

func TestLoadFromEnv_WithS3(t *testing.T) {
	t.Setenv("KEY_ID", "testkey")
	t.Setenv("SECRET", "testsecret")
	t.Setenv("ENDPOINT", "s3.example.com")
	t.Setenv("REGION", "us-east-1")

	cfg, err := LoadFromEnv()
	require.NoError(t, err)
	assert.True(t, cfg.HasS3Config())
	require.NotNil(t, cfg.S3KeyID)
	assert.Equal(t, "testkey", *cfg.S3KeyID)
}

func TestHasS3Config_PartialConfig(t *testing.T) {
	t.Setenv("KEY_ID", "testkey")
	t.Setenv("SECRET", "")
	t.Setenv("ENDPOINT", "s3.example.com")
	t.Setenv("REGION", "")

	cfg, err := LoadFromEnv()
	require.NoError(t, err)
	assert.False(t, cfg.HasS3Config(), "partial S3 config should return false")
}

func TestLoadDotEnv_FileNotFound(t *testing.T) {
	err := LoadDotEnv("/nonexistent/.env")
	if err != nil {
		t.Errorf("expected no error for missing .env, got: %v", err)
	}
}

func TestLoadDotEnv_ParsesKeyValue(t *testing.T) {
	tmpDir := t.TempDir()
	envFile := filepath.Join(tmpDir, ".env")

	err := os.WriteFile(envFile, []byte("TEST_KEY=test_value\n"), 0644)
	if err != nil {
		t.Fatalf("write .env: %v", err)
	}

	if err := LoadDotEnv(envFile); err != nil {
		t.Fatalf("LoadDotEnv: %v", err)
	}

	if val := os.Getenv("TEST_KEY"); val != "test_value" {
		t.Errorf("TEST_KEY = %q, want %q", val, "test_value")
	}
	_ = os.Unsetenv("TEST_KEY")
}

func TestLoadDotEnv_SkipsComments(t *testing.T) {
	tmpDir := t.TempDir()
	envFile := filepath.Join(tmpDir, ".env")

	err := os.WriteFile(envFile, []byte("# comment\nTEST_COMMENT_KEY=value\n"), 0644)
	if err != nil {
		t.Fatalf("write .env: %v", err)
	}

	if err := LoadDotEnv(envFile); err != nil {
		t.Fatalf("LoadDotEnv: %v", err)
	}

	if val := os.Getenv("TEST_COMMENT_KEY"); val != "value" {
		t.Errorf("TEST_COMMENT_KEY = %q, want %q", val, "value")
	}
	_ = os.Unsetenv("TEST_COMMENT_KEY")
}

func TestLoadFromEnv_ProductionModeRejectsInsecureJWTSecret(t *testing.T) {
	t.Setenv("ENV", "production")
	t.Setenv("JWT_SECRET", "")
	t.Setenv("ENCRYPTION_KEY", "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789")

	_, err := LoadFromEnv()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "JWT_SECRET must be set in production")
}

func TestLoadFromEnv_ProductionModeRejectsInsecureEncryptionKey(t *testing.T) {
	t.Setenv("ENV", "production")
	t.Setenv("JWT_SECRET", "a-real-secret")
	t.Setenv("ENCRYPTION_KEY", "")

	_, err := LoadFromEnv()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "ENCRYPTION_KEY must be set in production")
}

func TestLoadFromEnv_ProductionModeAcceptsProperConfig(t *testing.T) {
	t.Setenv("ENV", "production")
	t.Setenv("JWT_SECRET", "a-real-secret")
	t.Setenv("ENCRYPTION_KEY", "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789")

	cfg, err := LoadFromEnv()
	require.NoError(t, err)
	assert.True(t, cfg.IsProduction())
}

func TestLoadFromEnv_RateLimitDefaults(t *testing.T) {
	t.Setenv("RATE_LIMIT_RPS", "")
	t.Setenv("RATE_LIMIT_BURST", "")

	cfg, err := LoadFromEnv()
	require.NoError(t, err)
	assert.InDelta(t, float64(100), cfg.RateLimitRPS, 0.001)
	assert.Equal(t, 200, cfg.RateLimitBurst)
}

func TestLoadFromEnv_RateLimitCustom(t *testing.T) {
	t.Setenv("RATE_LIMIT_RPS", "50.5")
	t.Setenv("RATE_LIMIT_BURST", "100")

	cfg, err := LoadFromEnv()
	require.NoError(t, err)
	assert.InDelta(t, 50.5, cfg.RateLimitRPS, 0.001)
	assert.Equal(t, 100, cfg.RateLimitBurst)
}

func TestLoadFromEnv_CORSDefaultWildcard(t *testing.T) {
	t.Setenv("CORS_ALLOWED_ORIGINS", "")

	cfg, err := LoadFromEnv()
	require.NoError(t, err)
	assert.Equal(t, []string{"*"}, cfg.CORSAllowedOrigins)
}

func TestLoadFromEnv_CORSCustomOrigins(t *testing.T) {
	t.Setenv("CORS_ALLOWED_ORIGINS", "https://app.example.com, https://admin.example.com")

	cfg, err := LoadFromEnv()
	require.NoError(t, err)
	assert.Equal(t, []string{"https://app.example.com", "https://admin.example.com"}, cfg.CORSAllowedOrigins)
}

func TestLoadFromEnv_CORSSingleOrigin(t *testing.T) {
	t.Setenv("CORS_ALLOWED_ORIGINS", "https://only.example.com")

	cfg, err := LoadFromEnv()
	require.NoError(t, err)
	assert.Equal(t, []string{"https://only.example.com"}, cfg.CORSAllowedOrigins)
}

func TestLoadDotEnv_StripsDoubleQuotes(t *testing.T) {
	tmpDir := t.TempDir()
	envFile := filepath.Join(tmpDir, ".env")

	err := os.WriteFile(envFile, []byte(`TEST_QUOTED_KEY="quoted_value"`+"\n"), 0644)
	require.NoError(t, err)

	require.NoError(t, LoadDotEnv(envFile))

	assert.Equal(t, "quoted_value", os.Getenv("TEST_QUOTED_KEY"))
	_ = os.Unsetenv("TEST_QUOTED_KEY")
}

func TestLoadDotEnv_StripsSingleQuotes(t *testing.T) {
	tmpDir := t.TempDir()
	envFile := filepath.Join(tmpDir, ".env")

	err := os.WriteFile(envFile, []byte("TEST_SINGLE_QUOTED='single_value'\n"), 0644)
	require.NoError(t, err)

	require.NoError(t, LoadDotEnv(envFile))

	assert.Equal(t, "single_value", os.Getenv("TEST_SINGLE_QUOTED"))
	_ = os.Unsetenv("TEST_SINGLE_QUOTED")
}

func TestLoadDotEnv_NoStripMismatchedQuotes(t *testing.T) {
	tmpDir := t.TempDir()
	envFile := filepath.Join(tmpDir, ".env")

	err := os.WriteFile(envFile, []byte(`TEST_MISMATCHED="mismatched'`+"\n"), 0644)
	require.NoError(t, err)

	require.NoError(t, LoadDotEnv(envFile))

	assert.Equal(t, `"mismatched'`, os.Getenv("TEST_MISMATCHED"))
	_ = os.Unsetenv("TEST_MISMATCHED")
}

func TestLoadDotEnv_EmptyQuotedValue(t *testing.T) {
	tmpDir := t.TempDir()
	envFile := filepath.Join(tmpDir, ".env")

	err := os.WriteFile(envFile, []byte(`TEST_EMPTY_QUOTED=""`+"\n"), 0644)
	require.NoError(t, err)

	require.NoError(t, LoadDotEnv(envFile))

	assert.Empty(t, os.Getenv("TEST_EMPTY_QUOTED"))
	_ = os.Unsetenv("TEST_EMPTY_QUOTED")
}

func TestLoadDotEnv_InternalQuotesPreserved(t *testing.T) {
	tmpDir := t.TempDir()
	envFile := filepath.Join(tmpDir, ".env")

	err := os.WriteFile(envFile, []byte(`TEST_INTERNAL="value with \"inner\" quotes"`+"\n"), 0644)
	require.NoError(t, err)

	require.NoError(t, LoadDotEnv(envFile))

	assert.Equal(t, `value with \"inner\" quotes`, os.Getenv("TEST_INTERNAL"))
	_ = os.Unsetenv("TEST_INTERNAL")
}

func TestStripQuotes(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  string
	}{
		{name: "double quotes", input: `"hello"`, want: "hello"},
		{name: "single quotes", input: `'hello'`, want: "hello"},
		{name: "no quotes", input: "hello", want: "hello"},
		{name: "empty string", input: "", want: ""},
		{name: "single char", input: `"`, want: `"`},
		{name: "mismatched quotes", input: `"hello'`, want: `"hello'`},
		{name: "empty double quotes", input: `""`, want: ""},
		{name: "empty single quotes", input: `''`, want: ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := stripQuotes(tt.input)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestLoadDotEnv_EnvVarPrecedence(t *testing.T) {
	t.Setenv("TEST_PRECEDENCE_KEY", "from_env")

	tmpDir := t.TempDir()
	envFile := filepath.Join(tmpDir, ".env")

	err := os.WriteFile(envFile, []byte("TEST_PRECEDENCE_KEY=from_file\n"), 0644)
	if err != nil {
		t.Fatalf("write .env: %v", err)
	}

	if err := LoadDotEnv(envFile); err != nil {
		t.Fatalf("LoadDotEnv: %v", err)
	}

	if val := os.Getenv("TEST_PRECEDENCE_KEY"); val != "from_env" {
		t.Errorf("TEST_PRECEDENCE_KEY = %q, want %q (env precedence)", val, "from_env")
	}
}
