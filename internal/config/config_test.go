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
