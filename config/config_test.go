package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadFromEnv_AllVarsSet(t *testing.T) {
	// Set all required env vars
	t.Setenv("KEY_ID", "testkey")
	t.Setenv("SECRET", "testsecret")
	t.Setenv("ENDPOINT", "s3.example.com")
	t.Setenv("REGION", "us-east-1")
	t.Setenv("BUCKET", "test-bucket")
	t.Setenv("META_DB_PATH", "/tmp/test.sqlite")

	cfg, err := LoadFromEnv()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if cfg.S3KeyID != "testkey" {
		t.Errorf("S3KeyID = %q, want %q", cfg.S3KeyID, "testkey")
	}
	if cfg.S3Bucket != "test-bucket" {
		t.Errorf("S3Bucket = %q, want %q", cfg.S3Bucket, "test-bucket")
	}
	if cfg.MetaDBPath != "/tmp/test.sqlite" {
		t.Errorf("MetaDBPath = %q, want %q", cfg.MetaDBPath, "/tmp/test.sqlite")
	}
}

func TestLoadFromEnv_Defaults(t *testing.T) {
	t.Setenv("KEY_ID", "testkey")
	t.Setenv("SECRET", "testsecret")
	t.Setenv("ENDPOINT", "s3.example.com")
	t.Setenv("REGION", "us-east-1")
	// Don't set BUCKET, META_DB_PATH, PARQUET_PATH

	cfg, err := LoadFromEnv()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if cfg.S3Bucket != "duck-demo" {
		t.Errorf("S3Bucket default = %q, want %q", cfg.S3Bucket, "duck-demo")
	}
	if cfg.MetaDBPath != "ducklake_meta.sqlite" {
		t.Errorf("MetaDBPath default = %q, want %q", cfg.MetaDBPath, "ducklake_meta.sqlite")
	}
	if cfg.ParquetPath != "titanic.parquet" {
		t.Errorf("ParquetPath default = %q, want %q", cfg.ParquetPath, "titanic.parquet")
	}
}

func TestLoadFromEnv_MissingKeyID(t *testing.T) {
	t.Setenv("SECRET", "testsecret")
	t.Setenv("ENDPOINT", "s3.example.com")
	t.Setenv("REGION", "us-east-1")

	_, err := LoadFromEnv()
	if err == nil {
		t.Error("expected error for missing KEY_ID")
	}
}

func TestLoadFromEnv_MissingSecret(t *testing.T) {
	t.Setenv("KEY_ID", "testkey")
	t.Setenv("ENDPOINT", "s3.example.com")
	t.Setenv("REGION", "us-east-1")

	_, err := LoadFromEnv()
	if err == nil {
		t.Error("expected error for missing SECRET")
	}
}

func TestLoadFromEnv_MissingEndpoint(t *testing.T) {
	t.Setenv("KEY_ID", "testkey")
	t.Setenv("SECRET", "testsecret")
	t.Setenv("REGION", "us-east-1")

	_, err := LoadFromEnv()
	if err == nil {
		t.Error("expected error for missing ENDPOINT")
	}
}

func TestLoadFromEnv_MissingRegion(t *testing.T) {
	t.Setenv("KEY_ID", "testkey")
	t.Setenv("SECRET", "testsecret")
	t.Setenv("ENDPOINT", "s3.example.com")

	_, err := LoadFromEnv()
	if err == nil {
		t.Error("expected error for missing REGION")
	}
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
	// Clean up
	os.Unsetenv("TEST_KEY")
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
	os.Unsetenv("TEST_COMMENT_KEY")
}

func TestLoadDotEnv_EnvVarPrecedence(t *testing.T) {
	// Set env var first
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

	// Env var should take precedence over .env file
	if val := os.Getenv("TEST_PRECEDENCE_KEY"); val != "from_env" {
		t.Errorf("TEST_PRECEDENCE_KEY = %q, want %q (env precedence)", val, "from_env")
	}
}
