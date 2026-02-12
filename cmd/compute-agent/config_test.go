package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLoadAgentConfig(t *testing.T) {
	t.Run("required_agent_token", func(t *testing.T) {
		// Clear env to test required field
		t.Setenv("AGENT_TOKEN", "")
		_, err := loadAgentConfig()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "AGENT_TOKEN is required")
	})

	t.Run("defaults", func(t *testing.T) {
		t.Setenv("AGENT_TOKEN", "test-token")
		t.Setenv("CATALOG_DSN", "")
		t.Setenv("S3_KEY_ID", "")
		t.Setenv("S3_SECRET", "")
		t.Setenv("S3_ENDPOINT", "")
		t.Setenv("S3_REGION", "")
		t.Setenv("S3_BUCKET", "")
		t.Setenv("LISTEN_ADDR", "")
		t.Setenv("MAX_MEMORY_GB", "")

		cfg, err := loadAgentConfig()
		require.NoError(t, err)
		assert.Equal(t, "test-token", cfg.AgentToken)
		assert.Equal(t, ":9443", cfg.ListenAddr)   // default
		assert.Equal(t, "duck-demo", cfg.S3Bucket) // default
		assert.Equal(t, 0, cfg.MaxMemoryGB)
	})

	t.Run("custom_values", func(t *testing.T) {
		t.Setenv("AGENT_TOKEN", "secret-token")
		t.Setenv("CATALOG_DSN", "host=pg.example.com dbname=catalog")
		t.Setenv("S3_KEY_ID", "AKID123")
		t.Setenv("S3_SECRET", "secret123")
		t.Setenv("S3_ENDPOINT", "s3.amazonaws.com")
		t.Setenv("S3_REGION", "us-east-1")
		t.Setenv("S3_BUCKET", "custom-bucket")
		t.Setenv("LISTEN_ADDR", ":8080")
		t.Setenv("MAX_MEMORY_GB", "64")

		cfg, err := loadAgentConfig()
		require.NoError(t, err)
		assert.Equal(t, "secret-token", cfg.AgentToken)
		assert.Equal(t, "host=pg.example.com dbname=catalog", cfg.CatalogDSN)
		assert.Equal(t, "AKID123", cfg.S3KeyID)
		assert.Equal(t, "secret123", cfg.S3Secret)
		assert.Equal(t, "s3.amazonaws.com", cfg.S3Endpoint)
		assert.Equal(t, "us-east-1", cfg.S3Region)
		assert.Equal(t, "custom-bucket", cfg.S3Bucket)
		assert.Equal(t, ":8080", cfg.ListenAddr)
		assert.Equal(t, 64, cfg.MaxMemoryGB)
	})

	t.Run("invalid_max_memory_gb", func(t *testing.T) {
		t.Setenv("AGENT_TOKEN", "tok")
		t.Setenv("MAX_MEMORY_GB", "not-a-number")

		_, err := loadAgentConfig()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "invalid MAX_MEMORY_GB")
	})
}
