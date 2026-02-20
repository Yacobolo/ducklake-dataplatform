package main

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

// AgentConfig holds configuration for the compute agent, loaded from environment variables.
type AgentConfig struct {
	CatalogDSN      string
	S3KeyID         string
	S3Secret        string
	S3Endpoint      string
	S3Region        string
	S3Bucket        string
	AgentToken      string
	ListenAddr      string
	MaxMemoryGB     int
	QueryResultTTL  time.Duration
	CleanupInterval time.Duration
	CursorMode      bool
}

func loadAgentConfig() (*AgentConfig, error) {
	cfg := &AgentConfig{
		CatalogDSN: os.Getenv("CATALOG_DSN"),
		S3KeyID:    os.Getenv("S3_KEY_ID"),
		S3Secret:   os.Getenv("S3_SECRET"),
		S3Endpoint: os.Getenv("S3_ENDPOINT"),
		S3Region:   os.Getenv("S3_REGION"),
		S3Bucket:   os.Getenv("S3_BUCKET"),
		AgentToken: os.Getenv("AGENT_TOKEN"),
		ListenAddr: os.Getenv("LISTEN_ADDR"),
		CursorMode: true,
	}
	if v := os.Getenv("FEATURE_CURSOR_MODE"); v != "" {
		switch v {
		case "0", "false", "FALSE", "off", "OFF", "no", "NO":
			cfg.CursorMode = false
		}
	}
	if v := os.Getenv("MAX_MEMORY_GB"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			return nil, fmt.Errorf("invalid MAX_MEMORY_GB: %w", err)
		}
		cfg.MaxMemoryGB = n
	}
	if v := os.Getenv("QUERY_RESULT_TTL"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			return nil, fmt.Errorf("invalid QUERY_RESULT_TTL: %w", err)
		}
		cfg.QueryResultTTL = d
	}
	if v := os.Getenv("QUERY_CLEANUP_INTERVAL"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			return nil, fmt.Errorf("invalid QUERY_CLEANUP_INTERVAL: %w", err)
		}
		cfg.CleanupInterval = d
	}
	if cfg.AgentToken == "" {
		return nil, fmt.Errorf("AGENT_TOKEN is required")
	}
	if cfg.ListenAddr == "" {
		cfg.ListenAddr = ":9443"
	}
	if cfg.S3Bucket == "" {
		cfg.S3Bucket = "duck-demo"
	}
	if cfg.QueryResultTTL <= 0 {
		cfg.QueryResultTTL = 10 * time.Minute
	}
	if cfg.CleanupInterval <= 0 {
		cfg.CleanupInterval = 1 * time.Minute
	}
	return cfg, nil
}
