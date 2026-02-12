package main

import (
	"fmt"
	"os"
	"strconv"
)

// AgentConfig holds configuration for the compute agent, loaded from environment variables.
type AgentConfig struct {
	CatalogDSN  string
	S3KeyID     string
	S3Secret    string
	S3Endpoint  string
	S3Region    string
	S3Bucket    string
	AgentToken  string
	ListenAddr  string
	MaxMemoryGB int
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
	}
	if v := os.Getenv("MAX_MEMORY_GB"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			return nil, fmt.Errorf("invalid MAX_MEMORY_GB: %w", err)
		}
		cfg.MaxMemoryGB = n
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
	return cfg, nil
}
