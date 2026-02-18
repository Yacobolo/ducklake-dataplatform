package main

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// AgentConfig holds configuration for the compute agent, loaded from environment variables.
type AgentConfig struct {
	CatalogDSN            string
	S3KeyID               string
	S3Secret              string
	S3Endpoint            string
	S3Region              string
	S3Bucket              string
	AgentToken            string
	ListenAddr            string
	MaxMemoryGB           int
	Env                   string
	TLSCertFile           string
	TLSKeyFile            string
	AllowInsecureHTTP     bool
	QueryTimeout          time.Duration
	MaxResultRows         int
	MaxConcurrentQueries  int
	RequireSignedRequests bool
	SignatureMaxSkew      time.Duration
}

func loadAgentConfig() (*AgentConfig, error) {
	cfg := &AgentConfig{
		CatalogDSN:  os.Getenv("CATALOG_DSN"),
		S3KeyID:     os.Getenv("S3_KEY_ID"),
		S3Secret:    os.Getenv("S3_SECRET"),
		S3Endpoint:  os.Getenv("S3_ENDPOINT"),
		S3Region:    os.Getenv("S3_REGION"),
		S3Bucket:    os.Getenv("S3_BUCKET"),
		AgentToken:  os.Getenv("AGENT_TOKEN"),
		ListenAddr:  os.Getenv("LISTEN_ADDR"),
		Env:         os.Getenv("ENV"),
		TLSCertFile: os.Getenv("TLS_CERT_FILE"),
		TLSKeyFile:  os.Getenv("TLS_KEY_FILE"),
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
	if strings.EqualFold(os.Getenv("ALLOW_INSECURE_HTTP"), "true") {
		cfg.AllowInsecureHTTP = true
	}
	if v := os.Getenv("AGENT_QUERY_TIMEOUT"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			return nil, fmt.Errorf("invalid AGENT_QUERY_TIMEOUT: %w", err)
		}
		cfg.QueryTimeout = d
	}
	if cfg.QueryTimeout == 0 {
		cfg.QueryTimeout = 2 * time.Minute
	}
	if v := os.Getenv("AGENT_MAX_RESULT_ROWS"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			return nil, fmt.Errorf("invalid AGENT_MAX_RESULT_ROWS: %w", err)
		}
		cfg.MaxResultRows = n
	}
	if cfg.MaxResultRows <= 0 {
		cfg.MaxResultRows = 10000
	}
	if v := os.Getenv("AGENT_MAX_CONCURRENT_QUERIES"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			return nil, fmt.Errorf("invalid AGENT_MAX_CONCURRENT_QUERIES: %w", err)
		}
		cfg.MaxConcurrentQueries = n
	}
	if cfg.MaxConcurrentQueries <= 0 {
		cfg.MaxConcurrentQueries = 8
	}
	if v := os.Getenv("AGENT_REQUIRE_SIGNED_REQUESTS"); v != "" {
		cfg.RequireSignedRequests = strings.EqualFold(v, "true")
	} else {
		cfg.RequireSignedRequests = true
	}
	if v := os.Getenv("AGENT_SIGNATURE_MAX_SKEW"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			return nil, fmt.Errorf("invalid AGENT_SIGNATURE_MAX_SKEW: %w", err)
		}
		cfg.SignatureMaxSkew = d
	}
	if cfg.SignatureMaxSkew <= 0 {
		cfg.SignatureMaxSkew = 2 * time.Minute
	}
	if (cfg.TLSCertFile == "") != (cfg.TLSKeyFile == "") {
		return nil, fmt.Errorf("both TLS_CERT_FILE and TLS_KEY_FILE must be set together")
	}
	if strings.EqualFold(cfg.Env, "production") && cfg.TLSCertFile == "" && !cfg.AllowInsecureHTTP {
		return nil, fmt.Errorf("TLS_CERT_FILE/TLS_KEY_FILE must be set in production unless ALLOW_INSECURE_HTTP=true")
	}
	return cfg, nil
}
