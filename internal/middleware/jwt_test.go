package middleware

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewOIDCValidatorFromJWKS(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		jwksURL        string
		issuerURL      string
		audience       string
		allowedIssuers []string
		wantIssuers    map[string]bool
	}{
		{
			name:           "populates allowed issuers from list",
			jwksURL:        "https://auth.example.com/.well-known/jwks.json",
			issuerURL:      "https://auth.example.com",
			audience:       "my-app",
			allowedIssuers: []string{"https://issuer1.example.com", "https://issuer2.example.com"},
			wantIssuers: map[string]bool{
				"https://issuer1.example.com": true,
				"https://issuer2.example.com": true,
			},
		},
		{
			name:           "empty allowed issuers defaults to issuer URL",
			jwksURL:        "https://auth.example.com/.well-known/jwks.json",
			issuerURL:      "https://auth.example.com",
			audience:       "my-app",
			allowedIssuers: nil,
			wantIssuers: map[string]bool{
				"https://auth.example.com": true,
			},
		},
		{
			name:           "empty allowed issuers with empty issuer URL",
			jwksURL:        "https://auth.example.com/.well-known/jwks.json",
			issuerURL:      "",
			audience:       "my-app",
			allowedIssuers: nil,
			wantIssuers:    map[string]bool{},
		},
		{
			name:           "single allowed issuer",
			jwksURL:        "https://auth.example.com/.well-known/jwks.json",
			issuerURL:      "https://auth.example.com",
			audience:       "my-app",
			allowedIssuers: []string{"https://custom-issuer.example.com"},
			wantIssuers: map[string]bool{
				"https://custom-issuer.example.com": true,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			v, err := NewOIDCValidatorFromJWKS(
				context.Background(),
				tt.jwksURL,
				tt.issuerURL,
				tt.audience,
				tt.allowedIssuers,
			)

			require.NoError(t, err)
			require.NotNil(t, v)
			assert.Equal(t, tt.wantIssuers, v.allowedIssuers)
			assert.NotNil(t, v.verifier)
		})
	}
}
