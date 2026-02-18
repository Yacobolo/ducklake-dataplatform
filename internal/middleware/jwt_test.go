package middleware

import (
	"context"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
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

func TestHS256Validator_Validate(t *testing.T) {
	t.Parallel()

	validator, err := NewHS256Validator("dev-secret")
	require.NoError(t, err)

	now := time.Now()
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{
		"sub":   "admin-hunt",
		"email": "admin@example.com",
		"admin": true,
		"iat":   now.Unix(),
		"exp":   now.Add(time.Hour).Unix(),
	})
	signed, err := token.SignedString([]byte("dev-secret"))
	require.NoError(t, err)

	claims, err := validator.Validate(context.Background(), signed)
	require.NoError(t, err)
	assert.Equal(t, "admin-hunt", claims.Subject)
	require.NotNil(t, claims.Email)
	assert.Equal(t, "admin@example.com", *claims.Email)
	assert.True(t, claims.Raw["admin"].(bool))
}

func TestHS256Validator_InvalidSignature(t *testing.T) {
	t.Parallel()

	validator, err := NewHS256Validator("expected-secret")
	require.NoError(t, err)

	token := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{
		"sub": "admin-hunt",
		"exp": time.Now().Add(time.Hour).Unix(),
	})
	signed, err := token.SignedString([]byte("wrong-secret"))
	require.NoError(t, err)

	_, err = validator.Validate(context.Background(), signed)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "token verification failed")
}
