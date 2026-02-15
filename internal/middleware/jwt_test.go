package middleware

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// makeToken creates a signed HS256 JWT from the given secret and claims.
func makeToken(secret string, claims jwt.MapClaims) string {
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	signed, _ := token.SignedString([]byte(secret))
	return signed
}

func TestNewSharedSecretValidator(t *testing.T) {
	t.Parallel()

	v := NewSharedSecretValidator("my-secret")

	require.NotNil(t, v)
	assert.Equal(t, []byte("my-secret"), v.secret)
}

func TestSharedSecretValidator_Validate(t *testing.T) {
	t.Parallel()

	const secret = "test-secret-32-bytes-long-xxxxx"

	tests := []struct {
		name      string
		token     string
		wantErr   string
		wantSub   string
		wantIss   string
		wantEmail *string
		wantName  *string
		wantAud   []string
	}{
		{
			name: "valid token with all claims",
			token: makeToken(secret, jwt.MapClaims{
				"sub":   "user-123",
				"iss":   "https://auth.example.com",
				"email": "user@example.com",
				"name":  "Test User",
				"aud":   "my-app",
				"exp":   time.Now().Add(time.Hour).Unix(),
			}),
			wantSub:   "user-123",
			wantIss:   "https://auth.example.com",
			wantEmail: ptrStr("user@example.com"),
			wantName:  ptrStr("Test User"),
			wantAud:   []string{"my-app"},
		},
		{
			name: "valid token with only subject",
			token: makeToken(secret, jwt.MapClaims{
				"sub": "user-456",
				"exp": time.Now().Add(time.Hour).Unix(),
			}),
			wantSub:   "user-456",
			wantIss:   "",
			wantEmail: nil,
			wantName:  nil,
			wantAud:   nil,
		},
		{
			name: "valid token with audience as string",
			token: makeToken(secret, jwt.MapClaims{
				"sub": "user-789",
				"aud": "single-audience",
				"exp": time.Now().Add(time.Hour).Unix(),
			}),
			wantSub: "user-789",
			wantAud: []string{"single-audience"},
		},
		{
			name: "expired token returns error",
			token: makeToken(secret, jwt.MapClaims{
				"sub": "user-expired",
				"exp": time.Now().Add(-time.Hour).Unix(),
			}),
			wantErr: "jwt parse:",
		},
		{
			name: "wrong secret returns error",
			token: makeToken("wrong-secret", jwt.MapClaims{
				"sub": "user-wrong",
				"exp": time.Now().Add(time.Hour).Unix(),
			}),
			wantErr: "jwt parse:",
		},
		{
			name: "RS256 token rejected (wrong signing method)",
			token: func() string {
				key, _ := rsa.GenerateKey(rand.Reader, 2048)
				tok := jwt.NewWithClaims(jwt.SigningMethodRS256, jwt.MapClaims{
					"sub": "rsa-user",
					"exp": time.Now().Add(time.Hour).Unix(),
				})
				signed, _ := tok.SignedString(key)
				return signed
			}(),
			wantErr: "jwt parse:",
		},
		{
			name:    "malformed token returns error",
			token:   "not.a.valid.jwt.token",
			wantErr: "jwt parse:",
		},
		{
			name:    "empty token returns error",
			token:   "",
			wantErr: "jwt parse:",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			v := NewSharedSecretValidator(secret)
			claims, err := v.Validate(context.Background(), tt.token)

			if tt.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)
				assert.Nil(t, claims)
				return
			}

			require.NoError(t, err)
			require.NotNil(t, claims)

			assert.Equal(t, tt.wantSub, claims.Subject)
			assert.Equal(t, tt.wantIss, claims.Issuer)

			if tt.wantEmail != nil {
				require.NotNil(t, claims.Email)
				assert.Equal(t, *tt.wantEmail, *claims.Email)
			} else {
				assert.Nil(t, claims.Email)
			}

			if tt.wantName != nil {
				require.NotNil(t, claims.Name)
				assert.Equal(t, *tt.wantName, *claims.Name)
			} else {
				assert.Nil(t, claims.Name)
			}

			if tt.wantAud != nil {
				assert.Equal(t, tt.wantAud, claims.Audience)
			} else {
				assert.Nil(t, claims.Audience)
			}

			// Raw claims should always be populated for valid tokens.
			assert.NotNil(t, claims.Raw)
		})
	}
}

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

// ptrStr is a helper to create a *string from a literal.
func ptrStr(s string) *string {
	return &s
}
