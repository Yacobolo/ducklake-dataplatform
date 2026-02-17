// Package middleware provides HTTP middleware for JWT and API key authentication.
package middleware

import (
	"context"
	"fmt"

	"github.com/coreos/go-oidc/v3/oidc"
)

// JWTClaims holds the parsed claims from a validated JWT.
type JWTClaims struct {
	Subject  string
	Issuer   string
	Audience []string
	Email    *string
	Name     *string
	Raw      map[string]interface{}
}

// JWTValidator validates a JWT token and returns the parsed claims.
type JWTValidator interface {
	Validate(ctx context.Context, tokenString string) (*JWTClaims, error)
}

// OIDCValidator validates JWTs using OIDC discovery and JWKS.
type OIDCValidator struct {
	verifier       *oidc.IDTokenVerifier
	allowedIssuers map[string]bool
}

// NewOIDCValidator creates a validator from an OIDC issuer URL.
func NewOIDCValidator(ctx context.Context, issuerURL, audience string, allowedIssuers []string) (*OIDCValidator, error) {
	provider, err := oidc.NewProvider(ctx, issuerURL)
	if err != nil {
		return nil, fmt.Errorf("oidc provider discovery: %w", err)
	}
	verifier := provider.Verifier(&oidc.Config{
		ClientID: audience,
	})
	issuers := make(map[string]bool, len(allowedIssuers))
	for _, iss := range allowedIssuers {
		issuers[iss] = true
	}
	if len(issuers) == 0 {
		issuers[issuerURL] = true
	}
	return &OIDCValidator{verifier: verifier, allowedIssuers: issuers}, nil
}

// NewOIDCValidatorFromJWKS creates a validator from a JWKS URL (no OIDC discovery).
func NewOIDCValidatorFromJWKS(ctx context.Context, jwksURL, issuerURL, audience string, allowedIssuers []string) (*OIDCValidator, error) {
	keySet := oidc.NewRemoteKeySet(ctx, jwksURL)
	verifier := oidc.NewVerifier(issuerURL, keySet, &oidc.Config{
		ClientID: audience,
	})
	issuers := make(map[string]bool, len(allowedIssuers))
	for _, iss := range allowedIssuers {
		issuers[iss] = true
	}
	if len(issuers) == 0 && issuerURL != "" {
		issuers[issuerURL] = true
	}
	return &OIDCValidator{verifier: verifier, allowedIssuers: issuers}, nil
}

// Validate verifies the JWT using the OIDC provider's JWKS.
func (v *OIDCValidator) Validate(ctx context.Context, tokenString string) (*JWTClaims, error) {
	idToken, err := v.verifier.Verify(ctx, tokenString)
	if err != nil {
		return nil, fmt.Errorf("token verification failed: %w", err)
	}

	// Check issuer against allowlist.
	if len(v.allowedIssuers) > 0 && !v.allowedIssuers[idToken.Issuer] {
		return nil, fmt.Errorf("issuer %q not in allowed list", idToken.Issuer)
	}

	// Parse all claims.
	var raw map[string]interface{}
	if err := idToken.Claims(&raw); err != nil {
		return nil, fmt.Errorf("parse claims: %w", err)
	}

	claims := &JWTClaims{
		Subject:  idToken.Subject,
		Issuer:   idToken.Issuer,
		Audience: idToken.Audience,
		Raw:      raw,
	}
	if email, ok := raw["email"].(string); ok {
		claims.Email = &email
	}
	if name, ok := raw["name"].(string); ok {
		claims.Name = &name
	}

	return claims, nil
}
