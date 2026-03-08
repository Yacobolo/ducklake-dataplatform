package cli

import (
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/spf13/cobra"

	"duck-demo/pkg/cli/apiruntime"
)

func newAuthCmd(client *apiruntime.Client) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "auth",
		Short: "Authentication helpers",
	}

	cmd.AddCommand(newAuthTokenCmd())
	cmd.AddCommand(newAuthLocalLoginCmd(client))
	cmd.AddCommand(newAuthBootstrapCmd(client))
	cmd.AddCommand(newAuthProviderCmd(client))
	return cmd
}

func newAuthTokenCmd() *cobra.Command {
	var (
		principal string
		secret    string
		admin     bool
		expires   time.Duration
	)

	cmd := &cobra.Command{
		Use:   "token",
		Short: "Generate a dev-mode JWT token and save it to the active profile",
		Long:  "Generate an HS256 JWT token for development and testing. The token is saved to the active profile automatically.",
		RunE: func(cmd *cobra.Command, _ []string) error {
			now := time.Now()
			claims := jwt.MapClaims{
				"sub": principal,
				"iat": now.Unix(),
				"exp": now.Add(expires).Unix(),
			}
			if admin {
				claims["admin"] = true
			}

			token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
			signed, err := token.SignedString([]byte(secret))
			if err != nil {
				return fmt.Errorf("sign token: %w", err)
			}
			if err := saveTokenToActiveProfile(signed); err != nil {
				return err
			}

			if getOutputFormat(cmd) == "json" {
				return apiruntime.PrintJSON(os.Stdout, map[string]string{"token": signed, "principal": principal})
			}
			_, _ = fmt.Fprintln(os.Stdout, signed)
			return nil
		},
	}

	cmd.Flags().StringVar(&principal, "principal", "", "Principal name (JWT sub claim)")
	cmd.Flags().StringVar(&secret, "secret", "", "JWT signing secret (HS256)")
	cmd.Flags().BoolVar(&admin, "admin", false, "Include admin claim in the token")
	cmd.Flags().DurationVar(&expires, "expires", 24*time.Hour, "Token expiry duration")
	_ = cmd.MarkFlagRequired("principal")
	_ = cmd.MarkFlagRequired("secret")

	return cmd
}

func newAuthLocalLoginCmd(client *apiruntime.Client) *cobra.Command {
	var username string
	var password string

	cmd := &cobra.Command{
		Use:   "local-login",
		Short: "Login with local username/password",
		RunE: func(cmd *cobra.Command, _ []string) error {
			resp, err := client.Do("POST", "/auth/local/login", nil, map[string]string{
				"username": username,
				"password": password,
			})
			if err != nil {
				return err
			}
			if err := apiruntime.CheckError(resp); err != nil {
				return err
			}
			body, err := apiruntime.ReadBody(resp)
			if err != nil {
				return err
			}
			var payload map[string]interface{}
			if err := json.Unmarshal(body, &payload); err != nil {
				return err
			}
			token, _ := payload["token"].(string)
			if token == "" {
				return fmt.Errorf("login response missing token")
			}
			if err := saveTokenToActiveProfile(token); err != nil {
				return err
			}
			if getOutputFormat(cmd) == "json" {
				return apiruntime.PrintJSON(os.Stdout, payload)
			}
			_, _ = fmt.Fprintln(os.Stdout, "local login succeeded; token saved to active profile")
			return nil
		},
	}

	cmd.Flags().StringVar(&username, "username", "", "Local username")
	cmd.Flags().StringVar(&password, "password", "", "Local password")
	_ = cmd.MarkFlagRequired("username")
	_ = cmd.MarkFlagRequired("password")
	return cmd
}

func newAuthBootstrapCmd(client *apiruntime.Client) *cobra.Command {
	cmd := &cobra.Command{Use: "bootstrap", Short: "Bootstrap authentication flows"}
	cmd.AddCommand(newAuthBootstrapCompleteCmd(client))
	cmd.AddCommand(newAuthBootstrapTokenCreateCmd(client))
	return cmd
}

func newAuthBootstrapCompleteCmd(client *apiruntime.Client) *cobra.Command {
	var username string
	var password string
	var principalName string
	var bootstrapToken string

	cmd := &cobra.Command{
		Use:   "complete",
		Short: "Complete first-admin bootstrap",
		RunE: func(cmd *cobra.Command, _ []string) error {
			resp, err := client.Do("POST", "/auth/bootstrap/complete", nil, map[string]string{
				"username":        username,
				"password":        password,
				"principal_name":  principalName,
				"bootstrap_token": bootstrapToken,
			})
			if err != nil {
				return err
			}
			if err := apiruntime.CheckError(resp); err != nil {
				return err
			}
			body, err := apiruntime.ReadBody(resp)
			if err != nil {
				return err
			}
			var payload map[string]interface{}
			if err := json.Unmarshal(body, &payload); err != nil {
				return err
			}
			token, _ := payload["token"].(string)
			if token != "" {
				if err := saveTokenToActiveProfile(token); err != nil {
					return err
				}
			}
			if getOutputFormat(cmd) == "json" {
				return apiruntime.PrintJSON(os.Stdout, payload)
			}
			_, _ = fmt.Fprintln(os.Stdout, "bootstrap completed; token saved to active profile")
			return nil
		},
	}
	cmd.Flags().StringVar(&username, "username", "", "Bootstrap admin username")
	cmd.Flags().StringVar(&password, "password", "", "Bootstrap admin password")
	cmd.Flags().StringVar(&principalName, "principal", "", "Principal name (defaults to username)")
	cmd.Flags().StringVar(&bootstrapToken, "token", "", "Optional bootstrap token for recovery bootstrap")
	_ = cmd.MarkFlagRequired("username")
	_ = cmd.MarkFlagRequired("password")
	return cmd
}

func newAuthBootstrapTokenCreateCmd(client *apiruntime.Client) *cobra.Command {
	var ttl time.Duration

	cmd := &cobra.Command{
		Use:   "token-create",
		Short: "Create a one-time bootstrap token (admin only)",
		RunE: func(cmd *cobra.Command, _ []string) error {
			resp, err := client.Do("POST", "/auth/bootstrap/tokens", nil, map[string]int64{"ttl_seconds": int64(ttl.Seconds())})
			if err != nil {
				return err
			}
			if err := apiruntime.CheckError(resp); err != nil {
				return err
			}
			body, err := apiruntime.ReadBody(resp)
			if err != nil {
				return err
			}
			if getOutputFormat(cmd) == "json" {
				var payload map[string]interface{}
				_ = json.Unmarshal(body, &payload)
				return apiruntime.PrintJSON(os.Stdout, payload)
			}
			_, _ = fmt.Fprintln(os.Stdout, string(body))
			return nil
		},
	}
	cmd.Flags().DurationVar(&ttl, "ttl", 30*time.Minute, "Bootstrap token TTL")
	return cmd
}

func newAuthProviderCmd(client *apiruntime.Client) *cobra.Command {
	cmd := &cobra.Command{Use: "provider", Short: "Manage auth providers"}
	oidc := &cobra.Command{Use: "oidc", Short: "Manage OIDC provider settings"}
	oidc.AddCommand(newAuthProviderOIDCGetCmd(client))
	oidc.AddCommand(newAuthProviderOIDCSetCmd(client))
	oidc.AddCommand(newAuthProviderOIDCDisableCmd(client))
	cmd.AddCommand(oidc)
	return cmd
}

func newAuthProviderOIDCGetCmd(client *apiruntime.Client) *cobra.Command {
	return &cobra.Command{
		Use:   "get",
		Short: "Get OIDC provider configuration",
		RunE: func(cmd *cobra.Command, _ []string) error {
			resp, err := client.Do("GET", "/auth/provider/oidc", nil, nil)
			if err != nil {
				return err
			}
			if err := apiruntime.CheckError(resp); err != nil {
				return err
			}
			body, err := apiruntime.ReadBody(resp)
			if err != nil {
				return err
			}
			if getOutputFormat(cmd) == "json" {
				var payload map[string]interface{}
				_ = json.Unmarshal(body, &payload)
				return apiruntime.PrintJSON(os.Stdout, payload)
			}
			_, _ = fmt.Fprintln(os.Stdout, string(body))
			return nil
		},
	}
}

func newAuthProviderOIDCSetCmd(client *apiruntime.Client) *cobra.Command {
	var issuer string
	var jwks string
	var audience string
	var clientID string
	var clientSecret string
	var scopes string

	cmd := &cobra.Command{
		Use:   "set",
		Short: "Set OIDC provider configuration",
		RunE: func(_ *cobra.Command, _ []string) error {
			resp, err := client.Do("PUT", "/auth/provider/oidc", nil, map[string]interface{}{
				"enabled":       true,
				"issuer_url":    issuer,
				"jwks_url":      jwks,
				"audience":      audience,
				"client_id":     clientID,
				"client_secret": clientSecret,
				"scopes":        scopes,
			})
			if err != nil {
				return err
			}
			if err := apiruntime.CheckError(resp); err != nil {
				return err
			}
			_, _ = fmt.Fprintln(os.Stdout, "oidc provider updated")
			return nil
		},
	}

	cmd.Flags().StringVar(&issuer, "issuer", "", "OIDC issuer URL")
	cmd.Flags().StringVar(&jwks, "jwks", "", "Optional OIDC JWKS URL")
	cmd.Flags().StringVar(&audience, "audience", "", "OIDC audience")
	cmd.Flags().StringVar(&clientID, "client-id", "", "OIDC client ID")
	cmd.Flags().StringVar(&clientSecret, "client-secret", "", "OIDC client secret")
	cmd.Flags().StringVar(&scopes, "scopes", "openid profile email", "OIDC scopes")
	_ = cmd.MarkFlagRequired("issuer")
	return cmd
}

func newAuthProviderOIDCDisableCmd(client *apiruntime.Client) *cobra.Command {
	return &cobra.Command{
		Use:   "disable",
		Short: "Disable OIDC provider",
		RunE: func(_ *cobra.Command, _ []string) error {
			resp, err := client.Do("PUT", "/auth/provider/oidc", nil, map[string]interface{}{"enabled": false})
			if err != nil {
				return err
			}
			if err := apiruntime.CheckError(resp); err != nil {
				return err
			}
			_, _ = fmt.Fprintln(os.Stdout, "oidc provider disabled")
			return nil
		},
	}
}

func saveTokenToActiveProfile(token string) error {
	cfg, err := LoadUserConfig()
	if err != nil {
		cfg = &UserConfig{Profiles: make(map[string]Profile)}
	}
	profileName := cfg.CurrentProfile
	if profileName == "" {
		profileName = "default"
		cfg.CurrentProfile = profileName
	}
	p := cfg.Profiles[profileName]
	p.Token = token
	if cfg.Profiles == nil {
		cfg.Profiles = make(map[string]Profile)
	}
	cfg.Profiles[profileName] = p
	if err := SaveUserConfig(cfg); err != nil {
		return fmt.Errorf("save config: %w", err)
	}
	return nil
}
