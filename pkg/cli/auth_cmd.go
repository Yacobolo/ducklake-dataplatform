package cli

import (
	"fmt"
	"os"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/spf13/cobra"
)

func newAuthCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "auth",
		Short: "Authentication helpers",
	}

	cmd.AddCommand(newAuthTokenCmd())
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
		Example: `  # Generate a token for admin_user with the default dev secret
  duck auth token --principal admin_user --secret dev-secret-change-in-production

  # Generate an admin token with custom expiry
  duck auth token --principal admin_user --admin --secret mysecret --expires 48h`,
		RunE: func(_ *cobra.Command, _ []string) error {
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

			// Save to active profile
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
			p.Token = signed
			if cfg.Profiles == nil {
				cfg.Profiles = make(map[string]Profile)
			}
			cfg.Profiles[profileName] = p
			if err := SaveUserConfig(cfg); err != nil {
				return fmt.Errorf("save config: %w", err)
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
