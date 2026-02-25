package cli

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"

	"duck-demo/pkg/cli/gen"
)

func newConfigCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "config",
		Short: "Manage CLI configuration profiles",
	}

	cmd.AddCommand(newConfigShowCmd())
	cmd.AddCommand(newConfigSetProfileCmd())
	cmd.AddCommand(newConfigUseProfileCmd())

	return cmd
}

func newConfigShowCmd() *cobra.Command {
	var reveal bool

	cmd := &cobra.Command{
		Use:   "show",
		Short: "Display current configuration",
		RunE: func(cmd *cobra.Command, _ []string) error {
			cfg, err := LoadUserConfig()
			if err != nil {
				_, _ = fmt.Fprintf(os.Stderr, "No configuration found at %s\n", ConfigPath())
				return err
			}
			if !reveal {
				cfg = maskConfig(cfg)
			}
			if getOutputFormat(cmd) == "json" {
				return gen.PrintJSON(os.Stdout, cfg)
			}
			printConfigTable(cfg)
			return nil
		},
	}

	cmd.Flags().BoolVar(&reveal, "reveal", false, "Show sensitive values unmasked")

	return cmd
}

func printConfigTable(cfg *UserConfig) {
	columns := []string{"profile", "current", "host", "api_key", "token", "output"}
	rows := make([]map[string]interface{}, 0, len(cfg.Profiles))
	for name, p := range cfg.Profiles {
		current := ""
		if name == cfg.CurrentProfile {
			current = "*"
		}
		rows = append(rows, map[string]interface{}{
			"profile": name,
			"current": current,
			"host":    p.Host,
			"api_key": p.APIKey,
			"token":   p.Token,
			"output":  p.Output,
		})
	}
	gen.PrintTable(os.Stdout, columns, rows)
}

// maskConfig returns a copy of the config with sensitive fields masked.
func maskConfig(cfg *UserConfig) *UserConfig {
	masked := &UserConfig{
		CurrentProfile: cfg.CurrentProfile,
		Profiles:       make(map[string]Profile, len(cfg.Profiles)),
	}
	for name, p := range cfg.Profiles {
		masked.Profiles[name] = Profile{
			Host:   p.Host,
			APIKey: maskSecret(p.APIKey),
			Token:  maskSecret(p.Token),
			Output: p.Output,
		}
	}
	return masked
}

// maskSecret masks a sensitive string, showing first 4 and last 4 chars.
func maskSecret(s string) string {
	if s == "" {
		return ""
	}
	if len(s) <= 10 {
		return "****"
	}
	return s[:4] + "****" + s[len(s)-4:]
}

func newConfigSetProfileCmd() *cobra.Command {
	var (
		name   string
		host   string
		apiKey string
		token  string
		output string
	)

	cmd := &cobra.Command{
		Use:   "set-profile",
		Short: "Create or update a configuration profile",
		RunE: func(cmd *cobra.Command, _ []string) error {
			if name == "" {
				return fmt.Errorf("--name is required")
			}
			if cmd.Flags().Changed("output") {
				if err := validateOutputFormat(output); err != nil {
					return err
				}
			}

			cfg, err := LoadUserConfig()
			if err != nil {
				cfg = &UserConfig{
					CurrentProfile: "default",
					Profiles:       map[string]Profile{},
				}
			}

			p := cfg.Profiles[name]
			if cmd.Flags().Changed("host") {
				p.Host = host
			}
			if cmd.Flags().Changed("api-key") {
				p.APIKey = apiKey
			}
			if cmd.Flags().Changed("token") {
				p.Token = token
			}
			if cmd.Flags().Changed("output") {
				p.Output = output
			}
			cfg.Profiles[name] = p

			if err := SaveUserConfig(cfg); err != nil {
				return err
			}
			if getOutputFormat(cmd) == "json" {
				return gen.PrintJSON(os.Stdout, map[string]string{
					"status":  "ok",
					"profile": name,
					"path":    ConfigPath(),
				})
			}
			_, _ = fmt.Fprintf(os.Stdout, "Profile %q saved to %s\n", name, ConfigPath())
			return nil
		},
	}

	cmd.Flags().StringVar(&name, "name", "", "Profile name (required)")
	cmd.Flags().StringVar(&host, "host", "", "API host URL")
	cmd.Flags().StringVar(&apiKey, "api-key", "", "API key")
	cmd.Flags().StringVar(&token, "token", "", "JWT token")
	cmd.Flags().StringVar(&output, "output", "", "Default output format")
	_ = cmd.MarkFlagRequired("name")

	return cmd
}

func newConfigUseProfileCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "use-profile <name>",
		Short: "Set the active configuration profile",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg, err := LoadUserConfig()
			if err != nil {
				return fmt.Errorf("no config found: %w", err)
			}
			name := args[0]
			if _, ok := cfg.Profiles[name]; !ok {
				return fmt.Errorf("profile %q not found", name)
			}
			cfg.CurrentProfile = name
			if err := SaveUserConfig(cfg); err != nil {
				return err
			}
			if getOutputFormat(cmd) == "json" {
				return gen.PrintJSON(os.Stdout, map[string]string{
					"status":         "ok",
					"active_profile": name,
				})
			}
			_, _ = fmt.Fprintf(os.Stdout, "Active profile set to %q\n", name)
			return nil
		},
	}
}
