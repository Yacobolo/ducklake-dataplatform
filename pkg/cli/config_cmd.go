package cli

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"

	"gopkg.in/yaml.v3"
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
	return &cobra.Command{
		Use:   "show",
		Short: "Display current configuration",
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg, err := LoadUserConfig()
			if err != nil {
				fmt.Fprintf(os.Stderr, "No configuration found at %s\n", ConfigPath())
				return nil
			}
			data, err := yaml.Marshal(cfg)
			if err != nil {
				return fmt.Errorf("marshal config: %w", err)
			}
			fmt.Fprint(os.Stdout, string(data))
			return nil
		},
	}
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
		RunE: func(cmd *cobra.Command, args []string) error {
			if name == "" {
				return fmt.Errorf("--name is required")
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
			fmt.Fprintf(os.Stdout, "Profile %q saved to %s\n", name, ConfigPath())
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
			fmt.Fprintf(os.Stdout, "Active profile set to %q\n", name)
			return nil
		},
	}
}
