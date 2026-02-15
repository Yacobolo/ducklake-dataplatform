package cli

import (
	"errors"
	"fmt"
	"os"

	"github.com/spf13/cobra"

	"duck-demo/pkg/cli/gen"
)

var (
	version = "dev"
	commit  = "none"
)

// Execute runs the CLI.
func Execute() int {
	rootCmd := newRootCmd()
	if err := rootCmd.Execute(); err != nil {
		output, _ := rootCmd.PersistentFlags().GetString("output")
		if output == "json" {
			errObj := map[string]interface{}{
				"error": err.Error(),
			}
			var apiErr *gen.APIError
			if errors.As(err, &apiErr) {
				errObj["http_status"] = apiErr.HTTPStatus
				errObj["code"] = apiErr.Code
			}
			_ = gen.PrintJSON(os.Stdout, errObj)
		} else {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		}
		return 1
	}
	return 0
}

func newRootCmd() *cobra.Command {
	var (
		host    string
		apiKey  string
		token   string
		output  string
		profile string
		quiet   bool
	)

	rootCmd := &cobra.Command{
		Use:           "duck",
		Short:         "DuckDB Data Platform CLI",
		Long:          "Command-line interface for the DuckDB Data Platform API.",
		SilenceUsage:  true,
		SilenceErrors: true,
		PersistentPreRunE: func(cmd *cobra.Command, _ []string) error {
			// Load config from profile if flags/env not set
			cfg, err := LoadUserConfig()
			if err != nil {
				// Config file is optional
				cfg = &UserConfig{
					CurrentProfile: "default",
					Profiles:       map[string]Profile{},
				}
			}

			p := cfg.ActiveProfile(profile)

			// Apply precedence: flag > env > profile > default
			if !cmd.Flags().Changed("host") {
				if v := os.Getenv("DUCK_HOST"); v != "" {
					host = v
				} else if p.Host != "" {
					host = p.Host
				}
			}
			if !cmd.Flags().Changed("api-key") {
				if v := os.Getenv("DUCK_API_KEY"); v != "" {
					apiKey = v
				} else if p.APIKey != "" {
					apiKey = p.APIKey
				}
			}
			if !cmd.Flags().Changed("token") {
				if v := os.Getenv("DUCK_TOKEN"); v != "" {
					token = v
				} else if p.Token != "" {
					token = p.Token
				}
			}
			if !cmd.Flags().Changed("output") {
				if v := os.Getenv("DUCK_OUTPUT"); v != "" {
					output = v
				} else if p.Output != "" {
					output = p.Output
				}
			}

			return nil
		},
	}

	rootCmd.PersistentFlags().StringVar(&host, "host", "http://localhost:8080", "API host URL")
	rootCmd.PersistentFlags().StringVar(&apiKey, "api-key", "", "API key for authentication")
	rootCmd.PersistentFlags().StringVar(&token, "token", "", "JWT token for authentication")
	rootCmd.PersistentFlags().StringVarP(&output, "output", "o", "table", "Output format (table, json)")
	rootCmd.PersistentFlags().StringVarP(&profile, "profile", "p", "", "Config profile to use")
	rootCmd.PersistentFlags().BoolVarP(&quiet, "quiet", "q", false, "Only output resource identifiers")

	// Create client using a lazy initializer
	client := gen.NewClient(host, apiKey, token)

	// Wire PersistentPreRun to update client after config resolution
	originalPreRun := rootCmd.PersistentPreRunE
	rootCmd.PersistentPreRunE = func(cmd *cobra.Command, args []string) error {
		if originalPreRun != nil {
			if err := originalPreRun(cmd, args); err != nil {
				return err
			}
		}
		// Validate output format
		if output != "" && output != "table" && output != "json" {
			return fmt.Errorf("unsupported output format %q: use 'table' or 'json'", output)
		}
		// Update client with resolved values
		client.BaseURL = host
		client.APIKey = apiKey
		client.Token = token
		return nil
	}

	// Add generated commands
	gen.AddGeneratedCommands(rootCmd, client)

	// Add hand-written commands
	rootCmd.AddCommand(newVersionCmd())
	rootCmd.AddCommand(newConfigCmd())
	rootCmd.AddCommand(newAuthCmd())

	// Declarative configuration commands
	rootCmd.AddCommand(newPlanCmd(client))
	rootCmd.AddCommand(newApplyCmd(client))
	rootCmd.AddCommand(newExportCmd(client))
	rootCmd.AddCommand(newValidateCmd(client))

	// Agent discovery commands
	rootCmd.AddCommand(newCommandsCmd())
	rootCmd.AddCommand(newAPICmd())
	rootCmd.AddCommand(newFindCmd(client))
	rootCmd.AddCommand(newDescribeCmd(client))

	// Shell completions
	rootCmd.AddCommand(newCompletionCmd())

	return rootCmd
}

func newCompletionCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "completion [bash|zsh|fish|powershell]",
		Short: "Generate shell completion scripts",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			switch args[0] {
			case "bash":
				return cmd.Root().GenBashCompletion(os.Stdout)
			case "zsh":
				return cmd.Root().GenZshCompletion(os.Stdout)
			case "fish":
				return cmd.Root().GenFishCompletion(os.Stdout, true)
			case "powershell":
				return cmd.Root().GenPowerShellCompletionWithDesc(os.Stdout)
			default:
				return fmt.Errorf("unsupported shell: %s", args[0])
			}
		},
	}
	return cmd
}
