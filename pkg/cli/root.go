package cli

import (
	"errors"
	"fmt"
	"os"

	"github.com/spf13/cobra"

	"duck-demo/pkg/cli/apiruntime"
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
			var apiErr *apiruntime.APIError
			if errors.As(err, &apiErr) {
				errObj["http_status"] = apiErr.HTTPStatus
				errObj["code"] = apiErr.Code
			}
			_ = apiruntime.PrintJSON(os.Stdout, errObj)
		} else {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		}
		return 1
	}
	return 0
}

func newRootCmd() *cobra.Command {
	var (
		host                    string
		apiKey                  string
		token                   string
		output                  string
		profile                 string
		quiet                   bool
		duckAccessExtensionPath string

		apiKeyPriority int
		tokenPriority  int
	)

	rootCmd := &cobra.Command{
		Use:           "duck",
		Short:         "DuckDB Data Platform CLI",
		Long:          "Command-line interface for the DuckDB Data Platform API.",
		SilenceUsage:  true,
		SilenceErrors: true,
		PersistentPreRunE: func(cmd *cobra.Command, _ []string) error {
			apiKeyPriority = 0
			tokenPriority = 0

			// Load config from profile if flags/env not set
			cfg, err := LoadUserConfig()
			if err != nil {
				// Config file is optional
				cfg = &UserConfig{
					CurrentProfile: "default",
					Profiles:       map[string]Profile{},
				}
			}

			p, err := cfg.ActiveProfile(profile)
			if err != nil {
				return err
			}

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
					apiKeyPriority = 30
				} else if p.APIKey != "" {
					apiKey = p.APIKey
					apiKeyPriority = 10
				}
			} else if apiKey != "" {
				apiKeyPriority = 50
			}
			if !cmd.Flags().Changed("token") {
				if v := os.Getenv("DUCK_TOKEN"); v != "" {
					token = v
					tokenPriority = 40
				} else if p.Token != "" {
					token = p.Token
					tokenPriority = 20
				}
			} else if token != "" {
				tokenPriority = 60
			}
			if !cmd.Flags().Changed("output") {
				if v := os.Getenv("DUCK_OUTPUT"); v != "" {
					output = v
				} else if p.Output != "" {
					output = p.Output
				}
			}
			if !cmd.Flags().Changed("duck-access-extension-path") {
				if v := os.Getenv("DUCK_ACCESS_EXTENSION_PATH"); v != "" {
					duckAccessExtensionPath = v
				} else if p.DuckAccessExtensionPath != "" {
					duckAccessExtensionPath = p.DuckAccessExtensionPath
				}
			}

			if apiKey != "" && token != "" {
				if tokenPriority > apiKeyPriority {
					apiKey = ""
				} else {
					token = ""
				}
			}

			return nil
		},
	}

	rootCmd.PersistentFlags().StringVar(&host, "host", "http://localhost:8080", "API host URL")
	rootCmd.PersistentFlags().StringVar(&apiKey, "api-key", "", "API key for authentication")
	rootCmd.PersistentFlags().StringVar(&token, "token", "", "JWT token for authentication")
	rootCmd.PersistentFlags().StringVarP(&output, "output", "o", "table", "Output format (table, json, csv)")
	rootCmd.PersistentFlags().StringVarP(&profile, "profile", "p", "", "Config profile to use")
	rootCmd.PersistentFlags().BoolVarP(&quiet, "quiet", "q", false, "Only output resource identifiers")
	rootCmd.PersistentFlags().StringVar(&duckAccessExtensionPath, "duck-access-extension-path", "", "Path to duck_access.duckdb_extension for local BYOC execution")

	// Create client using a lazy initializer
	client := apiruntime.NewClient(host, apiKey, token)

	// Wire PersistentPreRun to update client after config resolution
	originalPreRun := rootCmd.PersistentPreRunE
	rootCmd.PersistentPreRunE = func(cmd *cobra.Command, args []string) error {
		if originalPreRun != nil {
			if err := originalPreRun(cmd, args); err != nil {
				return err
			}
		}
		// Propagate resolved output to pflag so getOutputFormat() sees config values.
		if output != "" {
			_ = cmd.Root().PersistentFlags().Set("output", output)
		}
		// Validate output format
		if err := validateOutputFormat(output); err != nil {
			return err
		}
		if yesFlag := cmd.Flags().Lookup("yes"); yesFlag != nil {
			yes, _ := cmd.Flags().GetBool("yes")
			if !yes && !apiruntime.IsStdinTTY() {
				return fmt.Errorf("confirmation required but stdin is not a terminal; use --yes to skip")
			}
		}
		// Update client with resolved values
		client.BaseURL = host
		client.APIKey = apiKey
		client.Token = token
		return nil
	}

	// Add runtime-generated API commands
	addRuntimeGeneratedCommands(rootCmd, client)

	// Add hand-written commands
	rootCmd.AddCommand(newVersionCmd())
	rootCmd.AddCommand(newConfigCmd())
	rootCmd.AddCommand(newAuthCmd(client))
	rootCmd.AddCommand(newInitCmd(client))

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
