package cli

import (
	"errors"
	"fmt"
	"os"

	"github.com/spf13/cobra"

	"github.com/Yacobolo/quackstack/pkg/cli/apiruntime"
)

var (
	version   = "dev"
	commit    = "none"
	branch    = "unknown"
	tag       = "unknown"
	buildTime = "unknown"
)

const (
	groupAuth      = "auth"
	groupLifecycle = "lifecycle"
	groupExplore   = "explore"
	groupServer    = "server"
	groupAPI       = "api"
	groupPlatform  = "platform"
)

// Execute runs the CLI.
func Execute() int {
	rootCmd := newRootCmd()
	if err := rootCmd.Execute(); err != nil {
		output := getOutputFormat(rootCmd)
		cliErr, _ := err.(*CLIError)
		if output == "json" {
			if cliErr != nil && cliErr.JSONPayload != nil {
				_ = apiruntime.PrintJSON(os.Stdout, cliErr.JSONPayload)
			} else {
				errObj := map[string]interface{}{
					"error": err.Error(),
				}
				var apiErr *apiruntime.APIError
				if errors.As(err, &apiErr) {
					errObj["http_status"] = apiErr.HTTPStatus
					errObj["code"] = apiErr.Code
				}
				_ = apiruntime.PrintJSON(os.Stdout, errObj)
			}
		} else if err.Error() != "" {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		}
		return exitCodeForError(err)
	}
	return 0
}

func newRootCmd() *cobra.Command {
	var (
		host                     string
		apiKey                   string
		token                    string
		output                   string
		profile                  string
		quiet                    bool
		debug                    bool
		traceHTTP                bool
		logFormat                string
		logFile                  string
		quackAccessExtensionPath string

		apiKeyPriority int
		tokenPriority  int
	)

	rootCmd := &cobra.Command{
		Use:           "quack",
		Short:         "QuackStack CLI",
		Long:          "Command-line interface for the QuackStack API.",
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
				if v := os.Getenv("QUACK_HOST"); v != "" {
					host = v
				} else if p.Host != "" {
					host = p.Host
				}
			}
			if !cmd.Flags().Changed("api-key") {
				if v := os.Getenv("QUACK_API_KEY"); v != "" {
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
				if v := os.Getenv("QUACK_TOKEN"); v != "" {
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
				if v := os.Getenv("QUACK_OUTPUT"); v != "" {
					output = normalizeOutputFormat(v)
				} else if p.Output != "" {
					output = normalizeOutputFormat(p.Output)
				}
			}
			if !cmd.Flags().Changed("quack-access-extension-path") {
				if v := os.Getenv("QUACK_ACCESS_EXTENSION_PATH"); v != "" {
					quackAccessExtensionPath = v
				} else if p.QuackAccessExtensionPath != "" {
					quackAccessExtensionPath = p.QuackAccessExtensionPath
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
	rootCmd.PersistentFlags().StringVarP(&output, "output", "o", "text", "Output format (text, json)")
	rootCmd.PersistentFlags().StringVarP(&profile, "profile", "p", "", "Config profile to use")
	rootCmd.PersistentFlags().BoolVarP(&quiet, "quiet", "q", false, "Only output resource identifiers")
	rootCmd.PersistentFlags().BoolVar(&debug, "debug", false, "Enable debug logging")
	rootCmd.PersistentFlags().BoolVar(&traceHTTP, "trace-http", false, "Log HTTP requests and responses")
	rootCmd.PersistentFlags().StringVar(&logFormat, "log-format", "text", "Log format (text, json)")
	rootCmd.PersistentFlags().StringVar(&logFile, "log-file", "", "Write logs to a file instead of stderr")
	rootCmd.PersistentFlags().StringVar(&quackAccessExtensionPath, "quack-access-extension-path", "", "Path to quack_access.duckdb_extension for local BYOC execution")

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
		if generatedCommandsErr != nil {
			return fmt.Errorf("build generated commands: %w", generatedCommandsErr)
		}
		// Propagate resolved output to pflag so getOutputFormat() sees config values.
		if output != "" {
			_ = cmd.Root().PersistentFlags().Set("output", output)
		}
		// Validate output format
		if err := validateOutputFormat(output); err != nil {
			return err
		}
		if logFormat != "text" && logFormat != "json" {
			return fmt.Errorf("unsupported log format %q: use 'text' or 'json'", logFormat)
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
		client.Debug = debug
		client.TraceHTTP = traceHTTP
		client.LogFormat = logFormat
		client.LogFile = logFile
		return nil
	}

	rootCmd.AddGroup(
		&cobra.Group{ID: groupAuth, Title: "Authentication"},
		&cobra.Group{ID: groupLifecycle, Title: "Platform Lifecycle"},
		&cobra.Group{ID: groupExplore, Title: "Exploration"},
		&cobra.Group{ID: groupServer, Title: "Server/Admin"},
		&cobra.Group{ID: groupAPI, Title: "API And Tooling"},
		&cobra.Group{ID: groupPlatform, Title: "Platform Resources"},
	)

	// Add runtime-generated API commands
	addRuntimeGeneratedCommands(rootCmd, client)

	// Add hand-written commands
	addGroupedCommand(rootCmd, newVersionCmd(), groupAPI)
	addGroupedCommand(rootCmd, newAuthCmd(client), groupAuth)
	addGroupedCommand(rootCmd, newProjectCmd(), groupLifecycle)
	addGroupedCommand(rootCmd, newServerCmd(), groupServer)

	// Declarative configuration commands
	addGroupedCommand(rootCmd, newPlanCmd(client), groupLifecycle)
	addGroupedCommand(rootCmd, newApplyCmd(client), groupLifecycle)
	addGroupedCommand(rootCmd, newExportCmd(client), groupLifecycle)
	addGroupedCommand(rootCmd, newValidateCmd(client), groupLifecycle)
	addGroupedCommand(rootCmd, newSummaryCmd(), groupLifecycle)
	addGroupedCommand(rootCmd, newSchemaCmd(), groupLifecycle)
	addGroupedCommand(rootCmd, newAdoptCmd(client), groupLifecycle)

	// Agent discovery commands
	addGroupedCommand(rootCmd, newCommandsCmd(), groupExplore)
	addGroupedCommand(rootCmd, newAPICmd(client), groupAPI)
	addGroupedCommand(rootCmd, newDiscoverCmd(), groupExplore)
	addGroupedCommand(rootCmd, newDocsCmd(), groupExplore)
	addGroupedCommand(rootCmd, newFindCmd(client), groupExplore)
	addGroupedCommand(rootCmd, newDescribeCmd(client), groupExplore)

	// Shell completions
	addGroupedCommand(rootCmd, newCompletionCmd(), groupAPI)

	return rootCmd
}

func addGroupedCommand(rootCmd *cobra.Command, cmd *cobra.Command, groupID string) {
	cmd.GroupID = groupID
	rootCmd.AddCommand(cmd)
}

func newCompletionCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "completion",
		Short: "Generate or install shell completion scripts",
	}
	cmd.AddCommand(newCompletionShellCmd("bash"))
	cmd.AddCommand(newCompletionShellCmd("zsh"))
	cmd.AddCommand(newCompletionShellCmd("fish"))
	cmd.AddCommand(newCompletionShellCmd("powershell"))
	cmd.AddCommand(newCompletionInstallCmd())
	cmd.AddCommand(newCompletionStatusCmd())
	cmd.AddCommand(newCompletionUninstallCmd())
	return cmd
}

func newCompletionShellCmd(shell string) *cobra.Command {
	return &cobra.Command{
		Use:   shell,
		Short: "Generate " + shell + " completion scripts",
		RunE: func(cmd *cobra.Command, _ []string) error {
			switch shell {
			case "bash":
				return cmd.Root().GenBashCompletion(os.Stdout)
			case "zsh":
				return cmd.Root().GenZshCompletion(os.Stdout)
			case "fish":
				return cmd.Root().GenFishCompletion(os.Stdout, true)
			case "powershell":
				return cmd.Root().GenPowerShellCompletionWithDesc(os.Stdout)
			default:
				return fmt.Errorf("unsupported shell: %s", shell)
			}
		},
	}
}
