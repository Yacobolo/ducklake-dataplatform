package cli

import (
	"fmt"
	"net/url"
	"os"
	"sort"
	"strings"

	"github.com/spf13/cobra"
	"gopkg.in/yaml.v3"

	"github.com/Yacobolo/quackstack/pkg/cli/apiruntime"
)

type resolvedAuthContext struct {
	Config       *UserConfig
	ProfileName  string
	Profile      Profile
	Token        string
	APIKey       string
	Host         string
	AuthType     string
	HostSource   string
	AuthSource   string
	Output       string
	OutputSource string
}

func newAuthProfilesCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "profiles",
		Short: "Manage CLI auth profiles",
	}

	cmd.AddCommand(newAuthProfilesListCmd())
	cmd.AddCommand(newAuthProfilesShowCmd())
	cmd.AddCommand(newAuthProfilesSaveCmd())
	cmd.AddCommand(newAuthProfilesUseCmd())
	cmd.AddCommand(newAuthProfilesDeleteCmd())
	return cmd
}

func newAuthProfilesListCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "list",
		Short: "List configured auth profiles",
		RunE: func(cmd *cobra.Command, _ []string) error {
			cfg, err := LoadUserConfig()
			if err != nil {
				cfg = &UserConfig{Profiles: map[string]Profile{}}
			}
			if getOutputFormat(cmd) == "json" {
				return apiruntime.PrintJSON(os.Stdout, cfg)
			}
			return printConfigTable(cmd, cfg)
		},
	}
}

func newAuthProfilesShowCmd() *cobra.Command {
	var reveal bool

	cmd := &cobra.Command{
		Use:   "show [name]",
		Short: "Show one configured auth profile",
		Args:  cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg, err := LoadUserConfig()
			if err != nil {
				return err
			}
			name := cfg.CurrentProfile
			if len(args) == 1 {
				name = args[0]
			}
			if name == "" {
				name = "default"
			}
			profile, ok := cfg.Profiles[name]
			if !ok {
				return fmt.Errorf("profile %q not found", name)
			}

			payload := map[string]any{
				"name":           name,
				"active":         cfg.CurrentProfile == name,
				"current_profile": cfg.CurrentProfile,
				"profile":        profile,
			}
			if !reveal {
				payload["profile"] = maskConfig(&UserConfig{
					CurrentProfile: name,
					Profiles:       map[string]Profile{name: profile},
				}).Profiles[name]
			}
			if getOutputFormat(cmd) == "json" {
				return apiruntime.PrintJSON(os.Stdout, payload)
			}
			if !reveal {
				profile = maskConfig(&UserConfig{CurrentProfile: name, Profiles: map[string]Profile{name: profile}}).Profiles[name]
			}
			data, err := yaml.Marshal(payload)
			if err != nil {
				return fmt.Errorf("marshal profile: %w", err)
			}
			_, _ = fmt.Fprint(os.Stdout, string(data))
			return nil
		},
	}

	cmd.Flags().BoolVar(&reveal, "reveal", false, "Show sensitive values unmasked")
	return cmd
}

func newAuthProfilesSaveCmd() *cobra.Command {
	var (
		name          string
		host          string
		apiKey        string
		token         string
		defaultOutput string
	)

	cmd := &cobra.Command{
		Use:   "save",
		Short: "Create or update an auth profile",
		RunE: func(cmd *cobra.Command, _ []string) error {
			cfg, err := LoadUserConfig()
			if err != nil {
				cfg = &UserConfig{CurrentProfile: "default", Profiles: map[string]Profile{}}
			}
			if strings.TrimSpace(name) == "" {
				name = cfg.CurrentProfile
			}
			if strings.TrimSpace(name) == "" {
				name = "default"
			}
			if host != "" {
				if err := validateHostURL(host); err != nil {
					return err
				}
			}
			if defaultOutput != "" {
				defaultOutput = normalizeOutputFormat(defaultOutput)
				if err := validateOutputFormat(defaultOutput); err != nil {
					return err
				}
			}

			profile := cfg.Profiles[name]
			if cmd.Flags().Changed("host") {
				profile.Host = host
			}
			if cmd.Flags().Changed("api-key") {
				profile.APIKey = apiKey
			}
			if cmd.Flags().Changed("token") {
				profile.Token = token
			}
			if cmd.Flags().Changed("default-output") {
				profile.Output = defaultOutput
			}
			cfg.Profiles[name] = profile
			if cfg.CurrentProfile == "" {
				cfg.CurrentProfile = name
			}
			if err := SaveUserConfig(cfg); err != nil {
				return err
			}
			if getOutputFormat(cmd) == "json" {
				return apiruntime.PrintJSON(os.Stdout, map[string]string{
					"status":  "ok",
					"profile": name,
					"path":    ConfigPath(),
				})
			}
			_, _ = fmt.Fprintf(os.Stdout, "Profile %q saved to %s\n", name, ConfigPath())
			return nil
		},
	}

	cmd.Flags().StringVar(&name, "name", "", "Profile name (defaults to the active profile)")
	cmd.Flags().StringVar(&host, "host", "", "API host URL")
	cmd.Flags().StringVar(&apiKey, "api-key", "", "API key")
	cmd.Flags().StringVar(&token, "token", "", "JWT token")
	cmd.Flags().StringVar(&defaultOutput, "default-output", "", "Default output format for the saved profile")
	return cmd
}

func newAuthProfilesUseCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "use <name>",
		Short: "Set the active auth profile",
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
				return apiruntime.PrintJSON(os.Stdout, map[string]string{
					"status":         "ok",
					"active_profile": name,
				})
			}
			_, _ = fmt.Fprintf(os.Stdout, "Active profile set to %q\n", name)
			return nil
		},
	}
}

func newAuthProfilesDeleteCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "delete <name>",
		Short: "Delete an auth profile",
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
			delete(cfg.Profiles, name)
			if cfg.CurrentProfile == name {
				cfg.CurrentProfile = ""
				names := make([]string, 0, len(cfg.Profiles))
				for profileName := range cfg.Profiles {
					names = append(names, profileName)
				}
				sort.Strings(names)
				if len(names) > 0 {
					cfg.CurrentProfile = names[0]
				}
			}
			if err := SaveUserConfig(cfg); err != nil {
				return err
			}
			if getOutputFormat(cmd) == "json" {
				return apiruntime.PrintJSON(os.Stdout, map[string]string{"status": "ok", "deleted_profile": name})
			}
			_, _ = fmt.Fprintf(os.Stdout, "Deleted profile %q\n", name)
			return nil
		},
	}
}

func newAuthDescribeCmd(client *apiruntime.Client) *cobra.Command {
	return &cobra.Command{
		Use:   "describe",
		Short: "Show effective auth settings and their sources",
		RunE: func(cmd *cobra.Command, _ []string) error {
			ctx, err := resolveAuthContext(cmd)
			if err != nil {
				return err
			}

			payload := map[string]any{
				"profile":       ctx.ProfileName,
				"host":          ctx.Host,
				"host_source":   ctx.HostSource,
				"auth_type":     ctx.AuthType,
				"auth_source":   ctx.AuthSource,
				"output":        ctx.Output,
				"output_source": ctx.OutputSource,
			}

			authenticated := false
			if ctx.AuthType != "none" {
				if err := probeAuth(client); err == nil {
					authenticated = true
				}
			}
			payload["authenticated"] = authenticated

			if getOutputFormat(cmd) == "json" {
				return apiruntime.PrintJSON(os.Stdout, payload)
			}
			apiruntime.PrintDetail(os.Stdout, payload)
			return nil
		},
	}
}

func newAuthEnvCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "env",
		Short: "Print environment variables for the active auth context",
		RunE: func(cmd *cobra.Command, _ []string) error {
			ctx, err := resolveAuthContext(cmd)
			if err != nil {
				return err
			}
			env := map[string]string{
				"QUACK_HOST":   ctx.Host,
				"QUACK_OUTPUT": ctx.Output,
			}
			switch ctx.AuthType {
			case "token":
				env["QUACK_TOKEN"] = ctx.Token
			case "api-key":
				env["QUACK_API_KEY"] = ctx.APIKey
			}
			if getOutputFormat(cmd) == "json" {
				return apiruntime.PrintJSON(os.Stdout, env)
			}
			keys := make([]string, 0, len(env))
			for key := range env {
				keys = append(keys, key)
			}
			sort.Strings(keys)
			for _, key := range keys {
				_, _ = fmt.Fprintf(os.Stdout, "export %s=%q\n", key, env[key])
			}
			return nil
		},
	}
}

func resolveAuthContext(cmd *cobra.Command) (*resolvedAuthContext, error) {
	cfg, err := LoadUserConfig()
	if err != nil {
		cfg = &UserConfig{CurrentProfile: "default", Profiles: map[string]Profile{}}
	}

	root := cmd.Root()
	profileName := cfg.CurrentProfile
	if profileFlag := root.PersistentFlags().Lookup("profile"); profileFlag != nil {
		if value, _ := root.PersistentFlags().GetString("profile"); strings.TrimSpace(value) != "" {
			profileName = value
		}
	}
	if profileName == "" {
		profileName = "default"
	}

	profile := cfg.Profiles[profileName]
	host := "http://localhost:8080"
	hostSource := "default"
	switch {
	case root.PersistentFlags().Changed("host"):
		host, _ = root.PersistentFlags().GetString("host")
		hostSource = "flag"
	case os.Getenv("QUACK_HOST") != "":
		host = os.Getenv("QUACK_HOST")
		hostSource = "env"
	case strings.TrimSpace(profile.Host) != "":
		host = profile.Host
		hostSource = "profile"
	}

	authType := "none"
	authSource := "none"
	switch {
	case root.PersistentFlags().Changed("token"):
		authType = "token"
		authSource = "flag"
		profile.Token, _ = root.PersistentFlags().GetString("token")
	case os.Getenv("QUACK_TOKEN") != "":
		authType = "token"
		authSource = "env"
		profile.Token = os.Getenv("QUACK_TOKEN")
	case root.PersistentFlags().Changed("api-key"):
		authType = "api-key"
		authSource = "flag"
		profile.APIKey, _ = root.PersistentFlags().GetString("api-key")
	case os.Getenv("QUACK_API_KEY") != "":
		authType = "api-key"
		authSource = "env"
		profile.APIKey = os.Getenv("QUACK_API_KEY")
	case strings.TrimSpace(profile.Token) != "":
		authType = "token"
		authSource = "profile"
	case strings.TrimSpace(profile.APIKey) != "":
		authType = "api-key"
		authSource = "profile"
	}

	output := "text"
	outputSource := "default"
	switch {
	case root.PersistentFlags().Changed("output"):
		output, _ = root.PersistentFlags().GetString("output")
		outputSource = "flag"
	case os.Getenv("QUACK_OUTPUT") != "":
		output = os.Getenv("QUACK_OUTPUT")
		outputSource = "env"
	case strings.TrimSpace(profile.Output) != "":
		output = profile.Output
		outputSource = "profile"
	}
	if output != "json" {
		output = "text"
	}

	return &resolvedAuthContext{
		Config:       cfg,
		ProfileName:  profileName,
		Profile:      profile,
		Token:        profile.Token,
		APIKey:       profile.APIKey,
		Host:         host,
		HostSource:   hostSource,
		AuthType:     authType,
		AuthSource:   authSource,
		Output:       output,
		OutputSource: outputSource,
	}, nil
}

func probeAuth(client *apiruntime.Client) error {
	resp, err := client.Do("GET", "/catalogs", url.Values{"max_results": []string{"1"}}, nil)
	if err != nil {
		return err
	}
	return apiruntime.CheckError(resp)
}
