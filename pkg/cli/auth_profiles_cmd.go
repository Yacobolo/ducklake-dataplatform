package cli

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/golang-jwt/jwt/v5"
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
	cmd.AddCommand(newAuthProfilesValidateCmd())
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
				return apiruntime.PrintJSON(os.Stdout, maskConfig(cfg))
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
				"name":            name,
				"active":          cfg.CurrentProfile == name,
				"current_profile": cfg.CurrentProfile,
				"profile":         profile,
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

type authProfileValidationResult struct {
	Name     string `json:"name"`
	Host     string `json:"host"`
	AuthType string `json:"auth_type"`
	Valid    bool   `json:"valid"`
	Error    string `json:"error,omitempty"`
}

func newAuthProfilesValidateCmd() *cobra.Command {
	var validateAll bool

	cmd := &cobra.Command{
		Use:   "validate [name]",
		Short: "Validate one or more configured auth profiles",
		Args:  cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg, err := LoadUserConfig()
			if err != nil {
				return err
			}

			names := selectedProfileNames(cfg, args, validateAll)
			results := make([]authProfileValidationResult, 0, len(names))
			for _, name := range names {
				profile, ok := cfg.Profiles[name]
				if !ok {
					return fmt.Errorf("profile %q not found", name)
				}
				results = append(results, validateProfile(name, profile))
			}

			if getOutputFormat(cmd) == "json" {
				return apiruntime.PrintJSON(os.Stdout, map[string]any{"profiles": results, "config_path": ConfigPath()})
			}

			rows := make([][]string, 0, len(results))
			for _, result := range results {
				status := "valid"
				if !result.Valid {
					status = "invalid"
				}
				rows = append(rows, []string{result.Name, result.Host, result.AuthType, status, result.Error})
			}
			apiruntime.PrintTable(os.Stdout, []string{"name", "host", "auth_type", "status", "error"}, rows)
			return nil
		},
	}

	cmd.Flags().BoolVar(&validateAll, "all", false, "Validate all saved profiles")
	return cmd
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

func newAuthWhoAmICmd(client *apiruntime.Client) *cobra.Command {
	return &cobra.Command{
		Use:   "whoami",
		Short: "Show the effective principal and session status",
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

			principal, principalSource, expiresAt := bestEffortTokenIdentity(ctx.Token)
			if principal != "" {
				payload["principal"] = principal
				payload["principal_source"] = principalSource
			}
			if expiresAt != nil {
				payload["token_expires_at"] = *expiresAt
			}

			if ctx.AuthType == "none" {
				payload["session_valid"] = false
			} else if err := probeAuth(client); err != nil {
				payload["session_valid"] = false
				payload["validation_error"] = err.Error()
			} else {
				payload["session_valid"] = true
				if payload["principal"] == nil {
					if matchedPrincipal, source, lookupErr := lookupPrincipalForAPIKey(client, ctx); lookupErr == nil && matchedPrincipal != "" {
						payload["principal"] = matchedPrincipal
						payload["principal_source"] = source
					}
				}
			}

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

func selectedProfileNames(cfg *UserConfig, args []string, validateAll bool) []string {
	if len(args) == 1 {
		return []string{args[0]}
	}
	if validateAll {
		names := make([]string, 0, len(cfg.Profiles))
		for name := range cfg.Profiles {
			names = append(names, name)
		}
		sort.Strings(names)
		return names
	}
	name := cfg.CurrentProfile
	if strings.TrimSpace(name) == "" {
		name = "default"
	}
	return []string{name}
}

func validateProfile(name string, profile Profile) authProfileValidationResult {
	host := strings.TrimSpace(profile.Host)
	if host == "" {
		host = "http://localhost:8080"
	}
	result := authProfileValidationResult{
		Name: name,
		Host: host,
	}

	switch {
	case strings.TrimSpace(profile.Token) != "":
		result.AuthType = "token"
	case strings.TrimSpace(profile.APIKey) != "":
		result.AuthType = "api-key"
	default:
		result.AuthType = "none"
		result.Error = "no auth configured"
		return result
	}

	if err := validateHostURL(host); err != nil {
		result.Error = err.Error()
		return result
	}

	client := apiruntime.NewClient(host, profile.APIKey, profile.Token)
	if err := probeAuth(client); err != nil {
		result.Error = err.Error()
		return result
	}

	result.Valid = true
	return result
}

func bestEffortTokenIdentity(token string) (string, string, *string) {
	trimmed := strings.TrimSpace(token)
	if trimmed == "" {
		return "", "", nil
	}

	claims := jwt.MapClaims{}
	parser := jwt.Parser{}
	if _, _, err := parser.ParseUnverified(trimmed, claims); err == nil {
		principal, source, expiresAt := extractJWTClaims(claims)
		if principal != "" || expiresAt != nil {
			return principal, source, expiresAt
		}
	}

	parts := strings.Split(trimmed, ".")
	if len(parts) != 3 {
		return "", "", nil
	}
	decoded, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return "", "", nil
	}

	var raw map[string]any
	if err := json.Unmarshal(decoded, &raw); err != nil {
		return "", "", nil
	}
	principal, _ := raw["sub"].(string)
	expiresAt := jwtExpiry(raw["exp"])
	if principal == "" && expiresAt == nil {
		return "", "", nil
	}
	return principal, "token-subject", expiresAt
}

func extractJWTClaims(claims jwt.MapClaims) (string, string, *string) {
	principal, _ := claims["sub"].(string)
	expiresAt := jwtExpiry(claims["exp"])
	if principal == "" && expiresAt == nil {
		return "", "", nil
	}
	return principal, "token-subject", expiresAt
}

func jwtExpiry(value any) *string {
	switch typed := value.(type) {
	case float64:
		ts := time.Unix(int64(typed), 0).UTC().Format(time.RFC3339)
		return &ts
	case json.Number:
		if parsed, err := typed.Int64(); err == nil {
			ts := time.Unix(parsed, 0).UTC().Format(time.RFC3339)
			return &ts
		}
	}
	return nil
}

func lookupPrincipalForAPIKey(client *apiruntime.Client, ctx *resolvedAuthContext) (string, string, error) {
	if ctx == nil || strings.TrimSpace(ctx.APIKey) == "" {
		return "", "", nil
	}

	resp, err := client.Do("GET", "/principals", url.Values{"max_results": []string{"1000"}}, nil)
	if err != nil {
		return "", "", err
	}
	if err := apiruntime.CheckError(resp); err != nil {
		return "", "", err
	}

	var principals listResponse
	if err := json.NewDecoder(resp.Body).Decode(&principals); err != nil {
		_ = resp.Body.Close()
		return "", "", err
	}
	_ = resp.Body.Close()

	var items []apiPrincipal
	if len(principals.Data) > 0 && string(principals.Data) != "null" {
		if err := json.Unmarshal(principals.Data, &items); err != nil {
			return "", "", err
		}
	}

	for _, principal := range items {
		query := url.Values{
			"max_results":  []string{"1000"},
			"principal_id": []string{principal.ID},
		}
		resp, err := client.Do("GET", "/api-keys", query, nil)
		if err != nil {
			return "", "", err
		}
		if err := apiruntime.CheckError(resp); err != nil {
			_ = resp.Body.Close()
			return "", "", err
		}

		var payload listResponse
		if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
			_ = resp.Body.Close()
			return "", "", err
		}
		_ = resp.Body.Close()

		var apiKeys []apiAPIKey
		if len(payload.Data) > 0 && string(payload.Data) != "null" {
			if err := json.Unmarshal(payload.Data, &apiKeys); err != nil {
				return "", "", err
			}
		}
		for _, apiKey := range apiKeys {
			if apiKey.KeyPrefix != "" && strings.HasPrefix(ctx.APIKey, apiKey.KeyPrefix) {
				return principal.Name, "api-key-prefix", nil
			}
		}
	}

	return "", "", nil
}

func probeAuth(client *apiruntime.Client) error {
	resp, err := client.Do("GET", "/catalogs", url.Values{"max_results": []string{"1"}}, nil)
	if err != nil {
		return err
	}
	return apiruntime.CheckError(resp)
}
