package cli

import (
	"os"
	"strings"

	"github.com/spf13/cobra"

	"github.com/Yacobolo/quackstack/pkg/cli/apiruntime"
)

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

func maskSecret(s string) string {
	if s == "" {
		return ""
	}
	if len(s) <= 10 {
		return "****"
	}
	return s[:4] + "****" + s[len(s)-4:]
}

func printConfigTable(_ *cobra.Command, cfg *UserConfig) error {
	columns := []string{"profile", "active", "host", "auth", "default_output"}
	rows := make([][]string, 0, len(cfg.Profiles))

	for name, p := range cfg.Profiles {
		auth := ""
		switch {
		case strings.TrimSpace(p.Token) != "":
			auth = "token"
		case strings.TrimSpace(p.APIKey) != "":
			auth = "api-key"
		}

		active := ""
		if cfg.CurrentProfile == name {
			active = "yes"
		}

		rows = append(rows, []string{name, active, p.Host, auth, p.Output})
	}

	if len(rows) == 0 {
		rows = append(rows, []string{"", "", "", "", ""})
	}

	apiruntime.PrintTable(os.Stdout, columns, rows)
	return nil
}
