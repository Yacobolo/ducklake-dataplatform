package cli

import (
	"fmt"
	"net/url"

	"github.com/spf13/cobra"
)

// getOutputFormat returns the effective output format from the root command's persistent flags.
func getOutputFormat(cmd *cobra.Command) string {
	v, _ := cmd.Root().PersistentFlags().GetString("output")
	return v
}

func validateOutputFormat(output string) error {
	if output != "" && output != "table" && output != "json" {
		return fmt.Errorf("unsupported output format %q: use 'table' or 'json'", output)
	}
	return nil
}

func validateHostURL(host string) error {
	if host == "" {
		return nil
	}

	parsed, err := url.Parse(host)
	if err != nil {
		return fmt.Errorf("invalid host %q: %w", host, err)
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return fmt.Errorf("invalid host %q: scheme must be http or https", host)
	}
	if parsed.Host == "" {
		return fmt.Errorf("invalid host %q: host is required", host)
	}
	if parsed.RawQuery != "" || parsed.Fragment != "" {
		return fmt.Errorf("invalid host %q: query string and fragment are not allowed", host)
	}

	return nil
}
