package cli

import (
	"fmt"
	"net/url"
	"strings"
)

func validateHostURL(host string) error {
	host = strings.TrimSpace(host)
	if host == "" {
		return fmt.Errorf("invalid host %q: host URL cannot be empty", host)
	}

	u, err := url.Parse(host)
	if err != nil {
		return fmt.Errorf("invalid host %q: %w", host, err)
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return fmt.Errorf("invalid host %q: scheme must be http or https", host)
	}
	if u.Host == "" {
		return fmt.Errorf("invalid host %q: missing host", host)
	}
	if u.Path != "" && u.Path != "/" {
		return fmt.Errorf("invalid host %q: host must not include a path", host)
	}
	if u.RawQuery != "" || u.Fragment != "" {
		return fmt.Errorf("invalid host %q: host must not include query or fragment", host)
	}
	return nil
}
