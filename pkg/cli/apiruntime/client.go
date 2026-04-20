// Package apiruntime provides compatibility wrappers over the reusable Cobra runtime.
package apiruntime

import (
	"net/http"
	"net/url"
	"strings"

	cobraruntime "github.com/Yacobolo/quackstack/pkg/apigen/runtime/cobra"
)

// Client is the shared HTTP client used by the CLI runtime.
type Client = cobraruntime.Client

// APIError is a structured API failure returned by CheckError.
type APIError = cobraruntime.APIError

const defaultAPIBasePath = "/v1"

// NewClient constructs a CLI HTTP client with sane defaults.
func NewClient(baseURL, apiKey, token string) *Client {
	return cobraruntime.NewClient(NormalizeBaseURL(baseURL), apiKey, token)
}

// NormalizeBaseURL applies Quack's default API base path when the host does not include one.
func NormalizeBaseURL(baseURL string) string {
	trimmed := strings.TrimSpace(baseURL)
	if trimmed == "" {
		return ""
	}

	parsed, err := url.Parse(trimmed)
	if err != nil {
		return strings.TrimRight(trimmed, "/")
	}

	switch strings.TrimSpace(parsed.Path) {
	case "", "/":
		parsed.Path = defaultAPIBasePath
	}
	return strings.TrimRight(parsed.String(), "/")
}

// CheckError returns a structured error for non-2xx responses.
func CheckError(resp *http.Response) error {
	return cobraruntime.CheckError(resp)
}

// ReadBody reads and closes an HTTP response body.
func ReadBody(resp *http.Response) ([]byte, error) {
	return cobraruntime.ReadBody(resp)
}
