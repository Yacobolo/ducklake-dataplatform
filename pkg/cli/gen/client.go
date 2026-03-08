package gen

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// Client is the shared HTTP client used by generated and hand-written CLI commands.
type Client struct {
	BaseURL    string
	APIKey     string
	Token      string
	HTTPClient *http.Client
}

// APIError is a structured API failure returned by CheckError.
type APIError struct {
	HTTPStatus int
	Code       int    `json:"code"`
	Message    string `json:"message"`
}

func (e *APIError) Error() string {
	return fmt.Sprintf("API error (HTTP %d): %s", e.HTTPStatus, e.Message)
}

// NewClient constructs a CLI HTTP client with sane defaults.
func NewClient(baseURL, apiKey, token string) *Client {
	return &Client{
		BaseURL:    strings.TrimRight(baseURL, "/"),
		APIKey:     apiKey,
		Token:      token,
		HTTPClient: &http.Client{Timeout: 30 * time.Second},
	}
}

// Do issues an authenticated HTTP request against the v1 API surface.
func (c *Client) Do(method, path string, query url.Values, body any) (*http.Response, error) {
	baseURL := strings.TrimRight(c.BaseURL, "/")
	if baseURL == "" {
		baseURL = "http://localhost:8080"
	}

	reqURL := baseURL + "/v1" + path
	if len(query) > 0 {
		reqURL += "?" + query.Encode()
	}

	var bodyReader io.Reader
	if body != nil {
		payload, err := json.Marshal(body)
		if err != nil {
			return nil, fmt.Errorf("marshal request body: %w", err)
		}
		bodyReader = bytes.NewReader(payload)
	}

	req, err := http.NewRequest(method, reqURL, bodyReader)
	if err != nil {
		return nil, fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("Accept", "application/json")
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if c.Token != "" {
		req.Header.Set("Authorization", "Bearer "+c.Token)
	} else if c.APIKey != "" {
		req.Header.Set("X-API-Key", c.APIKey)
	}

	resp, err := c.HTTPClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("execute request: %w", err)
	}
	return resp, nil
}

// CheckError returns a structured error for non-2xx responses.
func CheckError(resp *http.Response) error {
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return nil
	}

	body, err := ReadBody(resp)
	if err != nil {
		return fmt.Errorf("read error response: %w", err)
	}

	apiErr := &APIError{HTTPStatus: resp.StatusCode}
	if len(body) > 0 {
		if err := json.Unmarshal(body, apiErr); err == nil && strings.TrimSpace(apiErr.Message) != "" {
			if apiErr.Code == 0 {
				apiErr.Code = resp.StatusCode
			}
			return apiErr
		}
		apiErr.Message = string(body)
	}

	return apiErr
}

// ReadBody reads and closes an HTTP response body.
func ReadBody(resp *http.Response) ([]byte, error) {
	if resp == nil || resp.Body == nil {
		return nil, nil
	}
	defer resp.Body.Close() //nolint:errcheck
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read response body: %w", err)
	}
	return body, nil
}
