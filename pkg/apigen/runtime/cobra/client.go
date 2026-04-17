package cobra

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"
)

// Client is the shared HTTP client used by the generated Cobra runtime.
type Client struct {
	BaseURL    string
	APIKey     string
	Token      string
	HTTPClient *http.Client
	Debug      bool
	TraceHTTP  bool
	LogFormat  string
	LogFile    string
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

// NewClient constructs a runtime HTTP client with sane defaults.
func NewClient(baseURL, apiKey, token string) *Client {
	return &Client{
		BaseURL:    strings.TrimRight(baseURL, "/"),
		APIKey:     apiKey,
		Token:      token,
		HTTPClient: &http.Client{Timeout: 30 * time.Second},
	}
}

// Do issues an authenticated HTTP request against the generated API surface.
func (c *Client) Do(method, path string, query url.Values, body any) (*http.Response, error) {
	baseURL := strings.TrimRight(c.BaseURL, "/")
	if baseURL == "" {
		baseURL = "http://localhost:8080"
	}

	reqURL := baseURL + path
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

	req, err := http.NewRequestWithContext(context.Background(), method, reqURL, bodyReader)
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
	c.logRequest(req, resp, body)
	return resp, nil
}

func (c *Client) logRequest(req *http.Request, resp *http.Response, body any) {
	if !c.Debug && !c.TraceHTTP {
		return
	}

	writer := io.Writer(os.Stderr)
	if strings.TrimSpace(c.LogFile) != "" {
		f, err := os.OpenFile(c.LogFile, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
		if err == nil {
			writer = f
			defer func() {
				_ = f.Close()
			}()
		}
	}

	if strings.EqualFold(strings.TrimSpace(c.LogFormat), "json") {
		entry := map[string]any{
			"method":      req.Method,
			"url":         req.URL.String(),
			"status_code": resp.StatusCode,
		}
		if c.TraceHTTP && body != nil {
			entry["request_body"] = body
		}
		_ = json.NewEncoder(writer).Encode(entry)
		return
	}

	_, _ = fmt.Fprintf(writer, "[quack] %s %s -> %d\n", req.Method, req.URL.String(), resp.StatusCode)
	if c.TraceHTTP && body != nil {
		payload, _ := json.Marshal(body)
		if len(payload) > 0 {
			_, _ = fmt.Fprintf(writer, "[quack] body %s\n", string(payload))
		}
	}
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
