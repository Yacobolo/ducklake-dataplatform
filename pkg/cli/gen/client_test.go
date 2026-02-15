package gen

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// === NewClient ===

func TestNewClient_TrailingSlash(t *testing.T) {
	c := NewClient("http://localhost:8080/", "", "")
	assert.Equal(t, "http://localhost:8080", c.BaseURL)
}

func TestNewClient_NoTrailingSlash(t *testing.T) {
	c := NewClient("http://localhost:8080", "", "")
	assert.Equal(t, "http://localhost:8080", c.BaseURL)
}

func TestNewClient_SetsTimeout(t *testing.T) {
	c := NewClient("http://localhost:8080", "", "")
	require.NotNil(t, c.HTTPClient)
	assert.Equal(t, 30*time.Second, c.HTTPClient.Timeout)
}

func TestNewClient_StoresCredentials(t *testing.T) {
	c := NewClient("http://localhost:8080", "my-api-key", "my-token")
	assert.Equal(t, "my-api-key", c.APIKey)
	assert.Equal(t, "my-token", c.Token)
}

// === Client.Do ===

func TestDo_URLConstruction(t *testing.T) {
	var gotPath string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)

	c := NewClient(srv.URL, "", "")
	resp, err := c.Do(http.MethodGet, "/schemas", nil, nil)
	require.NoError(t, err)
	resp.Body.Close()

	assert.Equal(t, "/v1/schemas", gotPath)
}

func TestDo_QueryParams(t *testing.T) {
	var gotRawQuery string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotRawQuery = r.URL.RawQuery
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)

	c := NewClient(srv.URL, "", "")
	q := url.Values{}
	q.Set("limit", "10")
	q.Set("offset", "5")

	resp, err := c.Do(http.MethodGet, "/items", q, nil)
	require.NoError(t, err)
	resp.Body.Close()

	parsed, err := url.ParseQuery(gotRawQuery)
	require.NoError(t, err)
	assert.Equal(t, "10", parsed.Get("limit"))
	assert.Equal(t, "5", parsed.Get("offset"))
}

func TestDo_EmptyQueryParams(t *testing.T) {
	var gotRawQuery string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotRawQuery = r.URL.RawQuery
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)

	c := NewClient(srv.URL, "", "")
	resp, err := c.Do(http.MethodGet, "/items", url.Values{}, nil)
	require.NoError(t, err)
	resp.Body.Close()

	assert.Empty(t, gotRawQuery)
}

func TestDo_WithBody(t *testing.T) {
	var (
		gotContentType string
		gotBody        []byte
	)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotContentType = r.Header.Get("Content-Type")
		gotBody, _ = io.ReadAll(r.Body)
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)

	c := NewClient(srv.URL, "", "")
	body := map[string]string{"name": "test-schema"}
	resp, err := c.Do(http.MethodPost, "/schemas", nil, body)
	require.NoError(t, err)
	resp.Body.Close()

	assert.Equal(t, "application/json", gotContentType)

	var parsed map[string]string
	require.NoError(t, json.Unmarshal(gotBody, &parsed))
	assert.Equal(t, "test-schema", parsed["name"])
}

func TestDo_NilBody(t *testing.T) {
	var (
		gotContentType string
		gotBodyLen     int64
	)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotContentType = r.Header.Get("Content-Type")
		gotBodyLen = r.ContentLength
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)

	c := NewClient(srv.URL, "", "")
	resp, err := c.Do(http.MethodGet, "/schemas", nil, nil)
	require.NoError(t, err)
	resp.Body.Close()

	assert.Empty(t, gotContentType)
	assert.LessOrEqual(t, gotBodyLen, int64(0))
}

func TestDo_AcceptHeader(t *testing.T) {
	var gotAccept string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAccept = r.Header.Get("Accept")
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)

	c := NewClient(srv.URL, "", "")
	resp, err := c.Do(http.MethodGet, "/schemas", nil, nil)
	require.NoError(t, err)
	resp.Body.Close()

	assert.Equal(t, "application/json", gotAccept)
}

func TestDo_BearerToken(t *testing.T) {
	var gotAuth string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)

	c := NewClient(srv.URL, "", "my-jwt-token")
	resp, err := c.Do(http.MethodGet, "/schemas", nil, nil)
	require.NoError(t, err)
	resp.Body.Close()

	assert.Equal(t, "Bearer my-jwt-token", gotAuth)
}

func TestDo_APIKey(t *testing.T) {
	var (
		gotAPIKey string
		gotAuth   string
	)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAPIKey = r.Header.Get("X-API-Key")
		gotAuth = r.Header.Get("Authorization")
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)

	c := NewClient(srv.URL, "secret-key", "")
	resp, err := c.Do(http.MethodGet, "/schemas", nil, nil)
	require.NoError(t, err)
	resp.Body.Close()

	assert.Equal(t, "secret-key", gotAPIKey)
	assert.Empty(t, gotAuth)
}

func TestDo_TokenPrecedence(t *testing.T) {
	var (
		gotAPIKey string
		gotAuth   string
	)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAPIKey = r.Header.Get("X-API-Key")
		gotAuth = r.Header.Get("Authorization")
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)

	c := NewClient(srv.URL, "secret-key", "my-jwt-token")
	resp, err := c.Do(http.MethodGet, "/schemas", nil, nil)
	require.NoError(t, err)
	resp.Body.Close()

	assert.Equal(t, "Bearer my-jwt-token", gotAuth)
	assert.Empty(t, gotAPIKey)
}

func TestDo_NoAuth(t *testing.T) {
	var (
		gotAPIKey string
		gotAuth   string
	)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAPIKey = r.Header.Get("X-API-Key")
		gotAuth = r.Header.Get("Authorization")
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)

	c := NewClient(srv.URL, "", "")
	resp, err := c.Do(http.MethodGet, "/schemas", nil, nil)
	require.NoError(t, err)
	resp.Body.Close()

	assert.Empty(t, gotAuth)
	assert.Empty(t, gotAPIKey)
}

func TestDo_HTTPMethod(t *testing.T) {
	tests := []struct {
		name   string
		method string
	}{
		{name: "GET", method: http.MethodGet},
		{name: "POST", method: http.MethodPost},
		{name: "DELETE", method: http.MethodDelete},
		{name: "PATCH", method: http.MethodPatch},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var gotMethod string
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				gotMethod = r.Method
				w.WriteHeader(http.StatusOK)
			}))
			t.Cleanup(srv.Close)

			c := NewClient(srv.URL, "", "")
			resp, err := c.Do(tt.method, "/resource", nil, nil)
			require.NoError(t, err)
			resp.Body.Close()

			assert.Equal(t, tt.method, gotMethod)
		})
	}
}

func TestDo_ConnectionRefused(t *testing.T) {
	c := NewClient("http://127.0.0.1:1", "", "")
	_, err := c.Do(http.MethodGet, "/schemas", nil, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "execute request")
}

// === CheckError ===

func TestCheckError_SuccessRange(t *testing.T) {
	tests := []struct {
		name       string
		statusCode int
	}{
		{name: "200 OK", statusCode: 200},
		{name: "201 Created", statusCode: 201},
		{name: "204 No Content", statusCode: 204},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resp := &http.Response{
				StatusCode: tt.statusCode,
				Body:       io.NopCloser(strings.NewReader("")),
			}
			err := CheckError(resp)
			assert.NoError(t, err)
		})
	}
}

func TestCheckError_StructuredError(t *testing.T) {
	body := `{"code":403,"message":"forbidden"}`
	resp := &http.Response{
		StatusCode: 403,
		Body:       io.NopCloser(strings.NewReader(body)),
	}
	err := CheckError(resp)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "API error (HTTP 403): forbidden")
}

func TestCheckError_RawBodyFallback(t *testing.T) {
	resp := &http.Response{
		StatusCode: 500,
		Body:       io.NopCloser(strings.NewReader("Internal Server Error")),
	}
	err := CheckError(resp)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "API error (HTTP 500): Internal Server Error")
}

func TestCheckError_EmptyBody(t *testing.T) {
	resp := &http.Response{
		StatusCode: 500,
		Body:       io.NopCloser(strings.NewReader("")),
	}
	err := CheckError(resp)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "API error (HTTP 500): ")
}

func TestCheckError_EmptyMessage(t *testing.T) {
	body := `{"code":400,"message":""}`
	resp := &http.Response{
		StatusCode: 400,
		Body:       io.NopCloser(strings.NewReader(body)),
	}
	err := CheckError(resp)
	require.Error(t, err)
	// Empty message should fall back to raw body representation.
	assert.Contains(t, err.Error(), "API error (HTTP 400):")
	assert.Contains(t, err.Error(), body)
}

// === ReadBody ===

func TestReadBody_ReadsContent(t *testing.T) {
	expected := "hello, world"
	resp := &http.Response{
		Body: io.NopCloser(strings.NewReader(expected)),
	}
	data, err := ReadBody(resp)
	require.NoError(t, err)
	assert.Equal(t, expected, string(data))
}

// spyReadCloser tracks whether Close was called.
type spyReadCloser struct {
	io.Reader
	mu     sync.Mutex
	closed bool
}

func (s *spyReadCloser) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
	return nil
}

func (s *spyReadCloser) wasClosed() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closed
}

func TestReadBody_ClosesBody(t *testing.T) {
	spy := &spyReadCloser{Reader: strings.NewReader("some content")}
	resp := &http.Response{
		Body: spy,
	}
	_, err := ReadBody(resp)
	require.NoError(t, err)
	assert.True(t, spy.wasClosed(), "expected body to be closed after ReadBody")
}
