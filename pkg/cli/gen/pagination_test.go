package gen

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFetchAllPages_SinglePage(t *testing.T) {
	var requestCount int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt64(&requestCount, 1)
		resp := PaginatedResponse{
			Data:          []interface{}{"item1", "item2"},
			NextPageToken: "",
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(resp)
	}))
	defer srv.Close()

	client := NewClient(srv.URL, "", "")
	items, err := FetchAllPages(client, http.MethodGet, "/things", nil)

	require.NoError(t, err)
	assert.Len(t, items, 2)
	assert.Equal(t, "item1", items[0])
	assert.Equal(t, "item2", items[1])
	assert.Equal(t, int64(1), atomic.LoadInt64(&requestCount))
}

func TestFetchAllPages_MultiPage(t *testing.T) {
	var requestCount int64
	var receivedTokens []string

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := atomic.AddInt64(&requestCount, 1)
		receivedTokens = append(receivedTokens, r.URL.Query().Get("page_token"))

		var resp PaginatedResponse
		switch n {
		case 1:
			resp = PaginatedResponse{
				Data:          []interface{}{"a", "b"},
				NextPageToken: "p2",
			}
		case 2:
			resp = PaginatedResponse{
				Data:          []interface{}{"c", "d"},
				NextPageToken: "p3",
			}
		case 3:
			resp = PaginatedResponse{
				Data:          []interface{}{"e"},
				NextPageToken: "",
			}
		default:
			t.Fatalf("unexpected request #%d", n)
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(resp)
	}))
	defer srv.Close()

	client := NewClient(srv.URL, "", "")
	items, err := FetchAllPages(client, http.MethodGet, "/things", nil)

	require.NoError(t, err)
	assert.Len(t, items, 5)
	assert.Equal(t, []interface{}{"a", "b", "c", "d", "e"}, items)
	assert.Equal(t, int64(3), atomic.LoadInt64(&requestCount))

	// First request should have no page_token, 2nd should have "p2", 3rd "p3".
	require.Len(t, receivedTokens, 3)
	assert.Equal(t, "", receivedTokens[0])
	assert.Equal(t, "p2", receivedTokens[1])
	assert.Equal(t, "p3", receivedTokens[2])
}

func TestFetchAllPages_EmptyFirstPage(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		resp := PaginatedResponse{
			Data:          nil,
			NextPageToken: "",
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(resp)
	}))
	defer srv.Close()

	client := NewClient(srv.URL, "", "")
	items, err := FetchAllPages(client, http.MethodGet, "/things", nil)

	require.NoError(t, err)
	assert.Nil(t, items, "expected nil slice when server returns empty data")
}

func TestFetchAllPages_HTTPError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"code":    500,
			"message": "something broke",
		})
	}))
	defer srv.Close()

	client := NewClient(srv.URL, "", "")
	items, err := FetchAllPages(client, http.MethodGet, "/things", nil)

	require.Error(t, err)
	assert.Nil(t, items)
	assert.Contains(t, err.Error(), "API error")
}

func TestFetchAllPages_ConnectionError(t *testing.T) {
	// Start and immediately close a server to get a guaranteed-dead URL.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	deadURL := srv.URL
	srv.Close()

	client := NewClient(deadURL, "", "")
	items, err := FetchAllPages(client, http.MethodGet, "/things", nil)

	require.Error(t, err)
	assert.Nil(t, items)
	assert.Contains(t, err.Error(), "execute request")
}

func TestFetchAllPages_InvalidJSON(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{not valid json`))
	}))
	defer srv.Close()

	client := NewClient(srv.URL, "", "")
	items, err := FetchAllPages(client, http.MethodGet, "/things", nil)

	require.Error(t, err)
	assert.Nil(t, items)
	assert.Contains(t, err.Error(), "parse response")
}

func TestFetchAllPages_PreservesBaseQuery(t *testing.T) {
	var receivedQueries []url.Values

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedQueries = append(receivedQueries, r.URL.Query())

		var resp PaginatedResponse
		if len(receivedQueries) == 1 {
			resp = PaginatedResponse{
				Data:          []interface{}{"x"},
				NextPageToken: "tok2",
			}
		} else {
			resp = PaginatedResponse{
				Data:          []interface{}{"y"},
				NextPageToken: "",
			}
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(resp)
	}))
	defer srv.Close()

	baseQuery := url.Values{"max_results": []string{"50"}}
	client := NewClient(srv.URL, "", "")
	items, err := FetchAllPages(client, http.MethodGet, "/things", baseQuery)

	require.NoError(t, err)
	assert.Len(t, items, 2)

	// First request should carry max_results from baseQuery.
	require.Len(t, receivedQueries, 2)
	assert.Equal(t, "50", receivedQueries[0].Get("max_results"))

	// Second request should also carry max_results AND the page_token.
	assert.Equal(t, "50", receivedQueries[1].Get("max_results"))
	assert.Equal(t, "tok2", receivedQueries[1].Get("page_token"))

	// baseQuery must NOT be mutated — page_token should never appear in it.
	assert.Empty(t, baseQuery.Get("page_token"), "baseQuery must not be mutated by FetchAllPages")
}
