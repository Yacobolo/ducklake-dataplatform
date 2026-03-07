package api

import (
	"net/url"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBindPathParameter(t *testing.T) {
	t.Helper()

	t.Run("binds string aliases", func(t *testing.T) {
		var catalogName CatalogName

		err := bindPathParameter("catalogName", "analytics", true, &catalogName)

		require.NoError(t, err)
		assert.Equal(t, CatalogName("analytics"), catalogName)
	})

	t.Run("binds int aliases", func(t *testing.T) {
		var maxResults MaxResults

		err := bindPathParameter("max_results", "25", true, &maxResults)

		require.NoError(t, err)
		assert.Equal(t, MaxResults(25), maxResults)
	})

	t.Run("returns required missing error", func(t *testing.T) {
		var catalogName CatalogName

		err := bindPathParameter("catalogName", "", true, &catalogName)

		require.Error(t, err)
		assert.EqualError(t, err, "missing required path parameter \"catalogName\"")
	})

	t.Run("returns parse errors", func(t *testing.T) {
		var maxResults MaxResults

		err := bindPathParameter("max_results", "nope", true, &maxResults)

		require.Error(t, err)
		assert.ErrorContains(t, err, "invalid path parameter \"max_results\"")
	})
}

func TestBindQueryParameter(t *testing.T) {
	t.Helper()

	t.Run("binds optional pointer aliases and enums", func(t *testing.T) {
		params := url.Values{
			"max_results":    []string{"50"},
			"principal_type": []string{"user"},
		}

		var maxResults *MaxResults
		var principalType *ListGrantsParamsPrincipalType

		require.NoError(t, bindQueryParameter(params, "max_results", false, &maxResults))
		require.NoError(t, bindQueryParameter(params, "principal_type", false, &principalType))

		require.NotNil(t, maxResults)
		assert.Equal(t, MaxResults(50), *maxResults)
		require.NotNil(t, principalType)
		assert.Equal(t, ListGrantsParamsPrincipalType("user"), *principalType)
	})

	t.Run("binds required booleans", func(t *testing.T) {
		params := url.Values{"force": []string{"true"}}
		var force bool

		err := bindQueryParameter(params, "force", true, &force)

		require.NoError(t, err)
		assert.True(t, force)
	})

	t.Run("binds optional time pointers", func(t *testing.T) {
		params := url.Values{"from": []string{"2026-03-07T12:34:56Z"}}
		var from *time.Time

		err := bindQueryParameter(params, "from", false, &from)

		require.NoError(t, err)
		require.NotNil(t, from)
		assert.Equal(t, time.Date(2026, time.March, 7, 12, 34, 56, 0, time.UTC), *from)
	})

	t.Run("leaves optional pointers nil when absent", func(t *testing.T) {
		var pageToken *PageToken

		err := bindQueryParameter(url.Values{}, "page_token", false, &pageToken)

		require.NoError(t, err)
		assert.Nil(t, pageToken)
	})

	t.Run("returns required missing error", func(t *testing.T) {
		var query string

		err := bindQueryParameter(url.Values{}, "query", true, &query)

		require.Error(t, err)
		assert.EqualError(t, err, "missing required query parameter \"query\"")
	})

	t.Run("returns parse errors", func(t *testing.T) {
		params := url.Values{"from": []string{"yesterday"}}
		var from *time.Time

		err := bindQueryParameter(params, "from", false, &from)

		require.Error(t, err)
		assert.ErrorContains(t, err, "invalid query parameter \"from\"")
	})
}
