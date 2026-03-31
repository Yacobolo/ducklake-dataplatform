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
		var catalogName string

		err := bindPathParameter("catalogName", "analytics", true, &catalogName)

		require.NoError(t, err)
		assert.Equal(t, "analytics", catalogName)
	})

	t.Run("binds int aliases", func(t *testing.T) {
		var maxResults int32

		err := bindPathParameter("max_results", "25", true, &maxResults)

		require.NoError(t, err)
		assert.EqualValues(t, 25, maxResults)
	})

	t.Run("returns required missing error", func(t *testing.T) {
		var catalogName string

		err := bindPathParameter("catalogName", "", true, &catalogName)

		require.Error(t, err)
		assert.EqualError(t, err, "missing required path parameter \"catalogName\"")
	})

	t.Run("returns parse errors", func(t *testing.T) {
		var maxResults int32

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

		var maxResults *int32
		var principalType *string

		require.NoError(t, bindQueryParameter(params, "max_results", false, &maxResults))
		require.NoError(t, bindQueryParameter(params, "principal_type", false, &principalType))

		require.NotNil(t, maxResults)
		assert.EqualValues(t, 50, *maxResults)
		require.NotNil(t, principalType)
		assert.Equal(t, "user", *principalType)
	})

	t.Run("binds repeated string slices", func(t *testing.T) {
		params := url.Values{
			"f": []string{"borough:Queens", "borough:Brooklyn"},
		}

		var filters *[]string

		err := bindQueryParameter(params, "f", false, &filters)

		require.NoError(t, err)
		require.NotNil(t, filters)
		assert.Equal(t, []string{"borough:Queens", "borough:Brooklyn"}, *filters)
	})

	t.Run("binds comma separated string slices", func(t *testing.T) {
		params := url.Values{
			"f": []string{"borough:Queens,borough:Brooklyn"},
		}

		var filters *[]string

		err := bindQueryParameter(params, "f", false, &filters)

		require.NoError(t, err)
		require.NotNil(t, filters)
		assert.Equal(t, []string{"borough:Queens", "borough:Brooklyn"}, *filters)
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
		var pageToken *string

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
