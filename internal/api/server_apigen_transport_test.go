package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWriteAPIGenError_WritesContractJSON(t *testing.T) {
	t.Helper()

	recorder := httptest.NewRecorder()

	writeAPIGenError(recorder, http.StatusBadRequest, `missing required query parameter "query"`)

	result := recorder.Result()
	t.Cleanup(func() {
		_ = result.Body.Close()
	})

	require.Equal(t, http.StatusBadRequest, result.StatusCode)
	assert.Equal(t, "application/json", result.Header.Get("Content-Type"))

	var payload Error
	require.NoError(t, json.NewDecoder(result.Body).Decode(&payload))
	assert.Equal(t, int32(http.StatusBadRequest), payload.Code)
	assert.Equal(t, `missing required query parameter "query"`, payload.Message)
}

func TestWriteAPIGenError_Sanitizes5xxMessages(t *testing.T) {
	t.Helper()

	recorder := httptest.NewRecorder()

	writeAPIGenError(recorder, http.StatusInternalServerError, "sql: connection pool exhausted")

	result := recorder.Result()
	t.Cleanup(func() {
		_ = result.Body.Close()
	})

	require.Equal(t, http.StatusInternalServerError, result.StatusCode)
	assert.Equal(t, "application/json", result.Header.Get("Content-Type"))

	var payload Error
	require.NoError(t, json.NewDecoder(result.Body).Decode(&payload))
	assert.Equal(t, int32(http.StatusInternalServerError), payload.Code)
	assert.Equal(t, "internal server error", payload.Message)
}

func TestDecodeAPIGenJSONBody(t *testing.T) {
	t.Helper()

	t.Run("accepts valid body", func(t *testing.T) {
		var body GenCreatePipelineJSONBody

		err := decodeAPIGenJSONBody(strings.NewReader(`{"name":"nightly"}`), &body)

		require.NoError(t, err)
		assert.Equal(t, "nightly", body.Name)
	})

	t.Run("rejects empty body", func(t *testing.T) {
		var body GenCreatePipelineJSONBody

		err := decodeAPIGenJSONBody(strings.NewReader(""), &body)

		require.EqualError(t, err, "request body must not be empty")
	})

	t.Run("rejects unknown fields", func(t *testing.T) {
		var body GenCreatePipelineJSONBody

		err := decodeAPIGenJSONBody(strings.NewReader(`{"name":"nightly","extra":true}`), &body)

		require.Error(t, err)
		assert.EqualError(t, err, `invalid JSON body: json: unknown field "extra"`)
	})

	t.Run("rejects trailing json values", func(t *testing.T) {
		var body GenCreatePipelineJSONBody

		err := decodeAPIGenJSONBody(strings.NewReader(`{"name":"nightly"} {"name":"second"}`), &body)

		require.EqualError(t, err, "request body must contain a single JSON value")
	})
}
