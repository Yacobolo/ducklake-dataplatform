package api

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"duck-demo/internal/domain"
)

func TestDefaultErrorResponses(t *testing.T) {
	t.Parallel()

	t.Run("bad request", func(t *testing.T) {
		t.Parallel()

		resp := badRequestErrorResponse(domain.ErrValidation("bad input"))
		assert.Equal(t, int32(400), resp.Body.Code)
		assert.Equal(t, "bad input", resp.Body.Message)
		assert.Equal(t, int32(defaultRateLimitLimit), resp.Headers.XRateLimitLimit)
	})

	t.Run("forbidden", func(t *testing.T) {
		t.Parallel()

		resp := forbiddenErrorResponse(domain.ErrAccessDenied("forbidden"))
		assert.Equal(t, int32(403), resp.Body.Code)
		assert.Equal(t, "forbidden", resp.Body.Message)
		assert.Equal(t, int32(defaultRateLimitRemaining), resp.Headers.XRateLimitRemaining)
	})

	t.Run("not found", func(t *testing.T) {
		t.Parallel()

		resp := notFoundErrorResponse(domain.ErrNotFound("missing"))
		assert.Equal(t, int32(404), resp.Body.Code)
		assert.Equal(t, "missing", resp.Body.Message)
		assert.Equal(t, int64(defaultRateLimitReset), resp.Headers.XRateLimitReset)
	})

	t.Run("conflict", func(t *testing.T) {
		t.Parallel()

		resp := conflictErrorResponse(domain.ErrConflict("duplicate"))
		assert.Equal(t, int32(409), resp.Body.Code)
		assert.Equal(t, "duplicate", resp.Body.Message)
		assert.Equal(t, int32(defaultRateLimitLimit), resp.Headers.XRateLimitLimit)
	})

	t.Run("not implemented", func(t *testing.T) {
		t.Parallel()

		resp := internalErrorResponse(domain.ErrNotImplemented("later"))
		assert.Equal(t, int32(501), resp.Body.Code)
		assert.Equal(t, "later", resp.Body.Message)
		assert.Equal(t, int64(defaultRateLimitReset), resp.Headers.XRateLimitReset)
	})

	t.Run("internal", func(t *testing.T) {
		t.Parallel()

		resp := internalErrorResponse(errors.New("boom"))
		assert.Equal(t, int32(500), resp.Body.Code)
		assert.Equal(t, "boom", resp.Body.Message)
		assert.Equal(t, int32(defaultRateLimitRemaining), resp.Headers.XRateLimitRemaining)
	})
}

func TestRespondDomainError(t *testing.T) {
	t.Parallel()

	t.Run("supported domain error returns typed response", func(t *testing.T) {
		t.Parallel()

		resp, ok := respondDomainError[string](domain.ErrAccessDenied("forbidden"), domainErrorResponder[string]{
			Forbidden: func(resp ForbiddenJSONResponse) string {
				return resp.Body.Message
			},
		})
		require.True(t, ok)
		assert.Equal(t, "forbidden", resp)
	})

	t.Run("unsupported domain error returns false", func(t *testing.T) {
		t.Parallel()

		resp, ok := respondDomainError[string](domain.ErrConflict("duplicate"), domainErrorResponder[string]{
			Forbidden: func(resp ForbiddenJSONResponse) string {
				return resp.Body.Message
			},
		})
		require.False(t, ok)
		assert.Empty(t, resp)
	})

	t.Run("generic error can map to internal when requested", func(t *testing.T) {
		t.Parallel()

		resp, ok := respondDomainError[int](errors.New("boom"), domainErrorResponder[int]{
			Internal: func(resp InternalErrorJSONResponse) int {
				return int(resp.Body.Code)
			},
		})
		require.True(t, ok)
		assert.Equal(t, 500, resp)
	})

	t.Run("not implemented can map to internal response type", func(t *testing.T) {
		t.Parallel()

		resp, ok := respondDomainError[int](domain.ErrNotImplemented("later"), domainErrorResponder[int]{
			Internal: func(resp InternalErrorJSONResponse) int {
				return int(resp.Body.Code)
			},
		})
		require.True(t, ok)
		assert.Equal(t, 501, resp)
	})
}
