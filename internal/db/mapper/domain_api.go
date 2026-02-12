// Package mapper converts between domain, database, and API layer types.
package mapper

import (
	"errors"
	"net/http"

	"duck-demo/internal/domain"
)

// HTTPStatusFromDomainError maps domain errors to HTTP status codes.
func HTTPStatusFromDomainError(err error) int {
	var notFound *domain.NotFoundError
	var accessDenied *domain.AccessDeniedError
	var validation *domain.ValidationError
	var conflict *domain.ConflictError

	switch {
	case errors.As(err, &notFound):
		return http.StatusNotFound
	case errors.As(err, &accessDenied):
		return http.StatusForbidden
	case errors.As(err, &validation):
		return http.StatusBadRequest
	case errors.As(err, &conflict):
		return http.StatusConflict
	default:
		return http.StatusInternalServerError
	}
}
