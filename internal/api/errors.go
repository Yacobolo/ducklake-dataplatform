package api

import (
	"errors"
	"net/http"

	"duck-demo/internal/domain"
)

// httpStatusFromDomainError maps domain errors to HTTP status codes.
func httpStatusFromDomainError(err error) int {
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
