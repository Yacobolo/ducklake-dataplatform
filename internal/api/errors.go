package api

import (
	"errors"
	"net/http"

	"github.com/Yacobolo/quackstack/internal/domain"
)

// httpStatusFromDomainError maps domain errors to HTTP status codes.
func httpStatusFromDomainError(err error) int {
	var notFound *domain.NotFoundError
	var accessDenied *domain.AccessDeniedError
	var validation *domain.ValidationError
	var conflict *domain.ConflictError
	var notImplemented *domain.NotImplementedError

	switch {
	case errors.As(err, &notFound):
		return http.StatusNotFound
	case errors.As(err, &accessDenied):
		return http.StatusForbidden
	case errors.As(err, &validation):
		return http.StatusBadRequest
	case errors.As(err, &conflict):
		return http.StatusConflict
	case errors.As(err, &notImplemented):
		return http.StatusNotImplemented
	default:
		return http.StatusInternalServerError
	}
}

type domainErrorResponder[T any] struct {
	BadRequest     func(BadRequestJSONResponse) T
	Forbidden      func(ForbiddenJSONResponse) T
	NotFound       func(NotFoundJSONResponse) T
	Conflict       func(ConflictJSONResponse) T
	NotImplemented func(InternalErrorJSONResponse) T
	Internal       func(InternalErrorJSONResponse) T
}

func respondDomainError[T any](err error, responder domainErrorResponder[T]) (T, bool) {
	return respondDomainErrorForOperation("", err, responder)
}

func respondDomainErrorForOperation[T any](operationID string, err error, responder domainErrorResponder[T]) (T, bool) {
	var zero T
	status := httpStatusFromDomainError(err)
	if operationID != "" && !APIGenOperationAllowsStatus(operationID, status) {
		if APIGenOperationAllowsStatus(operationID, http.StatusInternalServerError) {
			status = http.StatusInternalServerError
		} else {
			return zero, false
		}
	}

	switch status {
	case http.StatusBadRequest:
		if responder.BadRequest != nil {
			return responder.BadRequest(badRequestErrorResponse(err)), true
		}
	case http.StatusForbidden:
		if responder.Forbidden != nil {
			return responder.Forbidden(forbiddenErrorResponse(err)), true
		}
	case http.StatusNotFound:
		if responder.NotFound != nil {
			return responder.NotFound(notFoundErrorResponse(err)), true
		}
	case http.StatusConflict:
		if responder.Conflict != nil {
			return responder.Conflict(conflictErrorResponse(err)), true
		}
	case http.StatusNotImplemented:
		if responder.NotImplemented != nil {
			return responder.NotImplemented(internalErrorResponse(err)), true
		}
		if responder.Internal != nil {
			return responder.Internal(internalErrorResponse(err)), true
		}
	default:
		if responder.Internal != nil {
			return responder.Internal(internalErrorResponse(err)), true
		}
	}

	return zero, false
}

func badRequestErrorResponse(err error) BadRequestJSONResponse {
	return BadRequestJSONResponse{
		Body:    errorBodyWithCode(http.StatusBadRequest, err),
		Headers: BadRequestResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}
}

func forbiddenErrorResponse(err error) ForbiddenJSONResponse {
	return ForbiddenJSONResponse{
		Body:    defaultErrorBody(err),
		Headers: ForbiddenResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}
}

func notFoundErrorResponse(err error) NotFoundJSONResponse {
	return NotFoundJSONResponse{
		Body:    defaultErrorBody(err),
		Headers: NotFoundResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}
}

func conflictErrorResponse(err error) ConflictJSONResponse {
	return ConflictJSONResponse{
		Body:    defaultErrorBody(err),
		Headers: ConflictResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}
}

func internalErrorResponse(err error) InternalErrorJSONResponse {
	return InternalErrorJSONResponse{
		Body:    defaultErrorBody(err),
		Headers: InternalErrorResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}
}

func defaultErrorBody(err error) Error {
	return errorBodyWithCode(httpStatusFromDomainError(err), err)
}

func errorBodyWithCode(code int, err error) Error {
	return Error{Code: int32(code), Message: err.Error()} //nolint:gosec // HTTP/application error codes are bounded
}
