// Package domain defines core types, interfaces, and errors for the data platform.
package domain

import "fmt"

// NotFoundError indicates a resource was not found.
type NotFoundError struct {
	Message string
}

func (e *NotFoundError) Error() string { return e.Message }

// AccessDeniedError indicates insufficient permissions.
type AccessDeniedError struct {
	Message string
}

func (e *AccessDeniedError) Error() string { return e.Message }

// ValidationError indicates invalid input.
type ValidationError struct {
	Message string
}

func (e *ValidationError) Error() string { return e.Message }

// ConflictError indicates a conflict (e.g., duplicate resource).
type ConflictError struct {
	Message string
}

func (e *ConflictError) Error() string { return e.Message }

// ErrNotFound creates a NotFoundError with a formatted message.
func ErrNotFound(format string, args ...interface{}) *NotFoundError {
	return &NotFoundError{Message: fmt.Sprintf(format, args...)}
}

// ErrAccessDenied creates an AccessDeniedError with a formatted message.
func ErrAccessDenied(format string, args ...interface{}) *AccessDeniedError {
	return &AccessDeniedError{Message: fmt.Sprintf(format, args...)}
}

// ErrValidation creates a ValidationError with a formatted message.
func ErrValidation(format string, args ...interface{}) *ValidationError {
	return &ValidationError{Message: fmt.Sprintf(format, args...)}
}

// ErrConflict creates a ConflictError with a formatted message.
func ErrConflict(format string, args ...interface{}) *ConflictError {
	return &ConflictError{Message: fmt.Sprintf(format, args...)}
}
