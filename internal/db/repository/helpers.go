// Package repository implements domain repository interfaces using SQLite.
package repository

import (
	"database/sql"
	"errors"
	"strings"

	"duck-demo/internal/domain"
)

func boolToInt(b bool) int64 {
	if b {
		return 1
	}
	return 0
}

func mapDBError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, sql.ErrNoRows) {
		return &domain.NotFoundError{Message: "resource not found"}
	}
	if strings.Contains(err.Error(), "UNIQUE constraint failed") {
		return &domain.ConflictError{Message: "resource already exists"}
	}
	return err
}
