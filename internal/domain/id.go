package domain

import (
	"strconv"

	"github.com/google/uuid"
)

// NewID generates a UUIDv7 string for application-owned entities.
func NewID() string {
	return uuid.Must(uuid.NewV7()).String()
}

// DuckLakeIDToString converts a DuckLake integer ID to its string representation.
func DuckLakeIDToString(id int64) string {
	return strconv.FormatInt(id, 10)
}

// StringToDuckLakeID converts a string ID back to a DuckLake integer ID.
func StringToDuckLakeID(s string) (int64, error) {
	return strconv.ParseInt(s, 10, 64)
}
