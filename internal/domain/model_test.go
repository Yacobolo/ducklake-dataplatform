package domain

import (
	"strings"
	"testing"
)

func TestCreateModelRequestValidate_NameLength(t *testing.T) {
	base := CreateModelRequest{
		ProjectName:     "proj",
		SQL:             "SELECT 1",
		Materialization: MaterializationView,
	}

	t.Run("accepts_255_chars", func(t *testing.T) {
		req := base
		req.Name = strings.Repeat("a", MaxModelNameLength)
		if err := req.Validate(); err != nil {
			t.Fatalf("expected nil error, got %v", err)
		}
	})

	t.Run("rejects_256_chars", func(t *testing.T) {
		req := base
		req.Name = strings.Repeat("a", MaxModelNameLength+1)
		if err := req.Validate(); err == nil {
			t.Fatal("expected validation error, got nil")
		}
	})
}
