package domain

import "testing"

func TestUpdateModelRequestValidate(t *testing.T) {
	t.Run("nil materialization allowed", func(t *testing.T) {
		req := UpdateModelRequest{}
		if err := req.Validate(); err != nil {
			t.Fatalf("expected nil error, got %v", err)
		}
	})

	t.Run("valid materialization accepted", func(t *testing.T) {
		mat := MaterializationIncremental
		req := UpdateModelRequest{Materialization: &mat}
		if err := req.Validate(); err != nil {
			t.Fatalf("expected nil error, got %v", err)
		}
	})

	t.Run("invalid materialization rejected", func(t *testing.T) {
		mat := "bad_case"
		req := UpdateModelRequest{Materialization: &mat}
		if err := req.Validate(); err == nil {
			t.Fatal("expected validation error, got nil")
		}
	})
}
