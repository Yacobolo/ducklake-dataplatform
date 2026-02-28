// Package router provides endpoint selection strategies for compute routing.
package router

import (
	"context"

	"duck-demo/internal/domain"
)

// EndpointSelector chooses an endpoint from assignment candidates.
type EndpointSelector interface {
	Select(ctx context.Context, candidates []domain.ComputeEndpoint) (*domain.ComputeEndpoint, error)
}

// ActiveFirstSelector picks the first ACTIVE endpoint, else first candidate.
type ActiveFirstSelector struct{}

// NewActiveFirstSelector creates a simple endpoint selector for routing MVP.
func NewActiveFirstSelector() *ActiveFirstSelector {
	return &ActiveFirstSelector{}
}

// Select returns the first ACTIVE endpoint from candidates.
func (s *ActiveFirstSelector) Select(_ context.Context, candidates []domain.ComputeEndpoint) (*domain.ComputeEndpoint, error) {
	if len(candidates) == 0 {
		return nil, nil
	}

	for i := range candidates {
		if candidates[i].Status == "ACTIVE" {
			ep := candidates[i]
			return &ep, nil
		}
	}

	ep := candidates[0]
	return &ep, nil
}
