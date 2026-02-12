package compute

import (
	"context"

	"duck-demo/internal/domain"
)

var _ domain.ComputeResolver = (*DefaultResolver)(nil)

// DefaultResolver implements ComputeResolver. In Phase 2 it always returns nil
// (fall back to local *sql.DB). Phase 3 adds assignment lookup and REMOTE
// executor support.
type DefaultResolver struct {
	localExec *LocalExecutor
}

// NewDefaultResolver creates a DefaultResolver with the given local executor.
func NewDefaultResolver(localExec *LocalExecutor) *DefaultResolver {
	return &DefaultResolver{localExec: localExec}
}

// Resolve returns nil for Phase 2 — the engine falls back to the local *sql.DB.
// Phase 3 adds assignment lookup and REMOTE executor support.
func (r *DefaultResolver) Resolve(ctx context.Context, principalName string) (domain.ComputeExecutor, error) {
	return nil, nil
}
