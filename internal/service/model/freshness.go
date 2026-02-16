package model

import (
	"context"
	"time"

	"duck-demo/internal/domain"
)

// CheckFreshness evaluates whether a model is fresh based on its freshness policy.
func (s *Service) CheckFreshness(ctx context.Context, projectName, modelName string) (*domain.FreshnessStatus, error) {
	m, err := s.models.GetByName(ctx, projectName, modelName)
	if err != nil {
		return nil, err
	}

	if m.Freshness == nil || m.Freshness.MaxLagSeconds <= 0 {
		return &domain.FreshnessStatus{
			IsFresh:       true,
			MaxLagSeconds: 0,
		}, nil
	}

	// Find the last successful run that included this model.
	runs, _, err := s.runs.ListRuns(ctx, domain.ModelRunFilter{
		Status: strPtr(domain.ModelRunStatusSuccess),
		Page:   domain.PageRequest{MaxResults: 1},
	})
	if err != nil {
		return nil, err
	}

	status := &domain.FreshnessStatus{
		MaxLagSeconds: m.Freshness.MaxLagSeconds,
	}

	if len(runs) == 0 || runs[0].FinishedAt == nil {
		// No successful run found — model is stale.
		status.IsFresh = false
		return status, nil
	}

	lastRun := runs[0].FinishedAt
	status.LastRunAt = lastRun

	deadline := lastRun.Add(time.Duration(m.Freshness.MaxLagSeconds) * time.Second)
	now := time.Now()

	if now.After(deadline) {
		status.IsFresh = false
		staleSince := deadline
		status.StaleSince = &staleSince
	} else {
		status.IsFresh = true
	}

	return status, nil
}

func strPtr(s string) *string {
	return &s
}
