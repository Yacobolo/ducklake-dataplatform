package model

import (
	"context"
	"fmt"
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

	status := &domain.FreshnessStatus{
		MaxLagSeconds: m.Freshness.MaxLagSeconds,
	}

	lastRunAt, err := s.findLastSuccessfulModelRun(ctx, m.QualifiedName())
	if err != nil {
		return nil, err
	}

	if lastRunAt == nil {
		// No successful run found — model is stale.
		status.IsFresh = false
		return status, nil
	}

	status.LastRunAt = lastRunAt

	deadline := lastRunAt.Add(time.Duration(m.Freshness.MaxLagSeconds) * time.Second)
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

func (s *Service) findLastSuccessfulModelRun(ctx context.Context, qualifiedModelName string) (*time.Time, error) {
	page := domain.PageRequest{MaxResults: domain.MaxMaxResults}

	for {
		runs, total, err := s.runs.ListRuns(ctx, domain.ModelRunFilter{
			Status: strPtr(domain.ModelRunStatusSuccess),
			Page:   page,
		})
		if err != nil {
			return nil, fmt.Errorf("list successful runs: %w", err)
		}

		for _, run := range runs {
			if run.FinishedAt == nil {
				continue
			}

			steps, err := s.runs.ListStepsByRun(ctx, run.ID)
			if err != nil {
				return nil, fmt.Errorf("list run steps for run %s: %w", run.ID, err)
			}

			for _, step := range steps {
				if step.ModelName == qualifiedModelName && step.Status == domain.ModelRunStatusSuccess {
					return run.FinishedAt, nil
				}
			}
		}

		nextOffset := page.Offset() + page.Limit()
		if int64(nextOffset) >= total || len(runs) == 0 {
			return nil, nil
		}
		page.PageToken = domain.EncodePageToken(nextOffset)
	}
}

func strPtr(s string) *string {
	return &s
}
