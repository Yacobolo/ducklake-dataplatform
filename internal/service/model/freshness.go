package model

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
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

// CheckSourceFreshness evaluates freshness directly against a source relation timestamp column.
// If timestampColumn is empty, it selects the first existing column from:
// loaded_at, updated_at, created_at, ingested_at.
func (s *Service) CheckSourceFreshness(
	ctx context.Context,
	principal string,
	sourceSchema string,
	sourceTable string,
	timestampColumn string,
	maxLagSeconds int64,
) (*domain.SourceFreshnessStatus, error) {
	status := &domain.SourceFreshnessStatus{
		SourceSchema:  sourceSchema,
		SourceTable:   sourceTable,
		TimestampCol:  timestampColumn,
		MaxLagSeconds: maxLagSeconds,
	}
	if maxLagSeconds <= 0 {
		status.IsFresh = true
		return status, nil
	}

	conn, err := s.duckDB.Conn(ctx)
	if err != nil {
		return nil, fmt.Errorf("acquire connection for source freshness: %w", err)
	}
	defer func() { _ = conn.Close() }()

	if strings.TrimSpace(status.TimestampCol) == "" {
		status.TimestampCol, err = s.resolveFreshnessTimestampColumn(ctx, conn, principal, sourceSchema, sourceTable)
		if err != nil {
			return nil, err
		}
	}

	query := fmt.Sprintf(
		"SELECT MAX(%s) FROM %s",
		quoteIdent(status.TimestampCol),
		renderRelationParts(sourceSchema, sourceTable),
	)
	rows, err := s.engine.QueryOnConn(ctx, conn, principal, query)
	if err != nil {
		return nil, fmt.Errorf("query source freshness timestamp: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var last sqlNullTime
	if rows.Next() {
		if err := rows.Scan(&last); err != nil {
			return nil, fmt.Errorf("scan source freshness timestamp: %w", err)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate source freshness timestamp rows: %w", err)
	}
	if !last.Valid {
		status.IsFresh = false
		return status, nil
	}
	status.LastLoadedAt = &last.Time

	deadline := last.Time.Add(time.Duration(maxLagSeconds) * time.Second)
	if time.Now().After(deadline) {
		status.IsFresh = false
		staleSince := deadline
		status.StaleSince = &staleSince
		return status, nil
	}
	status.IsFresh = true
	return status, nil
}

type sqlNullTime struct {
	Time  time.Time
	Valid bool
}

func (nt *sqlNullTime) Scan(value any) error {
	if value == nil {
		nt.Time = time.Time{}
		nt.Valid = false
		return nil
	}
	switch v := value.(type) {
	case time.Time:
		nt.Time = v
		nt.Valid = true
		return nil
	default:
		return fmt.Errorf("unsupported time scan type %T", value)
	}
}

func (s *Service) resolveFreshnessTimestampColumn(
	ctx context.Context,
	conn *sql.Conn,
	principal string,
	sourceSchema string,
	sourceTable string,
) (string, error) {
	query := fmt.Sprintf(
		"SELECT column_name FROM information_schema.columns WHERE table_schema = '%s' AND table_name = '%s'",
		strings.ReplaceAll(sourceSchema, "'", "''"),
		strings.ReplaceAll(sourceTable, "'", "''"),
	)
	rows, err := s.engine.QueryOnConn(ctx, conn, principal, query)
	if err != nil {
		return "", fmt.Errorf("query source columns for freshness: %w", err)
	}
	defer func() { _ = rows.Close() }()

	cols := make(map[string]struct{})
	for rows.Next() {
		var c string
		if err := rows.Scan(&c); err != nil {
			return "", fmt.Errorf("scan source freshness column: %w", err)
		}
		cols[strings.ToLower(strings.TrimSpace(c))] = struct{}{}
	}
	if err := rows.Err(); err != nil {
		return "", fmt.Errorf("iterate source freshness columns: %w", err)
	}

	candidates := []string{"loaded_at", "updated_at", "created_at", "ingested_at"}
	for _, c := range candidates {
		if _, ok := cols[c]; ok {
			return c, nil
		}
	}
	return "", domain.ErrValidation(
		"source freshness requires timestamp column; none of loaded_at, updated_at, created_at, ingested_at found on %s.%s",
		sourceSchema,
		sourceTable,
	)
}
