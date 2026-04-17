package repository

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"github.com/Yacobolo/quackstack/internal/db/dbstore"
	"github.com/Yacobolo/quackstack/internal/domain"
)

// Compile-time check.
var _ domain.PipelineRunRepository = (*PipelineRunRepo)(nil)

// PipelineRunRepo implements PipelineRunRepository using SQLite.
type PipelineRunRepo struct {
	q  *dbstore.Queries
	db *sql.DB
}

// NewPipelineRunRepo creates a new PipelineRunRepo.
func NewPipelineRunRepo(db *sql.DB) *PipelineRunRepo {
	return &PipelineRunRepo{q: dbstore.New(db), db: db}
}

// CreateRun inserts a new pipeline run.
func (r *PipelineRunRepo) CreateRun(ctx context.Context, run *domain.PipelineRun) (*domain.PipelineRun, error) {
	paramsJSON, err := json.Marshal(run.Parameters)
	if err != nil {
		return nil, fmt.Errorf("marshal parameters: %w", err)
	}
	provenanceJSON, err := json.Marshal(run.Provenance)
	if err != nil {
		return nil, fmt.Errorf("marshal provenance: %w", err)
	}

	row, err := r.q.CreatePipelineRun(ctx, dbstore.CreatePipelineRunParams{
		ID:                 defaultString(run.ID, newID()),
		PipelineID:         run.PipelineID,
		Status:             run.Status,
		TriggerType:        run.TriggerType,
		TriggeredBy:        run.TriggeredBy,
		EffectivePrincipal: run.EffectivePrincipal,
		Parameters:         string(paramsJSON),
		GitCommitHash:      nullStringPtr(run.GitCommitHash),
		QueuedAt:           nullTimeString(run.QueuedAt),
		QueueStartedAt:     nullTimeString(run.QueueStartedAt),
		RepairedFromRunID:  nullStringPtr(run.RepairedFromRunID),
		Provenance:         string(provenanceJSON),
		SlaBreachedAt:      nullTimeString(run.SLABreachedAt),
	})
	if err != nil {
		return nil, mapDBError(err)
	}
	return pipelineRunFromDB(row), nil
}

// GetRunByID returns a pipeline run by its ID.
func (r *PipelineRunRepo) GetRunByID(ctx context.Context, id string) (*domain.PipelineRun, error) {
	row, err := r.q.GetPipelineRunByID(ctx, id)
	if err != nil {
		return nil, mapDBError(err)
	}
	return pipelineRunFromDB(row), nil
}

// ListRuns returns a filtered, paginated list of pipeline runs.
func (r *PipelineRunRepo) ListRuns(ctx context.Context, filter domain.PipelineRunFilter) ([]domain.PipelineRun, int64, error) {
	pipelineIDFilter := ""
	if filter.PipelineID != nil {
		pipelineIDFilter = *filter.PipelineID
	}
	statusFilter := ""
	if filter.Status != nil {
		statusFilter = *filter.Status
	}

	total, err := r.q.CountPipelineRuns(ctx, dbstore.CountPipelineRunsParams{
		Column1:    pipelineIDFilter,
		PipelineID: pipelineIDFilter,
		Column3:    statusFilter,
		Status:     statusFilter,
	})
	if err != nil {
		return nil, 0, err
	}

	rows, err := r.q.ListPipelineRuns(ctx, dbstore.ListPipelineRunsParams{
		Column1:    pipelineIDFilter,
		PipelineID: pipelineIDFilter,
		Column3:    statusFilter,
		Status:     statusFilter,
		Limit:      int64(filter.Page.Limit()),
		Offset:     int64(filter.Page.Offset()),
	})
	if err != nil {
		return nil, 0, err
	}

	runs := make([]domain.PipelineRun, 0, len(rows))
	for _, row := range rows {
		runs = append(runs, *pipelineRunFromDB(row))
	}
	return runs, total, nil
}

// ListQueuedRuns returns queued runs for a pipeline ordered by queue time.
func (r *PipelineRunRepo) ListQueuedRuns(ctx context.Context, pipelineID string, limit int) ([]domain.PipelineRun, error) {
	rows, err := r.q.ListQueuedPipelineRuns(ctx, dbstore.ListQueuedPipelineRunsParams{
		PipelineID: pipelineID,
		Limit:      int64(limit),
	})
	if err != nil {
		return nil, mapDBError(err)
	}
	runs := make([]domain.PipelineRun, 0, len(rows))
	for _, row := range rows {
		runs = append(runs, *pipelineRunFromDB(row))
	}
	return runs, nil
}

// UpdateRunStatus updates the status and optional error message of a pipeline run.
func (r *PipelineRunRepo) UpdateRunStatus(ctx context.Context, id string, status string, errorMsg *string) error {
	return mapDBError(r.q.UpdatePipelineRunStatus(ctx, dbstore.UpdatePipelineRunStatusParams{
		Status:       status,
		ErrorMessage: nullStrFromPtr(errorMsg),
		ID:           id,
	}))
}

// MarkRunSLABreached records that the run exceeded its SLA window.
func (r *PipelineRunRepo) MarkRunSLABreached(ctx context.Context, id string, errorMsg *string) error {
	return mapDBError(r.q.MarkPipelineRunSLABreached(ctx, dbstore.MarkPipelineRunSLABreachedParams{
		ErrorMessage: nullStrFromPtr(errorMsg),
		ID:           id,
	}))
}

// UpdateRunQueueStarted marks a queued run as admitted for execution.
func (r *PipelineRunRepo) UpdateRunQueueStarted(ctx context.Context, id string) error {
	return mapDBError(r.q.UpdatePipelineRunQueueStarted(ctx, id))
}

// UpdateRunStarted marks a pipeline run as started.
func (r *PipelineRunRepo) UpdateRunStarted(ctx context.Context, id string) error {
	return mapDBError(r.q.UpdatePipelineRunStarted(ctx, id))
}

// UpdateRunFinished marks a pipeline run as finished with a final status.
func (r *PipelineRunRepo) UpdateRunFinished(ctx context.Context, id string, status string, errorMsg *string) error {
	return mapDBError(r.q.UpdatePipelineRunFinished(ctx, dbstore.UpdatePipelineRunFinishedParams{
		Status:       status,
		ErrorMessage: nullStrFromPtr(errorMsg),
		ID:           id,
	}))
}

// CountActiveRuns returns the number of active (PENDING or RUNNING) runs for a pipeline.
func (r *PipelineRunRepo) CountActiveRuns(ctx context.Context, pipelineID string) (int64, error) {
	return r.q.CountActivePipelineRuns(ctx, pipelineID)
}

// CancelPendingRuns cancels all pending runs for a pipeline.
func (r *PipelineRunRepo) CancelPendingRuns(ctx context.Context, pipelineID string) (int64, error) {
	err := r.q.CancelPendingPipelineRuns(ctx, pipelineID)
	if err != nil {
		return 0, err
	}
	// CancelPendingPipelineRuns is :exec, so we don't get rows affected.
	// Return 0 as a best-effort count.
	return 0, nil
}

// CreateJobRun inserts a new pipeline job run.
func (r *PipelineRunRepo) CreateJobRun(ctx context.Context, jr *domain.PipelineJobRun) (*domain.PipelineJobRun, error) {
	row, err := r.q.CreatePipelineJobRun(ctx, dbstore.CreatePipelineJobRunParams{
		ID:           defaultString(jr.ID, newID()),
		RunID:        jr.RunID,
		JobID:        jr.JobID,
		JobName:      jr.JobName,
		Status:       jr.Status,
		RetryAttempt: int64(jr.RetryAttempt),
	})
	if err != nil {
		return nil, mapDBError(err)
	}
	return pipelineJobRunFromDB(row), nil
}

// GetJobRunByID returns a pipeline job run by its ID.
func (r *PipelineRunRepo) GetJobRunByID(ctx context.Context, id string) (*domain.PipelineJobRun, error) {
	row, err := r.q.GetPipelineJobRunByID(ctx, id)
	if err != nil {
		return nil, mapDBError(err)
	}
	return pipelineJobRunFromDB(row), nil
}

// ListJobRunsByRun returns all job runs for a pipeline run.
func (r *PipelineRunRepo) ListJobRunsByRun(ctx context.Context, runID string) ([]domain.PipelineJobRun, error) {
	rows, err := r.q.ListPipelineJobRunsByRun(ctx, runID)
	if err != nil {
		return nil, err
	}

	jobRuns := make([]domain.PipelineJobRun, 0, len(rows))
	for _, row := range rows {
		jobRuns = append(jobRuns, *pipelineJobRunFromDB(row))
	}
	return jobRuns, nil
}

// UpdateJobRunStatus updates the status and optional error message of a job run.
func (r *PipelineRunRepo) UpdateJobRunStatus(ctx context.Context, id string, status string, errorMsg *string) error {
	return mapDBError(r.q.UpdatePipelineJobRunStatus(ctx, dbstore.UpdatePipelineJobRunStatusParams{
		Status:       status,
		ErrorMessage: nullStrFromPtr(errorMsg),
		ID:           id,
	}))
}

// UpdateJobRunStarted marks a job run as started.
func (r *PipelineRunRepo) UpdateJobRunStarted(ctx context.Context, id string, effectiveComputeEndpointID *string, attemptCount int) error {
	return mapDBError(r.q.UpdatePipelineJobRunStarted(ctx, dbstore.UpdatePipelineJobRunStartedParams{
		EffectiveComputeEndpointID: nullStringPtr(effectiveComputeEndpointID),
		AttemptCount:               int64(attemptCount),
		ID:                         id,
	}))
}

// UpdateJobRunFinished marks a job run as finished with a final status.
func (r *PipelineRunRepo) UpdateJobRunFinished(ctx context.Context, id string, status string, errorMsg *string, lastErrorCode *string, attemptCount int) error {
	return mapDBError(r.q.UpdatePipelineJobRunFinished(ctx, dbstore.UpdatePipelineJobRunFinishedParams{
		Status:        status,
		ErrorMessage:  nullStrFromPtr(errorMsg),
		LastErrorCode: nullStrFromPtr(lastErrorCode),
		AttemptCount:  int64(attemptCount),
		ID:            id,
	}))
}

// CreateRunEvent inserts a durable pipeline run event.
func (r *PipelineRunRepo) CreateRunEvent(ctx context.Context, event *domain.PipelineRunEvent) (*domain.PipelineRunEvent, error) {
	metadataJSON, err := json.Marshal(event.Metadata)
	if err != nil {
		return nil, fmt.Errorf("marshal event metadata: %w", err)
	}
	row, err := r.q.CreatePipelineRunEvent(ctx, dbstore.CreatePipelineRunEventParams{
		ID:        defaultString(event.ID, newID()),
		RunID:     event.RunID,
		JobRunID:  nullStringPtr(event.JobRunID),
		EventType: event.EventType,
		Message:   nullStrFromPtr(event.Message),
		ErrorCode: nullStrFromPtr(event.ErrorCode),
		Metadata:  string(metadataJSON),
	})
	if err != nil {
		return nil, mapDBError(err)
	}
	return pipelineRunEventFromDB(row), nil
}

// ListRunEvents returns durable run events ordered oldest-first.
func (r *PipelineRunRepo) ListRunEvents(ctx context.Context, runID string, page domain.PageRequest) ([]domain.PipelineRunEvent, int64, error) {
	total, err := r.q.CountPipelineRunEvents(ctx, runID)
	if err != nil {
		return nil, 0, mapDBError(err)
	}
	rows, err := r.q.ListPipelineRunEvents(ctx, dbstore.ListPipelineRunEventsParams{
		RunID:  runID,
		Limit:  int64(page.Limit()),
		Offset: int64(page.Offset()),
	})
	if err != nil {
		return nil, 0, mapDBError(err)
	}
	events := make([]domain.PipelineRunEvent, 0, len(rows))
	for _, row := range rows {
		events = append(events, *pipelineRunEventFromDB(row))
	}
	return events, total, nil
}

// === Private mappers ===

func pipelineRunFromDB(row dbstore.PipelineRun) *domain.PipelineRun {
	createdAt, _ := time.Parse("2006-01-02 15:04:05", row.CreatedAt)

	var params map[string]string
	_ = json.Unmarshal([]byte(row.Parameters), &params)
	if params == nil {
		params = map[string]string{}
	}

	var startedAt *time.Time
	if row.StartedAt.Valid {
		t, _ := time.Parse("2006-01-02 15:04:05", row.StartedAt.String)
		startedAt = &t
	}

	var finishedAt *time.Time
	if row.FinishedAt.Valid {
		t, _ := time.Parse("2006-01-02 15:04:05", row.FinishedAt.String)
		finishedAt = &t
	}

	var errMsg *string
	if row.ErrorMessage.Valid {
		errMsg = &row.ErrorMessage.String
	}

	var gitHash *string
	if row.GitCommitHash.Valid {
		gitHash = &row.GitCommitHash.String
	}

	var queuedAt *time.Time
	if row.QueuedAt.Valid {
		t, _ := time.Parse("2006-01-02 15:04:05", row.QueuedAt.String)
		queuedAt = &t
	}

	var queueStartedAt *time.Time
	if row.QueueStartedAt.Valid {
		t, _ := time.Parse("2006-01-02 15:04:05", row.QueueStartedAt.String)
		queueStartedAt = &t
	}

	var repairedFromRunID *string
	if row.RepairedFromRunID.Valid {
		repairedFromRunID = &row.RepairedFromRunID.String
	}

	var provenance *domain.PipelineRunProvenance
	if row.Provenance != "" && row.Provenance != "null" {
		var parsed domain.PipelineRunProvenance
		if err := json.Unmarshal([]byte(row.Provenance), &parsed); err == nil {
			provenance = &parsed
		}
	}

	var slaBreachedAt *time.Time
	if row.SlaBreachedAt.Valid {
		t, _ := time.Parse("2006-01-02 15:04:05", row.SlaBreachedAt.String)
		slaBreachedAt = &t
	}

	return &domain.PipelineRun{
		ID:                 row.ID,
		PipelineID:         row.PipelineID,
		Status:             row.Status,
		TriggerType:        row.TriggerType,
		TriggeredBy:        row.TriggeredBy,
		EffectivePrincipal: defaultString(row.EffectivePrincipal, row.TriggeredBy),
		Parameters:         params,
		GitCommitHash:      gitHash,
		QueuedAt:           queuedAt,
		QueueStartedAt:     queueStartedAt,
		StartedAt:          startedAt,
		FinishedAt:         finishedAt,
		ErrorMessage:       errMsg,
		RepairedFromRunID:  repairedFromRunID,
		Provenance:         provenance,
		SLABreachedAt:      slaBreachedAt,
		CreatedAt:          createdAt,
	}
}

func pipelineJobRunFromDB(row dbstore.PipelineJobRun) *domain.PipelineJobRun {
	createdAt, _ := time.Parse("2006-01-02 15:04:05", row.CreatedAt)

	var startedAt *time.Time
	if row.StartedAt.Valid {
		t, _ := time.Parse("2006-01-02 15:04:05", row.StartedAt.String)
		startedAt = &t
	}

	var finishedAt *time.Time
	if row.FinishedAt.Valid {
		t, _ := time.Parse("2006-01-02 15:04:05", row.FinishedAt.String)
		finishedAt = &t
	}

	var errMsg *string
	if row.ErrorMessage.Valid {
		errMsg = &row.ErrorMessage.String
	}

	var effectiveComputeEndpointID *string
	if row.EffectiveComputeEndpointID.Valid {
		effectiveComputeEndpointID = &row.EffectiveComputeEndpointID.String
	}

	var lastErrorCode *string
	if row.LastErrorCode.Valid {
		lastErrorCode = &row.LastErrorCode.String
	}

	return &domain.PipelineJobRun{
		ID:                         row.ID,
		RunID:                      row.RunID,
		JobID:                      row.JobID,
		JobName:                    row.JobName,
		Status:                     row.Status,
		StartedAt:                  startedAt,
		FinishedAt:                 finishedAt,
		ErrorMessage:               errMsg,
		RetryAttempt:               int(row.RetryAttempt),
		EffectiveComputeEndpointID: effectiveComputeEndpointID,
		AttemptCount:               int(row.AttemptCount),
		LastErrorCode:              lastErrorCode,
		CreatedAt:                  createdAt,
	}
}

func pipelineRunEventFromDB(row dbstore.PipelineRunEvent) *domain.PipelineRunEvent {
	createdAt, _ := time.Parse("2006-01-02 15:04:05", row.CreatedAt)

	var jobRunID *string
	if row.JobRunID.Valid {
		jobRunID = &row.JobRunID.String
	}
	var message *string
	if row.Message.Valid {
		message = &row.Message.String
	}
	var errorCode *string
	if row.ErrorCode.Valid {
		errorCode = &row.ErrorCode.String
	}
	metadata := map[string]any{}
	if row.Metadata != "" {
		_ = json.Unmarshal([]byte(row.Metadata), &metadata)
	}
	if metadata == nil {
		metadata = map[string]any{}
	}
	return &domain.PipelineRunEvent{
		ID:        row.ID,
		RunID:     row.RunID,
		JobRunID:  jobRunID,
		EventType: row.EventType,
		Message:   message,
		ErrorCode: errorCode,
		Metadata:  metadata,
		CreatedAt: createdAt,
	}
}

func nullStrFromPtr(s *string) sql.NullString {
	if s == nil {
		return sql.NullString{}
	}
	return sql.NullString{String: *s, Valid: true}
}

func nullTimeString(t *time.Time) sql.NullString {
	if t == nil {
		return sql.NullString{}
	}
	return sql.NullString{String: t.UTC().Format("2006-01-02 15:04:05"), Valid: true}
}
