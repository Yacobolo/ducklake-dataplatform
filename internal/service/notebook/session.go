package notebook

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"duck-demo/internal/domain"
)

// session holds the runtime state for a single notebook session.
type session struct {
	id         string
	notebookID string
	principal  string
	conn       *sql.Conn
	mu         sync.Mutex
	createdAt  time.Time
	lastUsed   atomic.Value // stores time.Time
	ctx        context.Context
	cancel     context.CancelFunc
	closing    atomic.Bool
}

// getLastUsed returns the session's last-used time safely via atomic.Value.
func (s *session) getLastUsed() time.Time {
	if v := s.lastUsed.Load(); v != nil {
		return v.(time.Time)
	}
	return s.createdAt
}

// setLastUsed stores the session's last-used time safely via atomic.Value.
func (s *session) setLastUsed(t time.Time) {
	s.lastUsed.Store(t)
}

// SessionManager manages notebook sessions with pinned DuckDB connections.
type SessionManager struct {
	mu       sync.RWMutex
	sessions map[string]*session
	duckDB   *sql.DB
	engine   domain.SessionEngine
	repo     domain.NotebookRepository
	jobRepo  domain.NotebookJobRepository
	audit    domain.AuditRepository
	ttl      time.Duration
}

// NewSessionManager creates a new SessionManager.
func NewSessionManager(
	duckDB *sql.DB,
	engine domain.SessionEngine,
	repo domain.NotebookRepository,
	jobRepo domain.NotebookJobRepository,
	audit domain.AuditRepository,
) *SessionManager {
	return &SessionManager{
		sessions: make(map[string]*session),
		duckDB:   duckDB,
		engine:   engine,
		repo:     repo,
		jobRepo:  jobRepo,
		audit:    audit,
		ttl:      30 * time.Minute,
	}
}

// CreateSession creates a new session with a pinned DuckDB connection.
func (m *SessionManager) CreateSession(ctx context.Context, notebookID, principal string) (*domain.NotebookSession, error) {
	// Verify notebook exists
	if _, err := m.repo.GetNotebook(ctx, notebookID); err != nil {
		return nil, err
	}

	conn, err := m.duckDB.Conn(ctx)
	if err != nil {
		return nil, fmt.Errorf("pin duckdb connection: %w", err)
	}

	now := time.Now()
	sessCtx, sessCancel := context.WithCancel(context.Background())
	s := &session{
		id:         domain.NewID(),
		notebookID: notebookID,
		principal:  principal,
		conn:       conn,
		createdAt:  now,
		ctx:        sessCtx,
		cancel:     sessCancel,
	}
	s.setLastUsed(now)

	m.mu.Lock()
	m.sessions[s.id] = s
	m.mu.Unlock()

	_ = m.audit.Insert(ctx, &domain.AuditEntry{
		PrincipalName: principal,
		Action:        "CREATE_SESSION",
		Status:        "ALLOWED",
	})

	return &domain.NotebookSession{
		ID:         s.id,
		NotebookID: notebookID,
		Principal:  principal,
		State:      "active",
		CreatedAt:  now,
		LastUsedAt: now,
	}, nil
}

// checkPrincipal verifies that the caller matches the session owner.
// If principalName is empty, the check is skipped (backward compatible).
func checkPrincipal(s *session, principalName string) error {
	if principalName != "" && s.principal != principalName {
		return domain.ErrAccessDenied("session belongs to a different principal")
	}
	return nil
}

// CloseSession closes a session and releases the DuckDB connection.
// If principalName is non-empty, the caller must match the session owner.
func (m *SessionManager) CloseSession(_ context.Context, sessionID string, principalName ...string) error {
	caller := ""
	if len(principalName) > 0 {
		caller = principalName[0]
	}

	m.mu.Lock()
	s, ok := m.sessions[sessionID]
	if !ok {
		m.mu.Unlock()
		return domain.ErrNotFound("session %s not found", sessionID)
	}

	if err := checkPrincipal(s, caller); err != nil {
		m.mu.Unlock()
		return err
	}

	delete(m.sessions, sessionID)
	m.mu.Unlock()

	// Cancel the session context to stop any in-flight async work.
	s.cancel()
	s.closing.Store(true)

	if err := s.conn.Close(); err != nil {
		return err
	}

	auditPrincipal := caller
	if auditPrincipal == "" {
		auditPrincipal = s.principal
	}
	_ = m.audit.Insert(context.Background(), &domain.AuditEntry{
		PrincipalName: auditPrincipal,
		Action:        "CLOSE_SESSION",
		Status:        "ALLOWED",
	})

	return nil
}

func (m *SessionManager) persistCellResult(ctx context.Context, cellID string, result *domain.CellExecutionResult) error {
	resultJSON, err := json.Marshal(result)
	if err != nil {
		return fmt.Errorf("marshal cell result: %w", err)
	}

	resultStr := string(resultJSON)
	if err := m.repo.UpdateCellResult(ctx, cellID, &resultStr); err != nil {
		return fmt.Errorf("update cached cell result: %w", err)
	}

	return nil
}

// getSession retrieves a session (caller must hold no locks).
func (m *SessionManager) getSession(sessionID string) (*session, error) {
	m.mu.RLock()
	s, ok := m.sessions[sessionID]
	m.mu.RUnlock()
	if !ok {
		return nil, domain.ErrNotFound("session %s not found", sessionID)
	}
	return s, nil
}

// scanRows materializes sql.Rows into columns + data rows.
func scanRows(rows *sql.Rows) ([]string, [][]interface{}, error) {
	cols, err := rows.Columns()
	if err != nil {
		return nil, nil, err
	}
	var data [][]interface{}
	for rows.Next() {
		values := make([]interface{}, len(cols))
		ptrs := make([]interface{}, len(cols))
		for i := range values {
			ptrs[i] = &values[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return nil, nil, err
		}
		// Convert byte slices to strings for JSON serialization
		row := make([]interface{}, len(values))
		for i, v := range values {
			if b, ok := v.([]byte); ok {
				row[i] = string(b)
			} else {
				row[i] = v
			}
		}
		data = append(data, row)
	}
	if err := rows.Err(); err != nil {
		return nil, nil, err
	}
	return cols, data, nil
}

// ExecuteCell executes a single cell's SQL on the pinned connection.
// If principalName is non-empty, the caller must match the session owner.
func (m *SessionManager) ExecuteCell(ctx context.Context, sessionID, cellID string, principalName ...string) (*domain.CellExecutionResult, error) {
	caller := ""
	if len(principalName) > 0 {
		caller = principalName[0]
	}

	s, err := m.getSession(sessionID)
	if err != nil {
		return nil, err
	}

	if err := checkPrincipal(s, caller); err != nil {
		return nil, err
	}

	cell, err := m.repo.GetCell(ctx, cellID)
	if err != nil {
		return nil, err
	}

	if cell.CellType != domain.CellTypeSQL {
		return nil, domain.ErrValidation("cannot execute non-SQL cell (type: %s)", string(cell.CellType))
	}

	// Serialize execution per session
	s.mu.Lock()
	defer s.mu.Unlock()

	// Check if the session is being closed/reaped (Issue #54).
	if s.closing.Load() {
		return nil, domain.ErrNotFound("session %s is closing", sessionID)
	}

	s.setLastUsed(time.Now())

	start := time.Now()
	rows, err := m.engine.QueryOnConn(ctx, s.conn, s.principal, cell.Content)
	duration := time.Since(start)

	result := &domain.CellExecutionResult{
		CellID:   cellID,
		Duration: duration,
	}

	if err != nil {
		errMsg := err.Error()
		result.Error = &errMsg
		if cacheErr := m.persistCellResult(ctx, cellID, result); cacheErr != nil {
			return nil, cacheErr
		}
		return result, nil
	}
	defer func() { _ = rows.Close() }()

	cols, data, scanErr := scanRows(rows)
	if scanErr != nil {
		errMsg := scanErr.Error()
		result.Error = &errMsg
		return result, nil
	}

	result.Columns = cols
	result.Rows = data
	result.RowCount = len(data)

	if err := m.persistCellResult(ctx, cellID, result); err != nil {
		return nil, err
	}

	return result, nil
}

// RunAll executes all SQL cells in a notebook sequentially.
// If principalName is non-empty, the caller must match the session owner.
func (m *SessionManager) RunAll(ctx context.Context, sessionID string, principalName ...string) (*domain.RunAllResult, error) {
	caller := ""
	if len(principalName) > 0 {
		caller = principalName[0]
	}

	s, err := m.getSession(sessionID)
	if err != nil {
		return nil, err
	}

	if err := checkPrincipal(s, caller); err != nil {
		return nil, err
	}

	cells, err := m.repo.ListCells(ctx, s.notebookID)
	if err != nil {
		return nil, fmt.Errorf("list cells: %w", err)
	}

	start := time.Now()
	var results []domain.CellExecutionResult

	for _, cell := range cells {
		if cell.CellType != domain.CellTypeSQL {
			continue
		}

		// Check for context cancellation between cells.
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		cellResult, err := m.ExecuteCell(ctx, sessionID, cell.ID)
		if err != nil {
			return nil, fmt.Errorf("execute cell %s: %w", cell.ID, err)
		}
		results = append(results, *cellResult)
		if cellResult.Error != nil {
			// If the context was cancelled, propagate it as a real error
			// so that callers (e.g. RunAllAsync) know execution was interrupted.
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			break // Stop on first SQL error
		}
	}

	return &domain.RunAllResult{
		NotebookID:    s.notebookID,
		Results:       results,
		TotalDuration: time.Since(start),
	}, nil
}

// RunAllAsync starts an async execution of all cells and returns a job.
// If principalName is non-empty, the caller must match the session owner.
func (m *SessionManager) RunAllAsync(ctx context.Context, sessionID string, principalName ...string) (*domain.NotebookJob, error) {
	caller := ""
	if len(principalName) > 0 {
		caller = principalName[0]
	}

	s, err := m.getSession(sessionID)
	if err != nil {
		return nil, err
	}

	if err := checkPrincipal(s, caller); err != nil {
		return nil, err
	}

	job := &domain.NotebookJob{
		ID:         domain.NewID(),
		NotebookID: s.notebookID,
		SessionID:  sessionID,
		State:      domain.JobStatePending,
	}

	job, err = m.jobRepo.CreateJob(ctx, job)
	if err != nil {
		return nil, fmt.Errorf("create job: %w", err)
	}

	// Launch async execution using the session's cancellable context
	// instead of context.Background() so that CloseSession/CloseAll
	// can stop the goroutine.
	go func() {
		sessCtx := s.ctx
		_ = m.jobRepo.UpdateJobState(sessCtx, job.ID, domain.JobStateRunning, nil, nil)

		result, execErr := m.RunAll(sessCtx, sessionID)

		if execErr != nil {
			errStr := execErr.Error()
			// Use a fresh background context for the final status update
			// in case the session context was cancelled.
			_ = m.jobRepo.UpdateJobState(context.Background(), job.ID, domain.JobStateFailed, nil, &errStr)
			return
		}

		resultJSON, err := json.Marshal(result)
		if err != nil {
			errStr := fmt.Sprintf("marshal async run result: %v", err)
			_ = m.jobRepo.UpdateJobState(context.Background(), job.ID, domain.JobStateFailed, nil, &errStr)
			return
		}
		resultStr := string(resultJSON)
		_ = m.jobRepo.UpdateJobState(context.Background(), job.ID, domain.JobStateComplete, &resultStr, nil)
	}()

	return job, nil
}

// GetJob returns a notebook job by ID.
func (m *SessionManager) GetJob(ctx context.Context, jobID string) (*domain.NotebookJob, error) {
	return m.jobRepo.GetJob(ctx, jobID)
}

// ListJobs lists jobs for a notebook.
func (m *SessionManager) ListJobs(ctx context.Context, notebookID string, page domain.PageRequest) ([]domain.NotebookJob, int64, error) {
	return m.jobRepo.ListJobs(ctx, notebookID, page)
}

// ReapIdle closes sessions that have been idle longer than the TTL.
// Should be called in a background goroutine.
func (m *SessionManager) ReapIdle(ctx context.Context) {
	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			m.reapOnce()
		}
	}
}

func (m *SessionManager) reapOnce() {
	// Collect stale sessions under the lock, but close connections after
	// releasing the lock to avoid holding m.mu while doing I/O (Issue #54).
	m.mu.Lock()
	var stale []*session
	cutoff := time.Now().Add(-m.ttl)
	for id, s := range m.sessions {
		if s.getLastUsed().Before(cutoff) {
			s.closing.Store(true)
			stale = append(stale, s)
			delete(m.sessions, id)
		}
	}
	m.mu.Unlock()

	// Close connections outside the lock.
	for _, s := range stale {
		s.cancel()
		_ = s.conn.Close()
	}
}

// CloseAll closes all active sessions. Called on server shutdown.
func (m *SessionManager) CloseAll() {
	m.mu.Lock()
	all := make([]*session, 0, len(m.sessions))
	for id, s := range m.sessions {
		s.closing.Store(true)
		all = append(all, s)
		delete(m.sessions, id)
	}
	m.mu.Unlock()

	for _, s := range all {
		s.cancel()
		_ = s.conn.Close()
	}
}
