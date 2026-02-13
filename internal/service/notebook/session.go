package notebook

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"sync"
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
	lastUsed   time.Time
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
	s := &session{
		id:         domain.NewID(),
		notebookID: notebookID,
		principal:  principal,
		conn:       conn,
		createdAt:  now,
		lastUsed:   now,
	}

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

// CloseSession closes a session and releases the DuckDB connection.
func (m *SessionManager) CloseSession(ctx context.Context, sessionID string) error {
	m.mu.Lock()
	s, ok := m.sessions[sessionID]
	if !ok {
		m.mu.Unlock()
		return domain.ErrNotFound("session %s not found", sessionID)
	}
	delete(m.sessions, sessionID)
	m.mu.Unlock()

	return s.conn.Close()
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
func (m *SessionManager) ExecuteCell(ctx context.Context, sessionID, cellID string) (*domain.CellExecutionResult, error) {
	s, err := m.getSession(sessionID)
	if err != nil {
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
	s.lastUsed = time.Now()

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
		// Cache the error result
		resultJSON, _ := json.Marshal(result)
		resultStr := string(resultJSON)
		_ = m.repo.UpdateCellResult(ctx, cellID, &resultStr)
		return result, nil
	}
	defer rows.Close()

	cols, data, scanErr := scanRows(rows)
	if scanErr != nil {
		errMsg := scanErr.Error()
		result.Error = &errMsg
		return result, nil
	}

	result.Columns = cols
	result.Rows = data
	result.RowCount = len(data)

	// Cache result
	resultJSON, _ := json.Marshal(result)
	resultStr := string(resultJSON)
	_ = m.repo.UpdateCellResult(ctx, cellID, &resultStr)

	return result, nil
}

// RunAll executes all SQL cells in a notebook sequentially.
func (m *SessionManager) RunAll(ctx context.Context, sessionID string) (*domain.RunAllResult, error) {
	s, err := m.getSession(sessionID)
	if err != nil {
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
		cellResult, err := m.ExecuteCell(ctx, sessionID, cell.ID)
		if err != nil {
			return nil, fmt.Errorf("execute cell %s: %w", cell.ID, err)
		}
		results = append(results, *cellResult)
		if cellResult.Error != nil {
			break // Stop on first error
		}
	}

	return &domain.RunAllResult{
		NotebookID:    s.notebookID,
		Results:       results,
		TotalDuration: time.Since(start),
	}, nil
}

// RunAllAsync starts an async execution of all cells and returns a job.
func (m *SessionManager) RunAllAsync(ctx context.Context, sessionID string) (*domain.NotebookJob, error) {
	s, err := m.getSession(sessionID)
	if err != nil {
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

	// Launch async execution
	go func() {
		bgCtx := context.Background()
		_ = m.jobRepo.UpdateJobState(bgCtx, job.ID, domain.JobStateRunning, nil, nil)

		result, execErr := m.RunAll(bgCtx, sessionID)

		if execErr != nil {
			errStr := execErr.Error()
			_ = m.jobRepo.UpdateJobState(bgCtx, job.ID, domain.JobStateFailed, nil, &errStr)
			return
		}

		resultJSON, _ := json.Marshal(result)
		resultStr := string(resultJSON)
		_ = m.jobRepo.UpdateJobState(bgCtx, job.ID, domain.JobStateComplete, &resultStr, nil)
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
	m.mu.Lock()
	var stale []string
	cutoff := time.Now().Add(-m.ttl)
	for id, s := range m.sessions {
		if s.lastUsed.Before(cutoff) {
			stale = append(stale, id)
		}
	}
	for _, id := range stale {
		if s, ok := m.sessions[id]; ok {
			_ = s.conn.Close()
			delete(m.sessions, id)
		}
	}
	m.mu.Unlock()
}

// CloseAll closes all active sessions. Called on server shutdown.
func (m *SessionManager) CloseAll() {
	m.mu.Lock()
	defer m.mu.Unlock()
	for id, s := range m.sessions {
		_ = s.conn.Close()
		delete(m.sessions, id)
	}
}
