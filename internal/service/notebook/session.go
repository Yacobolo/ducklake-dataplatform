package notebook

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
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
	mu             sync.RWMutex
	sessions       map[string]*session
	duckDB         *sql.DB
	engine         domain.SessionEngine
	repo           domain.NotebookRepository
	folders        domain.FolderRepository
	folderShares   domain.FolderShareRepository
	auth           domain.AuthorizationService
	notebookShares domain.NotebookShareRepository
	jobRepo        domain.NotebookJobRepository
	audit          domain.AuditRepository
	ttl            time.Duration
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

// SetAuthorization configures folder privilege checks for session access.
func (m *SessionManager) SetAuthorization(auth domain.AuthorizationService) {
	m.auth = auth
}

// SetAccessRepositories configures folder and share repositories for session authorization.
func (m *SessionManager) SetAccessRepositories(
	folders domain.FolderRepository,
	folderShares domain.FolderShareRepository,
	notebookShares domain.NotebookShareRepository,
) {
	m.folders = folders
	m.folderShares = folderShares
	m.notebookShares = notebookShares
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

func (m *SessionManager) accessResolver(ctx context.Context, principal string, isAdmin bool) (*principalAccessResolver, error) {
	return newPrincipalAccessResolver(ctx, m.folders, m.folderShares, m.auth, m.notebookShares, principal, isAdmin)
}

func (m *SessionManager) requireNotebookRole(ctx context.Context, notebookID, principal string, isAdmin bool, allowed func(string) bool, action string) (*domain.Notebook, error) {
	nb, err := m.repo.GetNotebook(ctx, notebookID)
	if err != nil {
		return nil, err
	}
	resolver, err := m.accessResolver(ctx, principal, isAdmin)
	if err != nil {
		return nil, fmt.Errorf("resolve notebook access: %w", err)
	}
	role, err := resolver.notebookRole(ctx, nb)
	if err != nil {
		return nil, fmt.Errorf("resolve notebook access: %w", err)
	}
	if !allowed(role) {
		return nil, domain.ErrAccessDenied("principal %q cannot %s notebook %q", principal, action, notebookID)
	}
	return nb, nil
}

func (m *SessionManager) requireNotebookReadAccess(ctx context.Context, notebookID, principal string, isAdmin bool) (*domain.Notebook, error) {
	return m.requireNotebookRole(ctx, notebookID, principal, isAdmin, roleAllowsRead, "read")
}

func (m *SessionManager) requireNotebookAccess(ctx context.Context, notebookID, principal string, isAdmin bool) (*domain.Notebook, error) {
	return m.requireNotebookReadAccess(ctx, notebookID, principal, isAdmin)
}

func (m *SessionManager) requireNotebookWriteAccess(ctx context.Context, notebookID, principal string, isAdmin bool) (*domain.Notebook, error) {
	return m.requireNotebookRole(ctx, notebookID, principal, isAdmin, roleAllowsWrite, "execute")
}

func (m *SessionManager) requireSessionAccess(ctx context.Context, notebookID, sessionID, principal string, isAdmin bool) (*session, error) {
	s, err := m.getSession(sessionID)
	if err != nil {
		return nil, err
	}
	if s.notebookID != notebookID {
		return nil, domain.ErrNotFound("session %s not found", sessionID)
	}
	if !isAdmin {
		if err := checkPrincipal(s, principal); err != nil {
			return nil, err
		}
	}
	if _, err := m.requireNotebookWriteAccess(ctx, notebookID, principal, isAdmin); err != nil {
		return nil, err
	}
	return s, nil
}

func (m *SessionManager) requireCellForNotebook(ctx context.Context, notebookID, cellID string) (*domain.Cell, error) {
	cell, err := m.repo.GetCell(ctx, cellID)
	if err != nil {
		return nil, err
	}
	if cell.NotebookID != notebookID {
		return nil, domain.ErrNotFound("cell %s not found", cellID)
	}
	return cell, nil
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

// CreateSessionForNotebook creates a session only when the caller can access the notebook.
func (m *SessionManager) CreateSessionForNotebook(ctx context.Context, notebookID, principal string, isAdmin bool) (*domain.NotebookSession, error) {
	if _, err := m.requireNotebookWriteAccess(ctx, notebookID, principal, isAdmin); err != nil {
		return nil, err
	}
	return m.CreateSession(ctx, notebookID, principal)
}

// CloseNotebookSession closes a session after validating the notebook/session relationship.
func (m *SessionManager) CloseNotebookSession(ctx context.Context, notebookID, sessionID, principal string, isAdmin bool) error {
	if _, err := m.requireSessionAccess(ctx, notebookID, sessionID, principal, isAdmin); err != nil {
		return err
	}
	if isAdmin {
		return m.CloseSession(ctx, sessionID)
	}
	return m.CloseSession(ctx, sessionID, principal)
}

// InvalidateNotebook closes all active sessions for a notebook after its effective context changes.
func (m *SessionManager) InvalidateNotebook(ctx context.Context, notebookID string) error {
	m.mu.RLock()
	sessionIDs := make([]string, 0)
	for id, sess := range m.sessions {
		if sess.notebookID == notebookID {
			sessionIDs = append(sessionIDs, id)
		}
	}
	m.mu.RUnlock()

	var errs []error
	for _, sessionID := range sessionIDs {
		if err := m.CloseSession(ctx, sessionID); err != nil {
			var notFound *domain.NotFoundError
			if errors.As(err, &notFound) {
				continue
			}
			errs = append(errs, fmt.Errorf("close session %s: %w", sessionID, err))
		}
	}
	return errors.Join(errs...)
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
	if cell.Disabled {
		return nil, domain.ErrValidation("cannot execute disabled cell")
	}
	cells, err := m.repo.ListCells(ctx, cell.NotebookID)
	if err != nil {
		return nil, fmt.Errorf("list cells for compile: %w", err)
	}
	if needsNotebookCompile(cell, cells) {
		compiled, err := CompileNotebookCellSQL(cells, cellID, false)
		if err != nil {
			if !errors.As(err, new(*domain.NotFoundError)) {
				return nil, err
			}
			compiled = cell.Content
		}
		cell.Content = compiled
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
	finishedAt := time.Now()
	duration := finishedAt.Sub(start)

	result := &domain.CellExecutionResult{
		CellID:     cellID,
		Duration:   duration,
		ExecutedAt: &finishedAt,
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

	if cell.Role == domain.CellRoleTest {
		severity := domain.NotebookTestSeverityError
		if cell.Test != nil && cell.Test.Severity != "" {
			severity = cell.Test.Severity
		}
		if len(data) > 0 && severity == domain.NotebookTestSeverityError {
			errMsg := fmt.Sprintf("test cell failed: expected zero rows, got %d", len(data))
			result.Error = &errMsg
		}
	}

	if err := m.persistCellResult(ctx, cellID, result); err != nil {
		return nil, err
	}

	return result, nil
}

// ExecuteNotebookCell executes a cell after validating notebook/session/cell relationships.
func (m *SessionManager) ExecuteNotebookCell(ctx context.Context, notebookID, sessionID, cellID, principal string, isAdmin bool) (*domain.CellExecutionResult, error) {
	s, err := m.requireSessionAccess(ctx, notebookID, sessionID, principal, isAdmin)
	if err != nil {
		return nil, err
	}
	cell, err := m.requireCellForNotebook(ctx, notebookID, cellID)
	if err != nil {
		return nil, err
	}
	if cell.NotebookID != s.notebookID {
		return nil, domain.ErrNotFound("cell %s not found", cellID)
	}
	if isAdmin {
		return m.ExecuteCell(ctx, sessionID, cellID)
	}
	return m.ExecuteCell(ctx, sessionID, cellID, principal)
}

func needsNotebookCompile(cell *domain.Cell, cells []domain.Cell) bool {
	if cell.Role == domain.CellRoleOutput {
		return true
	}
	if strings.Contains(cell.Content, "{{") || strings.Contains(cell.Content, "{%") {
		return true
	}
	for _, c := range cells {
		if c.CellType != domain.CellTypeSQL || c.Disabled || c.Name == nil || *c.Name == "" {
			continue
		}
		return true
	}
	return false
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
		if cell.CellType != domain.CellTypeSQL || cell.Disabled {
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

// RunAllNotebook executes all notebook cells only after validating notebook/session ownership.
func (m *SessionManager) RunAllNotebook(ctx context.Context, notebookID, sessionID, principal string, isAdmin bool) (*domain.RunAllResult, error) {
	if _, err := m.requireSessionAccess(ctx, notebookID, sessionID, principal, isAdmin); err != nil {
		return nil, err
	}
	if isAdmin {
		return m.RunAll(ctx, sessionID)
	}
	return m.RunAll(ctx, sessionID, principal)
}

// RunAllNotebookAsync starts async notebook execution only after validating notebook/session ownership.
func (m *SessionManager) RunAllNotebookAsync(ctx context.Context, notebookID, sessionID, principal string, isAdmin bool) (*domain.NotebookJob, error) {
	if _, err := m.requireSessionAccess(ctx, notebookID, sessionID, principal, isAdmin); err != nil {
		return nil, err
	}
	if isAdmin {
		return m.RunAllAsync(ctx, sessionID)
	}
	return m.RunAllAsync(ctx, sessionID, principal)
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

	auditPrincipal := caller
	if auditPrincipal == "" {
		auditPrincipal = s.principal
	}
	_ = m.audit.Insert(ctx, &domain.AuditEntry{
		PrincipalName: auditPrincipal,
		Action:        "RUN_ALL_ASYNC",
		Status:        "ALLOWED",
	})

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

		if errMsg := runAllFailureMessage(result); errMsg != nil {
			_ = m.jobRepo.UpdateJobState(context.Background(), job.ID, domain.JobStateFailed, nil, errMsg)
			return
		}

		resultStr := string(resultJSON)
		if runAllResultHasErrors(result) {
			errStr := firstRunAllError(result)
			_ = m.jobRepo.UpdateJobState(context.Background(), job.ID, domain.JobStateFailed, &resultStr, &errStr)
			return
		}
		_ = m.jobRepo.UpdateJobState(context.Background(), job.ID, domain.JobStateComplete, &resultStr, nil)
	}()

	return job, nil
}

func runAllResultHasErrors(result *domain.RunAllResult) bool {
	for i := range result.Results {
		if result.Results[i].Error != nil {
			return true
		}
	}
	return false
}

func firstRunAllError(result *domain.RunAllResult) string {
	for i := range result.Results {
		if result.Results[i].Error != nil {
			return *result.Results[i].Error
		}
	}
	return "notebook execution failed"
}

func runAllFailureMessage(result *domain.RunAllResult) *string {
	if result == nil {
		return nil
	}
	for _, cellResult := range result.Results {
		if cellResult.Error != nil && *cellResult.Error != "" {
			return cellResult.Error
		}
	}
	return nil
}

// GetJob returns a notebook job by ID.
func (m *SessionManager) GetJob(ctx context.Context, jobID string) (*domain.NotebookJob, error) {
	return m.jobRepo.GetJob(ctx, jobID)
}

// GetNotebookJob returns a notebook job after validating its parent notebook and caller access.
func (m *SessionManager) GetNotebookJob(ctx context.Context, notebookID, jobID, principal string, isAdmin bool) (*domain.NotebookJob, error) {
	if _, err := m.requireNotebookAccess(ctx, notebookID, principal, isAdmin); err != nil {
		return nil, err
	}
	job, err := m.jobRepo.GetJob(ctx, jobID)
	if err != nil {
		return nil, err
	}
	if job.NotebookID != notebookID {
		return nil, domain.ErrNotFound("job %s not found", jobID)
	}
	return job, nil
}

// ListJobs lists jobs for a notebook.
func (m *SessionManager) ListJobs(ctx context.Context, notebookID string, page domain.PageRequest) ([]domain.NotebookJob, int64, error) {
	return m.jobRepo.ListJobs(ctx, notebookID, page)
}

// ListNotebookJobs lists jobs for a notebook only when the caller can access that notebook.
func (m *SessionManager) ListNotebookJobs(ctx context.Context, notebookID, principal string, isAdmin bool, page domain.PageRequest) ([]domain.NotebookJob, int64, error) {
	if _, err := m.requireNotebookAccess(ctx, notebookID, principal, isAdmin); err != nil {
		return nil, 0, err
	}
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
