package api

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"duck-demo/internal/domain"
)

// === Mock services ===

// mockNotebookService implements notebookService using function fields.
type mockNotebookService struct {
	createNotebookFn func(ctx context.Context, principal string, req domain.CreateNotebookRequest) (*domain.Notebook, error)
	getNotebookFn    func(ctx context.Context, id string) (*domain.Notebook, []domain.Cell, error)
	listNotebooksFn  func(ctx context.Context, owner *string, page domain.PageRequest) ([]domain.Notebook, int64, error)
	updateNotebookFn func(ctx context.Context, principal string, isAdmin bool, id string, req domain.UpdateNotebookRequest) (*domain.Notebook, error)
	deleteNotebookFn func(ctx context.Context, principal string, isAdmin bool, id string) error
	createCellFn     func(ctx context.Context, principal string, isAdmin bool, notebookID string, req domain.CreateCellRequest) (*domain.Cell, error)
	updateCellFn     func(ctx context.Context, principal string, isAdmin bool, cellID string, req domain.UpdateCellRequest) (*domain.Cell, error)
	deleteCellFn     func(ctx context.Context, principal string, isAdmin bool, cellID string) error
	reorderCellsFn   func(ctx context.Context, principal string, isAdmin bool, notebookID string, req domain.ReorderCellsRequest) ([]domain.Cell, error)
}

func (m *mockNotebookService) CreateNotebook(ctx context.Context, principal string, req domain.CreateNotebookRequest) (*domain.Notebook, error) {
	if m.createNotebookFn != nil {
		return m.createNotebookFn(ctx, principal, req)
	}
	panic("CreateNotebook not implemented")
}
func (m *mockNotebookService) GetNotebook(ctx context.Context, id string) (*domain.Notebook, []domain.Cell, error) {
	if m.getNotebookFn != nil {
		return m.getNotebookFn(ctx, id)
	}
	panic("GetNotebook not implemented")
}
func (m *mockNotebookService) ListNotebooks(ctx context.Context, owner *string, page domain.PageRequest) ([]domain.Notebook, int64, error) {
	if m.listNotebooksFn != nil {
		return m.listNotebooksFn(ctx, owner, page)
	}
	panic("ListNotebooks not implemented")
}
func (m *mockNotebookService) UpdateNotebook(ctx context.Context, principal string, isAdmin bool, id string, req domain.UpdateNotebookRequest) (*domain.Notebook, error) {
	if m.updateNotebookFn != nil {
		return m.updateNotebookFn(ctx, principal, isAdmin, id, req)
	}
	panic("UpdateNotebook not implemented")
}
func (m *mockNotebookService) DeleteNotebook(ctx context.Context, principal string, isAdmin bool, id string) error {
	if m.deleteNotebookFn != nil {
		return m.deleteNotebookFn(ctx, principal, isAdmin, id)
	}
	panic("DeleteNotebook not implemented")
}
func (m *mockNotebookService) CreateCell(ctx context.Context, principal string, isAdmin bool, notebookID string, req domain.CreateCellRequest) (*domain.Cell, error) {
	if m.createCellFn != nil {
		return m.createCellFn(ctx, principal, isAdmin, notebookID, req)
	}
	panic("CreateCell not implemented")
}
func (m *mockNotebookService) UpdateCell(ctx context.Context, principal string, isAdmin bool, cellID string, req domain.UpdateCellRequest) (*domain.Cell, error) {
	if m.updateCellFn != nil {
		return m.updateCellFn(ctx, principal, isAdmin, cellID, req)
	}
	panic("UpdateCell not implemented")
}
func (m *mockNotebookService) DeleteCell(ctx context.Context, principal string, isAdmin bool, cellID string) error {
	if m.deleteCellFn != nil {
		return m.deleteCellFn(ctx, principal, isAdmin, cellID)
	}
	panic("DeleteCell not implemented")
}
func (m *mockNotebookService) ReorderCells(ctx context.Context, principal string, isAdmin bool, notebookID string, req domain.ReorderCellsRequest) ([]domain.Cell, error) {
	if m.reorderCellsFn != nil {
		return m.reorderCellsFn(ctx, principal, isAdmin, notebookID, req)
	}
	panic("ReorderCells not implemented")
}

// mockSessionService implements sessionService using function fields.
type mockSessionService struct {
	createSessionFn func(ctx context.Context, notebookID, principal string) (*domain.NotebookSession, error)
	closeSessionFn  func(ctx context.Context, sessionID string) error
	executeCellFn   func(ctx context.Context, sessionID, cellID string) (*domain.CellExecutionResult, error)
	runAllFn        func(ctx context.Context, sessionID string) (*domain.RunAllResult, error)
	runAllAsyncFn   func(ctx context.Context, sessionID string) (*domain.NotebookJob, error)
	getJobFn        func(ctx context.Context, jobID string) (*domain.NotebookJob, error)
	listJobsFn      func(ctx context.Context, notebookID string, page domain.PageRequest) ([]domain.NotebookJob, int64, error)
}

func (m *mockSessionService) CreateSession(ctx context.Context, notebookID, principal string) (*domain.NotebookSession, error) {
	if m.createSessionFn != nil {
		return m.createSessionFn(ctx, notebookID, principal)
	}
	panic("CreateSession not implemented")
}
func (m *mockSessionService) CloseSession(ctx context.Context, sessionID string, principalName ...string) error {
	if m.closeSessionFn != nil {
		return m.closeSessionFn(ctx, sessionID)
	}
	panic("CloseSession not implemented")
}
func (m *mockSessionService) ExecuteCell(ctx context.Context, sessionID, cellID string, principalName ...string) (*domain.CellExecutionResult, error) {
	if m.executeCellFn != nil {
		return m.executeCellFn(ctx, sessionID, cellID)
	}
	panic("ExecuteCell not implemented")
}
func (m *mockSessionService) RunAll(ctx context.Context, sessionID string, principalName ...string) (*domain.RunAllResult, error) {
	if m.runAllFn != nil {
		return m.runAllFn(ctx, sessionID)
	}
	panic("RunAll not implemented")
}
func (m *mockSessionService) RunAllAsync(ctx context.Context, sessionID string, principalName ...string) (*domain.NotebookJob, error) {
	if m.runAllAsyncFn != nil {
		return m.runAllAsyncFn(ctx, sessionID)
	}
	panic("RunAllAsync not implemented")
}
func (m *mockSessionService) GetJob(ctx context.Context, jobID string) (*domain.NotebookJob, error) {
	if m.getJobFn != nil {
		return m.getJobFn(ctx, jobID)
	}
	panic("GetJob not implemented")
}
func (m *mockSessionService) ListJobs(ctx context.Context, notebookID string, page domain.PageRequest) ([]domain.NotebookJob, int64, error) {
	if m.listJobsFn != nil {
		return m.listJobsFn(ctx, notebookID, page)
	}
	panic("ListJobs not implemented")
}

// mockGitRepoService implements gitRepoService using function fields.
type mockGitRepoService struct {
	createGitRepoFn func(ctx context.Context, principal string, req domain.CreateGitRepoRequest) (*domain.GitRepo, error)
	getGitRepoFn    func(ctx context.Context, id string) (*domain.GitRepo, error)
	listGitReposFn  func(ctx context.Context, page domain.PageRequest) ([]domain.GitRepo, int64, error)
	deleteGitRepoFn func(ctx context.Context, principal string, isAdmin bool, id string) error
	syncGitRepoFn   func(ctx context.Context, id string) (*domain.GitSyncResult, error)
}

func (m *mockGitRepoService) CreateGitRepo(ctx context.Context, principal string, req domain.CreateGitRepoRequest) (*domain.GitRepo, error) {
	if m.createGitRepoFn != nil {
		return m.createGitRepoFn(ctx, principal, req)
	}
	panic("CreateGitRepo not implemented")
}
func (m *mockGitRepoService) GetGitRepo(ctx context.Context, id string) (*domain.GitRepo, error) {
	if m.getGitRepoFn != nil {
		return m.getGitRepoFn(ctx, id)
	}
	panic("GetGitRepo not implemented")
}
func (m *mockGitRepoService) ListGitRepos(ctx context.Context, page domain.PageRequest) ([]domain.GitRepo, int64, error) {
	if m.listGitReposFn != nil {
		return m.listGitReposFn(ctx, page)
	}
	panic("ListGitRepos not implemented")
}
func (m *mockGitRepoService) DeleteGitRepo(ctx context.Context, principal string, isAdmin bool, id string) error {
	if m.deleteGitRepoFn != nil {
		return m.deleteGitRepoFn(ctx, principal, isAdmin, id)
	}
	panic("DeleteGitRepo not implemented")
}
func (m *mockGitRepoService) SyncGitRepo(ctx context.Context, id string) (*domain.GitSyncResult, error) {
	if m.syncGitRepoFn != nil {
		return m.syncGitRepoFn(ctx, id)
	}
	panic("SyncGitRepo not implemented")
}

// === Test helpers ===

// setupNotebookTestServer creates a lightweight test server wired with mock notebook services.
func setupNotebookTestServer(t *testing.T, nb notebookService, sess sessionService, git gitRepoService, principalName string, isAdmin bool) *httptest.Server {
	t.Helper()

	handler := NewHandler(
		nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, // query..catalogRegistration
		nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, // queryHistory..computeEndpoints
		nil, // apiKeys
		nb, sess, git,
		nil, // pipelineSvc
		nil, // modelSvc
		nil, // macroSvc
	)
	strictHandler := NewStrictHandler(handler, nil)

	r := chi.NewRouter()
	r.Use(func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			ctx := domain.WithPrincipal(req.Context(), domain.ContextPrincipal{
				Name:    principalName,
				IsAdmin: isAdmin,
				Type:    "user",
			})
			next.ServeHTTP(w, req.WithContext(ctx))
		})
	})
	HandlerFromMux(strictHandler, r)

	return httptest.NewServer(r)
}

// nbDoRequest sends an HTTP request and returns the response. Body is optional JSON.
func nbDoRequest(t *testing.T, method, url string, body string) *http.Response {
	t.Helper()
	var reqBody *bytes.Buffer
	if body != "" {
		reqBody = bytes.NewBufferString(body)
	} else {
		reqBody = bytes.NewBuffer(nil)
	}
	req, err := http.NewRequestWithContext(context.Background(), method, url, reqBody)
	require.NoError(t, err)
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	return resp
}

// nbDecodeJSON decodes a JSON response body into the given type.
func nbDecodeJSON[T any](t *testing.T, resp *http.Response) T {
	t.Helper()
	defer resp.Body.Close() //nolint:errcheck
	var result T
	err := json.NewDecoder(resp.Body).Decode(&result)
	require.NoError(t, err)
	return result
}

// === Shared test fixtures ===

var (
	fixedTime  = time.Date(2025, 1, 15, 10, 0, 0, 0, time.UTC)
	fixedTime2 = time.Date(2025, 1, 15, 11, 0, 0, 0, time.UTC)
)

func strPtr(s string) *string { return &s }

// === Tests ===

func TestAPI_NotebookCRUD(t *testing.T) {
	now := fixedTime
	nbID := "nb-001"
	desc := "Test notebook description"

	notebookSvc := &mockNotebookService{
		createNotebookFn: func(_ context.Context, principal string, req domain.CreateNotebookRequest) (*domain.Notebook, error) {
			return &domain.Notebook{
				ID:          nbID,
				Name:        req.Name,
				Description: req.Description,
				Owner:       principal,
				CreatedAt:   now,
				UpdatedAt:   now,
			}, nil
		},
		getNotebookFn: func(_ context.Context, id string) (*domain.Notebook, []domain.Cell, error) {
			if id == nbID {
				return &domain.Notebook{
						ID:          nbID,
						Name:        "My Notebook",
						Description: &desc,
						Owner:       "alice",
						CreatedAt:   now,
						UpdatedAt:   now,
					}, []domain.Cell{
						{ID: "cell-001", NotebookID: nbID, CellType: domain.CellTypeSQL, Content: "SELECT 1", Position: 0, CreatedAt: now, UpdatedAt: now},
					}, nil
			}
			return nil, nil, domain.ErrNotFound("notebook %s not found", id)
		},
		listNotebooksFn: func(_ context.Context, _ *string, _ domain.PageRequest) ([]domain.Notebook, int64, error) {
			return []domain.Notebook{
				{ID: nbID, Name: "My Notebook", Owner: "alice", CreatedAt: now, UpdatedAt: now},
			}, 1, nil
		},
		updateNotebookFn: func(_ context.Context, _ string, _ bool, id string, req domain.UpdateNotebookRequest) (*domain.Notebook, error) {
			return &domain.Notebook{
				ID:          id,
				Name:        "Updated Notebook",
				Description: req.Description,
				Owner:       "alice",
				CreatedAt:   now,
				UpdatedAt:   fixedTime2,
			}, nil
		},
		deleteNotebookFn: func(_ context.Context, _ string, _ bool, _ string) error {
			return nil
		},
	}

	srv := setupNotebookTestServer(t, notebookSvc, nil, nil, "alice", false)
	defer srv.Close()

	t.Run("create notebook", func(t *testing.T) {
		body := `{"name":"My Notebook","description":"Test notebook description"}`
		resp := nbDoRequest(t, http.MethodPost, srv.URL+"/notebooks", body)
		require.Equal(t, http.StatusCreated, resp.StatusCode)

		nb := nbDecodeJSON[Notebook](t, resp)
		require.NotNil(t, nb.Id)
		assert.Equal(t, nbID, *nb.Id)
		assert.Equal(t, "My Notebook", *nb.Name)
		assert.Equal(t, "Test notebook description", *nb.Description)
		assert.Equal(t, "alice", *nb.Owner)
	})

	t.Run("get notebook with cells", func(t *testing.T) {
		resp := nbDoRequest(t, http.MethodGet, srv.URL+"/notebooks/"+nbID, "")
		require.Equal(t, http.StatusOK, resp.StatusCode)

		detail := nbDecodeJSON[NotebookDetail](t, resp)
		require.NotNil(t, detail.Notebook)
		assert.Equal(t, nbID, *detail.Notebook.Id)
		assert.Equal(t, "My Notebook", *detail.Notebook.Name)
		require.NotNil(t, detail.Cells)
		require.Len(t, *detail.Cells, 1)
		cell := (*detail.Cells)[0]
		assert.Equal(t, "cell-001", *cell.Id)
		assert.Equal(t, "SELECT 1", *cell.Content)
	})

	t.Run("list notebooks", func(t *testing.T) {
		resp := nbDoRequest(t, http.MethodGet, srv.URL+"/notebooks", "")
		require.Equal(t, http.StatusOK, resp.StatusCode)

		list := nbDecodeJSON[PaginatedNotebooks](t, resp)
		require.NotNil(t, list.Data)
		require.Len(t, *list.Data, 1)
		assert.Equal(t, nbID, *(*list.Data)[0].Id)
	})

	t.Run("update notebook", func(t *testing.T) {
		body := `{"name":"Updated Notebook","description":"new desc"}`
		resp := nbDoRequest(t, http.MethodPatch, srv.URL+"/notebooks/"+nbID, body)
		require.Equal(t, http.StatusOK, resp.StatusCode)

		nb := nbDecodeJSON[Notebook](t, resp)
		assert.Equal(t, "Updated Notebook", *nb.Name)
	})

	t.Run("delete notebook", func(t *testing.T) {
		resp := nbDoRequest(t, http.MethodDelete, srv.URL+"/notebooks/"+nbID, "")
		defer resp.Body.Close() //nolint:errcheck
		require.Equal(t, http.StatusNoContent, resp.StatusCode)
	})

	// After deletion, mock getNotebook to return not found
	notebookSvc.getNotebookFn = func(_ context.Context, id string) (*domain.Notebook, []domain.Cell, error) {
		return nil, nil, domain.ErrNotFound("notebook %s not found", id)
	}

	t.Run("get deleted notebook returns 404", func(t *testing.T) {
		resp := nbDoRequest(t, http.MethodGet, srv.URL+"/notebooks/"+nbID, "")
		defer resp.Body.Close() //nolint:errcheck
		require.Equal(t, http.StatusNotFound, resp.StatusCode)
	})
}

func TestAPI_CellCRUD(t *testing.T) {
	now := fixedTime
	nbID := "nb-001"
	cellID := "cell-100"

	notebookSvc := &mockNotebookService{
		createCellFn: func(_ context.Context, _ string, _ bool, notebookID string, req domain.CreateCellRequest) (*domain.Cell, error) {
			return &domain.Cell{
				ID:         cellID,
				NotebookID: notebookID,
				CellType:   req.CellType,
				Content:    req.Content,
				Position:   0,
				CreatedAt:  now,
				UpdatedAt:  now,
			}, nil
		},
		updateCellFn: func(_ context.Context, _ string, _ bool, id string, req domain.UpdateCellRequest) (*domain.Cell, error) {
			content := "SELECT 1"
			if req.Content != nil {
				content = *req.Content
			}
			return &domain.Cell{
				ID:         id,
				NotebookID: nbID,
				CellType:   domain.CellTypeSQL,
				Content:    content,
				Position:   0,
				CreatedAt:  now,
				UpdatedAt:  fixedTime2,
			}, nil
		},
		reorderCellsFn: func(_ context.Context, _ string, _ bool, _ string, req domain.ReorderCellsRequest) ([]domain.Cell, error) {
			cells := make([]domain.Cell, len(req.CellIDs))
			for i, id := range req.CellIDs {
				cells[i] = domain.Cell{
					ID:         id,
					NotebookID: nbID,
					CellType:   domain.CellTypeSQL,
					Content:    fmt.Sprintf("SELECT %d", i+1),
					Position:   i,
					CreatedAt:  now,
					UpdatedAt:  now,
				}
			}
			return cells, nil
		},
		deleteCellFn: func(_ context.Context, _ string, _ bool, _ string) error {
			return nil
		},
	}

	srv := setupNotebookTestServer(t, notebookSvc, nil, nil, "alice", false)
	defer srv.Close()

	t.Run("create cell", func(t *testing.T) {
		body := `{"cell_type":"sql","content":"SELECT 1"}`
		resp := nbDoRequest(t, http.MethodPost, srv.URL+"/notebooks/"+nbID+"/cells", body)
		require.Equal(t, http.StatusCreated, resp.StatusCode)

		cell := nbDecodeJSON[Cell](t, resp)
		require.NotNil(t, cell.Id)
		assert.Equal(t, cellID, *cell.Id)
		assert.Equal(t, nbID, *cell.NotebookId)
		assert.Equal(t, "SELECT 1", *cell.Content)
		assert.Equal(t, CellCellType("sql"), *cell.CellType)
	})

	t.Run("update cell", func(t *testing.T) {
		body := `{"content":"SELECT 2"}`
		resp := nbDoRequest(t, http.MethodPatch, srv.URL+"/notebooks/"+nbID+"/cells/"+cellID, body)
		require.Equal(t, http.StatusOK, resp.StatusCode)

		cell := nbDecodeJSON[Cell](t, resp)
		assert.Equal(t, "SELECT 2", *cell.Content)
	})

	t.Run("reorder cells", func(t *testing.T) {
		body := `{"cell_ids":["cell-b","cell-a","cell-c"]}`
		resp := nbDoRequest(t, http.MethodPost, srv.URL+"/notebooks/"+nbID+"/cells/reorder", body)
		require.Equal(t, http.StatusOK, resp.StatusCode)

		var cells []Cell
		defer resp.Body.Close() //nolint:errcheck
		err := json.NewDecoder(resp.Body).Decode(&cells)
		require.NoError(t, err)
		require.Len(t, cells, 3)
		assert.Equal(t, "cell-b", *cells[0].Id)
		assert.Equal(t, int32(0), *cells[0].Position)
		assert.Equal(t, "cell-a", *cells[1].Id)
		assert.Equal(t, int32(1), *cells[1].Position)
	})

	t.Run("delete cell", func(t *testing.T) {
		resp := nbDoRequest(t, http.MethodDelete, srv.URL+"/notebooks/"+nbID+"/cells/"+cellID, "")
		defer resp.Body.Close() //nolint:errcheck
		require.Equal(t, http.StatusNoContent, resp.StatusCode)
	})
}

func TestAPI_Sessions(t *testing.T) {
	now := fixedTime
	nbID := "nb-001"
	sessionID := "sess-001"
	cellID := "cell-001"
	jobID := "job-001"

	sessionSvc := &mockSessionService{
		createSessionFn: func(_ context.Context, notebookID, principal string) (*domain.NotebookSession, error) {
			return &domain.NotebookSession{
				ID:         sessionID,
				NotebookID: notebookID,
				Principal:  principal,
				State:      "active",
				CreatedAt:  now,
				LastUsedAt: now,
			}, nil
		},
		executeCellFn: func(_ context.Context, _ string, cID string) (*domain.CellExecutionResult, error) {
			return &domain.CellExecutionResult{
				CellID:   cID,
				Columns:  []string{"count"},
				Rows:     [][]interface{}{{42}},
				RowCount: 1,
				Duration: 150 * time.Millisecond,
			}, nil
		},
		runAllFn: func(_ context.Context, _ string) (*domain.RunAllResult, error) {
			return &domain.RunAllResult{
				NotebookID: nbID,
				Results: []domain.CellExecutionResult{
					{CellID: cellID, Columns: []string{"x"}, Rows: [][]interface{}{{1}}, RowCount: 1, Duration: 50 * time.Millisecond},
				},
				TotalDuration: 50 * time.Millisecond,
			}, nil
		},
		runAllAsyncFn: func(_ context.Context, _ string) (*domain.NotebookJob, error) {
			return &domain.NotebookJob{
				ID:         jobID,
				NotebookID: nbID,
				SessionID:  sessionID,
				State:      domain.JobStatePending,
				CreatedAt:  now,
				UpdatedAt:  now,
			}, nil
		},
		listJobsFn: func(_ context.Context, _ string, _ domain.PageRequest) ([]domain.NotebookJob, int64, error) {
			return []domain.NotebookJob{
				{ID: jobID, NotebookID: nbID, SessionID: sessionID, State: domain.JobStatePending, CreatedAt: now, UpdatedAt: now},
			}, 1, nil
		},
		getJobFn: func(_ context.Context, id string) (*domain.NotebookJob, error) {
			if id == jobID {
				return &domain.NotebookJob{
					ID:         jobID,
					NotebookID: nbID,
					SessionID:  sessionID,
					State:      domain.JobStateComplete,
					Result:     strPtr(`{"ok":true}`),
					CreatedAt:  now,
					UpdatedAt:  fixedTime2,
				}, nil
			}
			return nil, domain.ErrNotFound("job %s not found", id)
		},
		closeSessionFn: func(_ context.Context, _ string) error {
			return nil
		},
	}

	srv := setupNotebookTestServer(t, nil, sessionSvc, nil, "alice", false)
	defer srv.Close()

	t.Run("create session", func(t *testing.T) {
		resp := nbDoRequest(t, http.MethodPost, srv.URL+"/notebooks/"+nbID+"/sessions", "")
		require.Equal(t, http.StatusCreated, resp.StatusCode)

		sess := nbDecodeJSON[NotebookSession](t, resp)
		require.NotNil(t, sess.Id)
		assert.Equal(t, sessionID, *sess.Id)
		assert.Equal(t, nbID, *sess.NotebookId)
		assert.Equal(t, "alice", *sess.Principal)
		assert.Equal(t, NotebookSessionState("active"), *sess.State)
	})

	t.Run("execute cell", func(t *testing.T) {
		resp := nbDoRequest(t, http.MethodPost, srv.URL+"/notebooks/"+nbID+"/sessions/"+sessionID+"/execute/"+cellID, "")
		require.Equal(t, http.StatusOK, resp.StatusCode)

		result := nbDecodeJSON[CellExecutionResult](t, resp)
		assert.Equal(t, cellID, *result.CellId)
		assert.Equal(t, int32(1), *result.RowCount)
		require.NotNil(t, result.Columns)
		assert.Equal(t, []string{"count"}, *result.Columns)
		assert.Equal(t, int64(150), *result.DurationMs)
	})

	t.Run("run all cells", func(t *testing.T) {
		resp := nbDoRequest(t, http.MethodPost, srv.URL+"/notebooks/"+nbID+"/sessions/"+sessionID+"/run-all", "")
		require.Equal(t, http.StatusOK, resp.StatusCode)

		result := nbDecodeJSON[RunAllResult](t, resp)
		assert.Equal(t, nbID, *result.NotebookId)
		require.NotNil(t, result.Results)
		require.Len(t, *result.Results, 1)
		assert.Equal(t, int64(50), *result.TotalDurationMs)
	})

	t.Run("run all cells async", func(t *testing.T) {
		resp := nbDoRequest(t, http.MethodPost, srv.URL+"/notebooks/"+nbID+"/sessions/"+sessionID+"/run-all-async", "")
		require.Equal(t, http.StatusAccepted, resp.StatusCode)

		job := nbDecodeJSON[NotebookJob](t, resp)
		assert.Equal(t, jobID, *job.Id)
		assert.Equal(t, NotebookJobState("pending"), *job.State)
	})

	t.Run("list jobs", func(t *testing.T) {
		resp := nbDoRequest(t, http.MethodGet, srv.URL+"/notebooks/"+nbID+"/jobs", "")
		require.Equal(t, http.StatusOK, resp.StatusCode)

		list := nbDecodeJSON[PaginatedNotebookJobs](t, resp)
		require.NotNil(t, list.Data)
		require.Len(t, *list.Data, 1)
		assert.Equal(t, jobID, *(*list.Data)[0].Id)
	})

	t.Run("get job", func(t *testing.T) {
		resp := nbDoRequest(t, http.MethodGet, srv.URL+"/notebooks/"+nbID+"/jobs/"+jobID, "")
		require.Equal(t, http.StatusOK, resp.StatusCode)

		job := nbDecodeJSON[NotebookJob](t, resp)
		assert.Equal(t, jobID, *job.Id)
		assert.Equal(t, NotebookJobState("complete"), *job.State)
		require.NotNil(t, job.Result)
		assert.Equal(t, `{"ok":true}`, *job.Result)
	})

	t.Run("close session", func(t *testing.T) {
		resp := nbDoRequest(t, http.MethodDelete, srv.URL+"/notebooks/"+nbID+"/sessions/"+sessionID, "")
		defer resp.Body.Close() //nolint:errcheck
		require.Equal(t, http.StatusNoContent, resp.StatusCode)
	})
}

func TestAPI_GitRepoCRUD(t *testing.T) {
	now := fixedTime
	repoID := "repo-001"

	gitSvc := &mockGitRepoService{
		createGitRepoFn: func(_ context.Context, principal string, req domain.CreateGitRepoRequest) (*domain.GitRepo, error) {
			return &domain.GitRepo{
				ID:        repoID,
				URL:       req.URL,
				Branch:    req.Branch,
				Path:      req.Path,
				AuthToken: req.AuthToken,
				Owner:     principal,
				CreatedAt: now,
				UpdatedAt: now,
			}, nil
		},
		getGitRepoFn: func(_ context.Context, id string) (*domain.GitRepo, error) {
			if id == repoID {
				return &domain.GitRepo{
					ID:        repoID,
					URL:       "https://github.com/org/repo.git",
					Branch:    "main",
					Path:      "/notebooks",
					Owner:     "alice",
					CreatedAt: now,
					UpdatedAt: now,
				}, nil
			}
			return nil, domain.ErrNotFound("git repo %s not found", id)
		},
		listGitReposFn: func(_ context.Context, _ domain.PageRequest) ([]domain.GitRepo, int64, error) {
			return []domain.GitRepo{
				{ID: repoID, URL: "https://github.com/org/repo.git", Branch: "main", Path: "/notebooks", Owner: "alice", CreatedAt: now, UpdatedAt: now},
			}, 1, nil
		},
		deleteGitRepoFn: func(_ context.Context, _ string, _ bool, _ string) error {
			return nil
		},
	}

	srv := setupNotebookTestServer(t, nil, nil, gitSvc, "alice", false)
	defer srv.Close()

	t.Run("create git repo", func(t *testing.T) {
		body := `{"url":"https://github.com/org/repo.git","branch":"main","path":"/notebooks","auth_token":"ghp_secret"}`
		resp := nbDoRequest(t, http.MethodPost, srv.URL+"/git-repos", body)
		require.Equal(t, http.StatusCreated, resp.StatusCode)

		repo := nbDecodeJSON[GitRepo](t, resp)
		require.NotNil(t, repo.Id)
		assert.Equal(t, repoID, *repo.Id)
		assert.Equal(t, "https://github.com/org/repo.git", *repo.Url)
		assert.Equal(t, "main", *repo.Branch)
		assert.Equal(t, "alice", *repo.Owner)
	})

	t.Run("get git repo", func(t *testing.T) {
		resp := nbDoRequest(t, http.MethodGet, srv.URL+"/git-repos/"+repoID, "")
		require.Equal(t, http.StatusOK, resp.StatusCode)

		repo := nbDecodeJSON[GitRepo](t, resp)
		assert.Equal(t, repoID, *repo.Id)
		assert.Equal(t, "https://github.com/org/repo.git", *repo.Url)
	})

	t.Run("list git repos", func(t *testing.T) {
		resp := nbDoRequest(t, http.MethodGet, srv.URL+"/git-repos", "")
		require.Equal(t, http.StatusOK, resp.StatusCode)

		list := nbDecodeJSON[PaginatedGitRepos](t, resp)
		require.NotNil(t, list.Data)
		require.Len(t, *list.Data, 1)
		assert.Equal(t, repoID, *(*list.Data)[0].Id)
	})

	t.Run("delete git repo", func(t *testing.T) {
		resp := nbDoRequest(t, http.MethodDelete, srv.URL+"/git-repos/"+repoID, "")
		defer resp.Body.Close() //nolint:errcheck
		require.Equal(t, http.StatusNoContent, resp.StatusCode)
	})
}

func TestAPI_NotebookNotFound(t *testing.T) {
	notebookSvc := &mockNotebookService{
		getNotebookFn: func(_ context.Context, id string) (*domain.Notebook, []domain.Cell, error) {
			return nil, nil, domain.ErrNotFound("notebook %s not found", id)
		},
	}
	gitSvc := &mockGitRepoService{
		getGitRepoFn: func(_ context.Context, id string) (*domain.GitRepo, error) {
			return nil, domain.ErrNotFound("git repo %s not found", id)
		},
	}

	srv := setupNotebookTestServer(t, notebookSvc, nil, gitSvc, "alice", false)
	defer srv.Close()

	t.Run("GET /notebooks/nonexistent returns 404", func(t *testing.T) {
		resp := nbDoRequest(t, http.MethodGet, srv.URL+"/notebooks/nonexistent", "")
		defer resp.Body.Close() //nolint:errcheck
		require.Equal(t, http.StatusNotFound, resp.StatusCode)

		var errResp Error
		_ = json.NewDecoder(resp.Body).Decode(&errResp)
		assert.Contains(t, errResp.Message, "not found")
	})

	t.Run("GET /git-repos/nonexistent returns 404", func(t *testing.T) {
		resp := nbDoRequest(t, http.MethodGet, srv.URL+"/git-repos/nonexistent", "")
		defer resp.Body.Close() //nolint:errcheck
		require.Equal(t, http.StatusNotFound, resp.StatusCode)

		var errResp Error
		_ = json.NewDecoder(resp.Body).Decode(&errResp)
		assert.Contains(t, errResp.Message, "not found")
	})
}

func TestAPI_NotebookForbidden(t *testing.T) {
	notebookSvc := &mockNotebookService{
		updateNotebookFn: func(_ context.Context, principal string, isAdmin bool, id string, _ domain.UpdateNotebookRequest) (*domain.Notebook, error) {
			if principal != "owner" && !isAdmin {
				return nil, domain.ErrAccessDenied("only the owner or an admin can update notebook %s", id)
			}
			return &domain.Notebook{ID: id, Name: "nb", Owner: "owner"}, nil
		},
		deleteNotebookFn: func(_ context.Context, principal string, isAdmin bool, id string) error {
			if principal != "owner" && !isAdmin {
				return domain.ErrAccessDenied("only the owner or an admin can delete notebook %s", id)
			}
			return nil
		},
	}

	srv := setupNotebookTestServer(t, notebookSvc, nil, nil, "mallory", false)
	defer srv.Close()

	t.Run("update as non-owner returns 403", func(t *testing.T) {
		body := `{"name":"hacked"}`
		resp := nbDoRequest(t, http.MethodPatch, srv.URL+"/notebooks/nb-owner", body)
		defer resp.Body.Close() //nolint:errcheck
		require.Equal(t, http.StatusForbidden, resp.StatusCode)

		var errResp Error
		_ = json.NewDecoder(resp.Body).Decode(&errResp)
		assert.Equal(t, int32(403), errResp.Code)
		assert.True(t, strings.Contains(errResp.Message, "owner") || strings.Contains(errResp.Message, "admin"))
	})

	t.Run("delete as non-owner returns 403", func(t *testing.T) {
		resp := nbDoRequest(t, http.MethodDelete, srv.URL+"/notebooks/nb-owner", "")
		defer resp.Body.Close() //nolint:errcheck
		require.Equal(t, http.StatusForbidden, resp.StatusCode)

		var errResp Error
		_ = json.NewDecoder(resp.Body).Decode(&errResp)
		assert.Equal(t, int32(403), errResp.Code)
	})
}
