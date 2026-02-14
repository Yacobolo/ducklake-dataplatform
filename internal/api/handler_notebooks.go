package api

import (
	"context"
	"errors"

	"duck-demo/internal/domain"
	"duck-demo/internal/middleware"
)

// notebookService defines the notebook operations used by the API handler.
type notebookService interface {
	CreateNotebook(ctx context.Context, principal string, req domain.CreateNotebookRequest) (*domain.Notebook, error)
	GetNotebook(ctx context.Context, id string) (*domain.Notebook, []domain.Cell, error)
	ListNotebooks(ctx context.Context, owner *string, page domain.PageRequest) ([]domain.Notebook, int64, error)
	UpdateNotebook(ctx context.Context, principal string, isAdmin bool, id string, req domain.UpdateNotebookRequest) (*domain.Notebook, error)
	DeleteNotebook(ctx context.Context, principal string, isAdmin bool, id string) error
	CreateCell(ctx context.Context, principal string, isAdmin bool, notebookID string, req domain.CreateCellRequest) (*domain.Cell, error)
	UpdateCell(ctx context.Context, principal string, isAdmin bool, cellID string, req domain.UpdateCellRequest) (*domain.Cell, error)
	DeleteCell(ctx context.Context, principal string, isAdmin bool, cellID string) error
	ReorderCells(ctx context.Context, principal string, isAdmin bool, notebookID string, req domain.ReorderCellsRequest) ([]domain.Cell, error)
}

// sessionService defines session and execution operations.
type sessionService interface {
	CreateSession(ctx context.Context, notebookID, principal string) (*domain.NotebookSession, error)
	CloseSession(ctx context.Context, sessionID string) error
	ExecuteCell(ctx context.Context, sessionID, cellID string) (*domain.CellExecutionResult, error)
	RunAll(ctx context.Context, sessionID string) (*domain.RunAllResult, error)
	RunAllAsync(ctx context.Context, sessionID string) (*domain.NotebookJob, error)
	GetJob(ctx context.Context, jobID string) (*domain.NotebookJob, error)
	ListJobs(ctx context.Context, notebookID string, page domain.PageRequest) ([]domain.NotebookJob, int64, error)
}

// gitRepoService defines git repository operations.
type gitRepoService interface {
	CreateGitRepo(ctx context.Context, principal string, req domain.CreateGitRepoRequest) (*domain.GitRepo, error)
	GetGitRepo(ctx context.Context, id string) (*domain.GitRepo, error)
	ListGitRepos(ctx context.Context, page domain.PageRequest) ([]domain.GitRepo, int64, error)
	DeleteGitRepo(ctx context.Context, principal string, isAdmin bool, id string) error
	SyncGitRepo(ctx context.Context, id string) (*domain.GitSyncResult, error)
}

// === Notebooks ===

// ListNotebooks implements the endpoint for listing notebooks.
func (h *APIHandler) ListNotebooks(ctx context.Context, req ListNotebooksRequestObject) (ListNotebooksResponseObject, error) {
	page := pageFromParams(req.Params.MaxResults, req.Params.PageToken)
	nbs, total, err := h.notebooks.ListNotebooks(ctx, req.Params.Owner, page)
	if err != nil {
		return nil, err
	}

	data := make([]Notebook, len(nbs))
	for i, nb := range nbs {
		data[i] = notebookToAPI(nb)
	}
	nextToken := domain.NextPageToken(page.Offset(), page.Limit(), total)
	return ListNotebooks200JSONResponse{
		Body:    PaginatedNotebooks{Data: &data, NextPageToken: optStr(nextToken)},
		Headers: ListNotebooks200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// CreateNotebook implements the endpoint for creating a notebook.
func (h *APIHandler) CreateNotebook(ctx context.Context, req CreateNotebookRequestObject) (CreateNotebookResponseObject, error) {
	domReq := domain.CreateNotebookRequest{
		Name:        req.Body.Name,
		Description: req.Body.Description,
	}

	principal, _ := middleware.PrincipalFromContext(ctx)
	result, err := h.notebooks.CreateNotebook(ctx, principal, domReq)
	if err != nil {
		switch {
		case errors.As(err, new(*domain.ValidationError)):
			return CreateNotebook400JSONResponse{BadRequestJSONResponse{Body: Error{Code: 400, Message: err.Error()}, Headers: BadRequestResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		case errors.As(err, new(*domain.ConflictError)):
			return CreateNotebook409JSONResponse{ConflictJSONResponse{Body: Error{Code: 409, Message: err.Error()}, Headers: ConflictResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		default:
			return CreateNotebook400JSONResponse{BadRequestJSONResponse{Body: Error{Code: 400, Message: err.Error()}, Headers: BadRequestResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		}
	}
	return CreateNotebook201JSONResponse{
		Body:    notebookToAPI(*result),
		Headers: CreateNotebook201ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// GetNotebook implements the endpoint for retrieving a notebook with its cells.
func (h *APIHandler) GetNotebook(ctx context.Context, req GetNotebookRequestObject) (GetNotebookResponseObject, error) {
	nb, cells, err := h.notebooks.GetNotebook(ctx, req.NotebookId)
	if err != nil {
		switch {
		case errors.As(err, new(*domain.NotFoundError)):
			return GetNotebook404JSONResponse{NotFoundJSONResponse{Body: Error{Code: 404, Message: err.Error()}, Headers: NotFoundResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		default:
			return nil, err
		}
	}

	apiNb := notebookToAPI(*nb)
	apiCells := make([]Cell, len(cells))
	for i, c := range cells {
		apiCells[i] = cellToAPI(c)
	}
	return GetNotebook200JSONResponse{
		Body:    NotebookDetail{Notebook: &apiNb, Cells: &apiCells},
		Headers: GetNotebook200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// UpdateNotebook implements the endpoint for updating notebook metadata.
func (h *APIHandler) UpdateNotebook(ctx context.Context, req UpdateNotebookRequestObject) (UpdateNotebookResponseObject, error) {
	domReq := domain.UpdateNotebookRequest{
		Name:        req.Body.Name,
		Description: req.Body.Description,
	}

	principal, _ := middleware.PrincipalFromContext(ctx)
	cp, _ := domain.PrincipalFromContext(ctx)
	isAdmin := cp.IsAdmin

	result, err := h.notebooks.UpdateNotebook(ctx, principal, isAdmin, req.NotebookId, domReq)
	if err != nil {
		switch {
		case errors.As(err, new(*domain.AccessDeniedError)):
			return UpdateNotebook403JSONResponse{ForbiddenJSONResponse{Body: Error{Code: 403, Message: err.Error()}, Headers: ForbiddenResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		case errors.As(err, new(*domain.NotFoundError)):
			return UpdateNotebook404JSONResponse{NotFoundJSONResponse{Body: Error{Code: 404, Message: err.Error()}, Headers: NotFoundResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		case errors.As(err, new(*domain.ValidationError)):
			return UpdateNotebook400JSONResponse{BadRequestJSONResponse{Body: Error{Code: 400, Message: err.Error()}, Headers: BadRequestResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		default:
			return nil, err
		}
	}
	return UpdateNotebook200JSONResponse{
		Body:    notebookToAPI(*result),
		Headers: UpdateNotebook200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// DeleteNotebook implements the endpoint for deleting a notebook.
func (h *APIHandler) DeleteNotebook(ctx context.Context, req DeleteNotebookRequestObject) (DeleteNotebookResponseObject, error) {
	principal, _ := middleware.PrincipalFromContext(ctx)
	cp, _ := domain.PrincipalFromContext(ctx)
	isAdmin := cp.IsAdmin

	if err := h.notebooks.DeleteNotebook(ctx, principal, isAdmin, req.NotebookId); err != nil {
		switch {
		case errors.As(err, new(*domain.AccessDeniedError)):
			return DeleteNotebook403JSONResponse{ForbiddenJSONResponse{Body: Error{Code: 403, Message: err.Error()}, Headers: ForbiddenResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		case errors.As(err, new(*domain.NotFoundError)):
			return DeleteNotebook404JSONResponse{NotFoundJSONResponse{Body: Error{Code: 404, Message: err.Error()}, Headers: NotFoundResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		default:
			return nil, err
		}
	}
	return DeleteNotebook204Response{}, nil
}

// === Cells ===

// CreateCell implements the endpoint for adding a cell to a notebook.
func (h *APIHandler) CreateCell(ctx context.Context, req CreateCellRequestObject) (CreateCellResponseObject, error) {
	domReq := domain.CreateCellRequest{
		CellType: domain.CellType(req.Body.CellType),
	}
	if req.Body.Content != nil {
		domReq.Content = *req.Body.Content
	}
	if req.Body.Position != nil {
		pos := int(*req.Body.Position)
		domReq.Position = &pos
	}

	principal, _ := middleware.PrincipalFromContext(ctx)
	cp, _ := domain.PrincipalFromContext(ctx)
	isAdmin := cp.IsAdmin

	result, err := h.notebooks.CreateCell(ctx, principal, isAdmin, req.NotebookId, domReq)
	if err != nil {
		switch {
		case errors.As(err, new(*domain.AccessDeniedError)):
			return CreateCell403JSONResponse{ForbiddenJSONResponse{Body: Error{Code: 403, Message: err.Error()}, Headers: ForbiddenResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		case errors.As(err, new(*domain.NotFoundError)):
			return CreateCell404JSONResponse{NotFoundJSONResponse{Body: Error{Code: 404, Message: err.Error()}, Headers: NotFoundResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		case errors.As(err, new(*domain.ValidationError)):
			return CreateCell400JSONResponse{BadRequestJSONResponse{Body: Error{Code: 400, Message: err.Error()}, Headers: BadRequestResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		default:
			return CreateCell400JSONResponse{BadRequestJSONResponse{Body: Error{Code: 400, Message: err.Error()}, Headers: BadRequestResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		}
	}
	return CreateCell201JSONResponse{
		Body:    cellToAPI(*result),
		Headers: CreateCell201ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// UpdateCell implements the endpoint for updating a cell.
func (h *APIHandler) UpdateCell(ctx context.Context, req UpdateCellRequestObject) (UpdateCellResponseObject, error) {
	domReq := domain.UpdateCellRequest{
		Content: req.Body.Content,
	}
	if req.Body.Position != nil {
		pos := int(*req.Body.Position)
		domReq.Position = &pos
	}

	principal, _ := middleware.PrincipalFromContext(ctx)
	cp, _ := domain.PrincipalFromContext(ctx)
	isAdmin := cp.IsAdmin

	result, err := h.notebooks.UpdateCell(ctx, principal, isAdmin, req.CellId, domReq)
	if err != nil {
		switch {
		case errors.As(err, new(*domain.AccessDeniedError)):
			return UpdateCell403JSONResponse{ForbiddenJSONResponse{Body: Error{Code: 403, Message: err.Error()}, Headers: ForbiddenResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		case errors.As(err, new(*domain.NotFoundError)):
			return UpdateCell404JSONResponse{NotFoundJSONResponse{Body: Error{Code: 404, Message: err.Error()}, Headers: NotFoundResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		case errors.As(err, new(*domain.ValidationError)):
			return UpdateCell400JSONResponse{BadRequestJSONResponse{Body: Error{Code: 400, Message: err.Error()}, Headers: BadRequestResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		default:
			return nil, err
		}
	}
	return UpdateCell200JSONResponse{
		Body:    cellToAPI(*result),
		Headers: UpdateCell200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// DeleteCell implements the endpoint for deleting a cell.
func (h *APIHandler) DeleteCell(ctx context.Context, req DeleteCellRequestObject) (DeleteCellResponseObject, error) {
	principal, _ := middleware.PrincipalFromContext(ctx)
	cp, _ := domain.PrincipalFromContext(ctx)
	isAdmin := cp.IsAdmin

	if err := h.notebooks.DeleteCell(ctx, principal, isAdmin, req.CellId); err != nil {
		switch {
		case errors.As(err, new(*domain.AccessDeniedError)):
			return DeleteCell403JSONResponse{ForbiddenJSONResponse{Body: Error{Code: 403, Message: err.Error()}, Headers: ForbiddenResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		case errors.As(err, new(*domain.NotFoundError)):
			return DeleteCell404JSONResponse{NotFoundJSONResponse{Body: Error{Code: 404, Message: err.Error()}, Headers: NotFoundResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		default:
			return nil, err
		}
	}
	return DeleteCell204Response{}, nil
}

// ReorderCells implements the endpoint for reordering cells in a notebook.
func (h *APIHandler) ReorderCells(ctx context.Context, req ReorderCellsRequestObject) (ReorderCellsResponseObject, error) {
	domReq := domain.ReorderCellsRequest{
		CellIDs: req.Body.CellIds,
	}

	principal, _ := middleware.PrincipalFromContext(ctx)
	cp, _ := domain.PrincipalFromContext(ctx)
	isAdmin := cp.IsAdmin

	cells, err := h.notebooks.ReorderCells(ctx, principal, isAdmin, req.NotebookId, domReq)
	if err != nil {
		switch {
		case errors.As(err, new(*domain.AccessDeniedError)):
			return ReorderCells403JSONResponse{ForbiddenJSONResponse{Body: Error{Code: 403, Message: err.Error()}, Headers: ForbiddenResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		case errors.As(err, new(*domain.NotFoundError)):
			return ReorderCells404JSONResponse{NotFoundJSONResponse{Body: Error{Code: 404, Message: err.Error()}, Headers: NotFoundResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		case errors.As(err, new(*domain.ValidationError)):
			return ReorderCells400JSONResponse{BadRequestJSONResponse{Body: Error{Code: 400, Message: err.Error()}, Headers: BadRequestResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		default:
			return nil, err
		}
	}

	data := make([]Cell, len(cells))
	for i, c := range cells {
		data[i] = cellToAPI(c)
	}
	return ReorderCells200JSONResponse{
		Body:    data,
		Headers: ReorderCells200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// === Sessions ===

// CreateNotebookSession implements the endpoint for starting a notebook session.
func (h *APIHandler) CreateNotebookSession(ctx context.Context, req CreateNotebookSessionRequestObject) (CreateNotebookSessionResponseObject, error) {
	principal, _ := middleware.PrincipalFromContext(ctx)

	result, err := h.sessions.CreateSession(ctx, req.NotebookId, principal)
	if err != nil {
		switch {
		case errors.As(err, new(*domain.NotFoundError)):
			return CreateNotebookSession404JSONResponse{NotFoundJSONResponse{Body: Error{Code: 404, Message: err.Error()}, Headers: NotFoundResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		default:
			return nil, err
		}
	}
	return CreateNotebookSession201JSONResponse{
		Body:    sessionToAPI(*result),
		Headers: CreateNotebookSession201ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// CloseNotebookSession implements the endpoint for closing a notebook session.
func (h *APIHandler) CloseNotebookSession(ctx context.Context, req CloseNotebookSessionRequestObject) (CloseNotebookSessionResponseObject, error) {
	if err := h.sessions.CloseSession(ctx, req.SessionId); err != nil {
		switch {
		case errors.As(err, new(*domain.NotFoundError)):
			return CloseNotebookSession404JSONResponse{NotFoundJSONResponse{Body: Error{Code: 404, Message: err.Error()}, Headers: NotFoundResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		default:
			return nil, err
		}
	}
	return CloseNotebookSession204Response{}, nil
}

// ExecuteCell implements the endpoint for executing a single cell in a session.
func (h *APIHandler) ExecuteCell(ctx context.Context, req ExecuteCellRequestObject) (ExecuteCellResponseObject, error) {
	result, err := h.sessions.ExecuteCell(ctx, req.SessionId, req.CellId)
	if err != nil {
		switch {
		case errors.As(err, new(*domain.AccessDeniedError)):
			return ExecuteCell403JSONResponse{ForbiddenJSONResponse{Body: Error{Code: 403, Message: err.Error()}, Headers: ForbiddenResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		case errors.As(err, new(*domain.NotFoundError)):
			return ExecuteCell404JSONResponse{NotFoundJSONResponse{Body: Error{Code: 404, Message: err.Error()}, Headers: NotFoundResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		case errors.As(err, new(*domain.ValidationError)):
			return ExecuteCell400JSONResponse{BadRequestJSONResponse{Body: Error{Code: 400, Message: err.Error()}, Headers: BadRequestResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		default:
			return nil, err
		}
	}
	return ExecuteCell200JSONResponse{
		Body:    cellExecutionResultToAPI(*result),
		Headers: ExecuteCell200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// RunAllCells implements the endpoint for executing all SQL cells synchronously.
func (h *APIHandler) RunAllCells(ctx context.Context, req RunAllCellsRequestObject) (RunAllCellsResponseObject, error) {
	result, err := h.sessions.RunAll(ctx, req.SessionId)
	if err != nil {
		switch {
		case errors.As(err, new(*domain.NotFoundError)):
			return RunAllCells404JSONResponse{NotFoundJSONResponse{Body: Error{Code: 404, Message: err.Error()}, Headers: NotFoundResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		default:
			return nil, err
		}
	}
	return RunAllCells200JSONResponse{
		Body:    runAllResultToAPI(*result),
		Headers: RunAllCells200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// RunAllCellsAsync implements the endpoint for starting async execution of all cells.
func (h *APIHandler) RunAllCellsAsync(ctx context.Context, req RunAllCellsAsyncRequestObject) (RunAllCellsAsyncResponseObject, error) {
	result, err := h.sessions.RunAllAsync(ctx, req.SessionId)
	if err != nil {
		switch {
		case errors.As(err, new(*domain.NotFoundError)):
			return RunAllCellsAsync404JSONResponse{NotFoundJSONResponse{Body: Error{Code: 404, Message: err.Error()}, Headers: NotFoundResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		default:
			return nil, err
		}
	}
	return RunAllCellsAsync202JSONResponse{
		Body:    notebookJobToAPI(*result),
		Headers: RunAllCellsAsync202ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// === Jobs ===

// ListNotebookJobs implements the endpoint for listing jobs for a notebook.
func (h *APIHandler) ListNotebookJobs(ctx context.Context, req ListNotebookJobsRequestObject) (ListNotebookJobsResponseObject, error) {
	page := pageFromParams(req.Params.MaxResults, req.Params.PageToken)
	jobs, total, err := h.sessions.ListJobs(ctx, req.NotebookId, page)
	if err != nil {
		switch {
		case errors.As(err, new(*domain.NotFoundError)):
			return ListNotebookJobs404JSONResponse{NotFoundJSONResponse{Body: Error{Code: 404, Message: err.Error()}, Headers: NotFoundResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		default:
			return nil, err
		}
	}

	data := make([]NotebookJob, len(jobs))
	for i, j := range jobs {
		data[i] = notebookJobToAPI(j)
	}
	nextToken := domain.NextPageToken(page.Offset(), page.Limit(), total)
	return ListNotebookJobs200JSONResponse{
		Body:    PaginatedNotebookJobs{Data: &data, NextPageToken: optStr(nextToken)},
		Headers: ListNotebookJobs200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// GetNotebookJob implements the endpoint for getting job status.
func (h *APIHandler) GetNotebookJob(ctx context.Context, req GetNotebookJobRequestObject) (GetNotebookJobResponseObject, error) {
	result, err := h.sessions.GetJob(ctx, req.JobId)
	if err != nil {
		switch {
		case errors.As(err, new(*domain.NotFoundError)):
			return GetNotebookJob404JSONResponse{NotFoundJSONResponse{Body: Error{Code: 404, Message: err.Error()}, Headers: NotFoundResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		default:
			return nil, err
		}
	}
	return GetNotebookJob200JSONResponse{
		Body:    notebookJobToAPI(*result),
		Headers: GetNotebookJob200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// === Git Repos ===

// ListGitRepos implements the endpoint for listing Git repositories.
func (h *APIHandler) ListGitRepos(ctx context.Context, req ListGitReposRequestObject) (ListGitReposResponseObject, error) {
	page := pageFromParams(req.Params.MaxResults, req.Params.PageToken)
	repos, total, err := h.gitRepos.ListGitRepos(ctx, page)
	if err != nil {
		return nil, err
	}

	data := make([]GitRepo, len(repos))
	for i, r := range repos {
		data[i] = gitRepoToAPI(r)
	}
	nextToken := domain.NextPageToken(page.Offset(), page.Limit(), total)
	return ListGitRepos200JSONResponse{
		Body:    PaginatedGitRepos{Data: &data, NextPageToken: optStr(nextToken)},
		Headers: ListGitRepos200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// CreateGitRepo implements the endpoint for registering a Git repository.
func (h *APIHandler) CreateGitRepo(ctx context.Context, req CreateGitRepoRequestObject) (CreateGitRepoResponseObject, error) {
	domReq := domain.CreateGitRepoRequest{
		URL:       req.Body.Url,
		Branch:    req.Body.Branch,
		AuthToken: req.Body.AuthToken,
	}
	if req.Body.Path != nil {
		domReq.Path = *req.Body.Path
	}

	principal, _ := middleware.PrincipalFromContext(ctx)
	result, err := h.gitRepos.CreateGitRepo(ctx, principal, domReq)
	if err != nil {
		switch {
		case errors.As(err, new(*domain.ValidationError)):
			return CreateGitRepo400JSONResponse{BadRequestJSONResponse{Body: Error{Code: 400, Message: err.Error()}, Headers: BadRequestResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		case errors.As(err, new(*domain.ConflictError)):
			return CreateGitRepo409JSONResponse{ConflictJSONResponse{Body: Error{Code: 409, Message: err.Error()}, Headers: ConflictResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		default:
			return CreateGitRepo400JSONResponse{BadRequestJSONResponse{Body: Error{Code: 400, Message: err.Error()}, Headers: BadRequestResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		}
	}
	return CreateGitRepo201JSONResponse{
		Body:    gitRepoToAPI(*result),
		Headers: CreateGitRepo201ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// GetGitRepo implements the endpoint for retrieving a Git repository.
func (h *APIHandler) GetGitRepo(ctx context.Context, req GetGitRepoRequestObject) (GetGitRepoResponseObject, error) {
	result, err := h.gitRepos.GetGitRepo(ctx, req.GitRepoId)
	if err != nil {
		switch {
		case errors.As(err, new(*domain.NotFoundError)):
			return GetGitRepo404JSONResponse{NotFoundJSONResponse{Body: Error{Code: 404, Message: err.Error()}, Headers: NotFoundResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		default:
			return nil, err
		}
	}
	return GetGitRepo200JSONResponse{
		Body:    gitRepoToAPI(*result),
		Headers: GetGitRepo200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// DeleteGitRepo implements the endpoint for deleting a Git repository.
func (h *APIHandler) DeleteGitRepo(ctx context.Context, req DeleteGitRepoRequestObject) (DeleteGitRepoResponseObject, error) {
	principal, _ := middleware.PrincipalFromContext(ctx)
	cp, _ := domain.PrincipalFromContext(ctx)
	isAdmin := cp.IsAdmin

	if err := h.gitRepos.DeleteGitRepo(ctx, principal, isAdmin, req.GitRepoId); err != nil {
		switch {
		case errors.As(err, new(*domain.NotFoundError)):
			return DeleteGitRepo404JSONResponse{NotFoundJSONResponse{Body: Error{Code: 404, Message: err.Error()}, Headers: NotFoundResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		default:
			return nil, err
		}
	}
	return DeleteGitRepo204Response{}, nil
}

// SyncGitRepo implements the endpoint for triggering a Git sync.
func (h *APIHandler) SyncGitRepo(ctx context.Context, req SyncGitRepoRequestObject) (SyncGitRepoResponseObject, error) {
	result, err := h.gitRepos.SyncGitRepo(ctx, req.GitRepoId)
	if err != nil {
		switch {
		case errors.As(err, new(*domain.NotFoundError)):
			return SyncGitRepo404JSONResponse{NotFoundJSONResponse{Body: Error{Code: 404, Message: err.Error()}, Headers: NotFoundResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset}}}, nil
		default:
			return nil, err
		}
	}

	created := int32(min(result.NotebooksCreated, int(^uint32(0)>>1))) //nolint:gosec // bounded by min
	updated := int32(min(result.NotebooksUpdated, int(^uint32(0)>>1))) //nolint:gosec // bounded by min
	deleted := int32(min(result.NotebooksDeleted, int(^uint32(0)>>1))) //nolint:gosec // bounded by min
	return SyncGitRepo200JSONResponse{
		Body: GitSyncResult{
			CommitSha:        &result.CommitSHA,
			NotebooksCreated: &created,
			NotebooksUpdated: &updated,
			NotebooksDeleted: &deleted,
		},
		Headers: SyncGitRepo200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// === Notebook Mappers ===

func notebookToAPI(nb domain.Notebook) Notebook {
	ct := nb.CreatedAt
	ut := nb.UpdatedAt
	return Notebook{
		Id:          &nb.ID,
		Name:        &nb.Name,
		Description: nb.Description,
		Owner:       &nb.Owner,
		CreatedAt:   &ct,
		UpdatedAt:   &ut,
	}
}

func cellToAPI(c domain.Cell) Cell {
	ct := c.CreatedAt
	ut := c.UpdatedAt
	cellType := CellCellType(c.CellType)
	pos := int32(c.Position) //nolint:gosec // positions are small ints
	return Cell{
		Id:         &c.ID,
		NotebookId: &c.NotebookID,
		CellType:   &cellType,
		Content:    &c.Content,
		Position:   &pos,
		LastResult: c.LastResult,
		CreatedAt:  &ct,
		UpdatedAt:  &ut,
	}
}

func sessionToAPI(s domain.NotebookSession) NotebookSession {
	ct := s.CreatedAt
	lu := s.LastUsedAt
	state := NotebookSessionState(s.State)
	return NotebookSession{
		Id:         &s.ID,
		NotebookId: &s.NotebookID,
		Principal:  &s.Principal,
		State:      &state,
		CreatedAt:  &ct,
		LastUsedAt: &lu,
	}
}

func cellExecutionResultToAPI(r domain.CellExecutionResult) CellExecutionResult {
	durationMs := r.Duration.Milliseconds()
	rowCount := int32(r.RowCount) //nolint:gosec // row counts are small
	return CellExecutionResult{
		CellId:     &r.CellID,
		Columns:    &r.Columns,
		Rows:       &r.Rows,
		RowCount:   &rowCount,
		Error:      r.Error,
		DurationMs: &durationMs,
	}
}

func runAllResultToAPI(r domain.RunAllResult) RunAllResult {
	totalMs := r.TotalDuration.Milliseconds()
	results := make([]CellExecutionResult, len(r.Results))
	for i, cr := range r.Results {
		results[i] = cellExecutionResultToAPI(cr)
	}
	return RunAllResult{
		NotebookId:      &r.NotebookID,
		Results:         &results,
		TotalDurationMs: &totalMs,
	}
}

func notebookJobToAPI(j domain.NotebookJob) NotebookJob {
	ct := j.CreatedAt
	ut := j.UpdatedAt
	state := NotebookJobState(j.State)
	return NotebookJob{
		Id:         &j.ID,
		NotebookId: &j.NotebookID,
		SessionId:  &j.SessionID,
		State:      &state,
		Result:     j.Result,
		Error:      j.Error,
		CreatedAt:  &ct,
		UpdatedAt:  &ut,
	}
}

func gitRepoToAPI(r domain.GitRepo) GitRepo {
	ct := r.CreatedAt
	ut := r.UpdatedAt
	return GitRepo{
		Id:         &r.ID,
		Url:        &r.URL,
		Branch:     &r.Branch,
		Path:       &r.Path,
		Owner:      &r.Owner,
		LastSyncAt: r.LastSyncAt,
		LastCommit: r.LastCommit,
		CreatedAt:  &ct,
		UpdatedAt:  &ut,
	}
}
