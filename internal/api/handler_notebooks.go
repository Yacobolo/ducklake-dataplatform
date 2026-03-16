package api

import (
	"context"

	"duck-demo/internal/domain"
)

// notebookService defines the notebook operations used by the API handler.
type notebookService interface {
	CreateNotebook(ctx context.Context, principal string, req domain.CreateNotebookRequest) (*domain.Notebook, error)
	GetNotebook(ctx context.Context, id string) (*domain.Notebook, []domain.Cell, error)
	GetNotebookForPrincipal(ctx context.Context, principal string, isAdmin bool, id string) (*domain.Notebook, []domain.Cell, error)
	GetPublishModel(ctx context.Context, notebookID string) (*domain.NotebookPublishModel, error)
	ListNotebooks(ctx context.Context, owner *string, page domain.PageRequest) ([]domain.Notebook, int64, error)
	ListNotebooksForPrincipal(ctx context.Context, principal string, isAdmin bool, owner *string, page domain.PageRequest) ([]domain.Notebook, int64, error)
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
	CreateSessionForNotebook(ctx context.Context, notebookID, principal string, isAdmin bool) (*domain.NotebookSession, error)
	CloseSession(ctx context.Context, sessionID string, principalName ...string) error
	CloseNotebookSession(ctx context.Context, notebookID, sessionID, principal string, isAdmin bool) error
	ExecuteCell(ctx context.Context, sessionID, cellID string, principalName ...string) (*domain.CellExecutionResult, error)
	ExecuteNotebookCell(ctx context.Context, notebookID, sessionID, cellID, principal string, isAdmin bool) (*domain.CellExecutionResult, error)
	RunAll(ctx context.Context, sessionID string, principalName ...string) (*domain.RunAllResult, error)
	RunAllNotebook(ctx context.Context, notebookID, sessionID, principal string, isAdmin bool) (*domain.RunAllResult, error)
	RunAllAsync(ctx context.Context, sessionID string, principalName ...string) (*domain.NotebookJob, error)
	RunAllNotebookAsync(ctx context.Context, notebookID, sessionID, principal string, isAdmin bool) (*domain.NotebookJob, error)
	GetJob(ctx context.Context, jobID string) (*domain.NotebookJob, error)
	GetNotebookJob(ctx context.Context, notebookID, jobID, principal string, isAdmin bool) (*domain.NotebookJob, error)
	ListJobs(ctx context.Context, notebookID string, page domain.PageRequest) ([]domain.NotebookJob, int64, error)
	ListNotebookJobs(ctx context.Context, notebookID, principal string, isAdmin bool, page domain.PageRequest) ([]domain.NotebookJob, int64, error)
}

// gitRepoService defines git repository operations.
type gitRepoService interface {
	CreateGitRepo(ctx context.Context, principal string, req domain.CreateGitRepoRequest) (*domain.GitRepo, error)
	GetGitRepo(ctx context.Context, id string) (*domain.GitRepo, error)
	GetGitRepoForPrincipal(ctx context.Context, principal string, isAdmin bool, id string) (*domain.GitRepo, error)
	ListGitRepos(ctx context.Context, page domain.PageRequest) ([]domain.GitRepo, int64, error)
	ListGitReposForPrincipal(ctx context.Context, principal string, isAdmin bool, page domain.PageRequest) ([]domain.GitRepo, int64, error)
	DeleteGitRepo(ctx context.Context, principal string, isAdmin bool, id string) error
	SyncGitRepo(ctx context.Context, principal string, isAdmin bool, id string) (*domain.GitSyncResult, error)
}

// === Notebooks ===

// ListNotebooks implements the endpoint for listing notebooks.
func (h *APIHandler) ListNotebooks(ctx context.Context, req GenListNotebooksRequest) (GenListNotebooksResponse, error) {
	page := pageFromParams(req.Params.MaxResults, req.Params.PageToken)
	cp, _ := domain.PrincipalFromContext(ctx)
	nbs, total, err := h.notebooks.ListNotebooksForPrincipal(ctx, cp.Name, cp.IsAdmin, req.Params.Owner, page)
	if err != nil {
		if resp, ok := respondDomainError[GenListNotebooksResponse](err, domainErrorResponder[GenListNotebooksResponse]{
			Forbidden: func(resp ForbiddenJSONResponse) GenListNotebooksResponse { return ListNotebooks403JSONResponse{resp} },
		}); ok {
			return resp, nil
		}
		return nil, err
	}

	data := make([]Notebook, len(nbs))
	for i, nb := range nbs {
		data[i] = notebookToAPI(nb)
	}
	nextToken := domain.NextPageToken(page.Offset(), page.Limit(), total)
	return GenListNotebooks200JSONResponse{
		Body:    PaginatedNotebooks{Data: data, NextPageToken: optStr(nextToken)},
		Headers: GenListNotebooks200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// CreateNotebook implements the endpoint for creating a notebook.
func (h *APIHandler) CreateNotebook(ctx context.Context, req GenCreateNotebookRequest) (GenCreateNotebookResponse, error) {
	domReq := domain.CreateNotebookRequest{
		Name:        req.Body.Name,
		Description: req.Body.Description,
		Source:      req.Body.Source,
	}

	cp, _ := domain.PrincipalFromContext(ctx)
	principal := cp.Name
	result, err := h.notebooks.CreateNotebook(ctx, principal, domReq)
	if err != nil {
		if resp, ok := respondDomainError[GenCreateNotebookResponse](err, domainErrorResponder[GenCreateNotebookResponse]{
			BadRequest: func(resp BadRequestJSONResponse) GenCreateNotebookResponse {
				return CreateNotebook400JSONResponse{resp}
			},
			Conflict: func(resp ConflictJSONResponse) GenCreateNotebookResponse { return CreateNotebook409JSONResponse{resp} },
		}); ok {
			return resp, nil
		}
		return CreateNotebook400JSONResponse{badRequestErrorResponse(err)}, nil
	}
	return GenCreateNotebook201JSONResponse{
		Body:    notebookToAPI(*result),
		Headers: GenCreateNotebook201ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// GetNotebook implements the endpoint for retrieving a notebook with its cells.
func (h *APIHandler) GetNotebook(ctx context.Context, req GenGetNotebookRequest) (GenGetNotebookResponse, error) {
	cp, _ := domain.PrincipalFromContext(ctx)
	nb, cells, err := h.notebooks.GetNotebookForPrincipal(ctx, cp.Name, cp.IsAdmin, req.NotebookId)
	if err != nil {
		if resp, ok := respondDomainError[GenGetNotebookResponse](err, domainErrorResponder[GenGetNotebookResponse]{
			Forbidden: func(resp ForbiddenJSONResponse) GenGetNotebookResponse { return GenGetNotebook403JSONResponse{resp} },
			NotFound:  func(resp NotFoundJSONResponse) GenGetNotebookResponse { return GenGetNotebook404JSONResponse{resp} },
		}); ok {
			return resp, nil
		}
		return nil, err
	}

	apiNb := notebookToAPI(*nb)
	apiCells := make([]Cell, len(cells))
	for i, c := range cells {
		apiCells[i] = cellToAPI(c)
	}
	var publishModel *NotebookPublishModel
	if model, err := h.notebooks.GetPublishModel(ctx, req.NotebookId); err == nil && model != nil {
		publishModel = notebookPublishModelToAPI(model)
	}
	return GenGetNotebook200JSONResponse{
		Body:    NotebookDetail{Notebook: &apiNb, Cells: &apiCells, PublishModel: publishModel},
		Headers: GenGetNotebook200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// UpdateNotebook implements the endpoint for updating notebook metadata.
func (h *APIHandler) UpdateNotebook(ctx context.Context, req GenUpdateNotebookRequest) (GenUpdateNotebookResponse, error) {
	domReq := domain.UpdateNotebookRequest{
		Name:        req.Body.Name,
		Description: req.Body.Description,
	}

	cp, _ := domain.PrincipalFromContext(ctx)
	principal := cp.Name
	isAdmin := cp.IsAdmin

	result, err := h.notebooks.UpdateNotebook(ctx, principal, isAdmin, req.NotebookId, domReq)
	if err != nil {
		if resp, ok := respondDomainError[GenUpdateNotebookResponse](err, domainErrorResponder[GenUpdateNotebookResponse]{
			BadRequest: func(resp BadRequestJSONResponse) GenUpdateNotebookResponse {
				return UpdateNotebook400JSONResponse{resp}
			},
			Forbidden: func(resp ForbiddenJSONResponse) GenUpdateNotebookResponse { return UpdateNotebook403JSONResponse{resp} },
			NotFound:  func(resp NotFoundJSONResponse) GenUpdateNotebookResponse { return UpdateNotebook404JSONResponse{resp} },
		}); ok {
			return resp, nil
		}
		return nil, err
	}
	return GenUpdateNotebook200JSONResponse{
		Body:    notebookToAPI(*result),
		Headers: GenUpdateNotebook200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// DeleteNotebook implements the endpoint for deleting a notebook.
func (h *APIHandler) DeleteNotebook(ctx context.Context, req GenDeleteNotebookRequest) (GenDeleteNotebookResponse, error) {
	cp, _ := domain.PrincipalFromContext(ctx)
	principal := cp.Name
	isAdmin := cp.IsAdmin

	if err := h.notebooks.DeleteNotebook(ctx, principal, isAdmin, req.NotebookId); err != nil {
		if resp, ok := respondDomainError[GenDeleteNotebookResponse](err, domainErrorResponder[GenDeleteNotebookResponse]{
			Forbidden: func(resp ForbiddenJSONResponse) GenDeleteNotebookResponse { return DeleteNotebook403JSONResponse{resp} },
			NotFound:  func(resp NotFoundJSONResponse) GenDeleteNotebookResponse { return DeleteNotebook404JSONResponse{resp} },
		}); ok {
			return resp, nil
		}
		return nil, err
	}
	return GenDeleteNotebook204Response{}, nil
}

// === Cells ===

// CreateCell implements the endpoint for adding a cell to a notebook.
func (h *APIHandler) CreateCell(ctx context.Context, req GenCreateCellRequest) (GenCreateCellResponse, error) {
	domReq := domain.CreateCellRequest{
		CellType: domain.CellType(req.Body.CellType),
	}
	if req.Body.Name != nil {
		domReq.Name = req.Body.Name
	}
	if req.Body.Role != nil {
		role := domain.CellRole(*req.Body.Role)
		domReq.Role = &role
	}
	if req.Body.Disabled != nil {
		domReq.Disabled = *req.Body.Disabled
	}
	if req.Body.Test != nil {
		domReq.Test = notebookCellTestConfigFromAPI(req.Body.Test)
	}
	if req.Body.VisualSpec != nil {
		domReq.VisualSpec = visualSpecFromAPI(req.Body.VisualSpec)
	}
	if req.Body.Content != nil {
		domReq.Content = *req.Body.Content
	}
	if req.Body.Position != nil {
		pos := int(*req.Body.Position)
		domReq.Position = &pos
	}

	cp, _ := domain.PrincipalFromContext(ctx)
	principal := cp.Name
	isAdmin := cp.IsAdmin

	result, err := h.notebooks.CreateCell(ctx, principal, isAdmin, req.NotebookId, domReq)
	if err != nil {
		if resp, ok := respondDomainError[GenCreateCellResponse](err, domainErrorResponder[GenCreateCellResponse]{
			BadRequest: func(resp BadRequestJSONResponse) GenCreateCellResponse { return CreateCell400JSONResponse{resp} },
			Forbidden:  func(resp ForbiddenJSONResponse) GenCreateCellResponse { return CreateCell403JSONResponse{resp} },
			NotFound:   func(resp NotFoundJSONResponse) GenCreateCellResponse { return CreateCell404JSONResponse{resp} },
		}); ok {
			return resp, nil
		}
		return CreateCell400JSONResponse{badRequestErrorResponse(err)}, nil
	}
	return GenCreateCell201JSONResponse{
		Body:    cellToAPI(*result),
		Headers: GenCreateCell201ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// UpdateCell implements the endpoint for updating a cell.
func (h *APIHandler) UpdateCell(ctx context.Context, req GenUpdateCellRequest) (GenUpdateCellResponse, error) {
	domReq := domain.UpdateCellRequest{
		Content: req.Body.Content,
	}
	if req.Body.Name != nil {
		domReq.Name = req.Body.Name
	}
	if req.Body.Role != nil {
		role := domain.CellRole(*req.Body.Role)
		domReq.Role = &role
	}
	if req.Body.Disabled != nil {
		domReq.Disabled = req.Body.Disabled
	}
	if req.Body.Test != nil {
		domReq.Test = notebookCellTestConfigFromAPI(req.Body.Test)
	}
	if req.Body.VisualSpec != nil {
		domReq.VisualSpec = visualSpecFromAPI(req.Body.VisualSpec)
	}
	if req.Body.Position != nil {
		pos := int(*req.Body.Position)
		domReq.Position = &pos
	}

	cp, _ := domain.PrincipalFromContext(ctx)
	principal := cp.Name
	isAdmin := cp.IsAdmin

	result, err := h.notebooks.UpdateCell(ctx, principal, isAdmin, req.CellId, domReq)
	if err != nil {
		if resp, ok := respondDomainError[GenUpdateCellResponse](err, domainErrorResponder[GenUpdateCellResponse]{
			BadRequest: func(resp BadRequestJSONResponse) GenUpdateCellResponse { return UpdateCell400JSONResponse{resp} },
			Forbidden:  func(resp ForbiddenJSONResponse) GenUpdateCellResponse { return UpdateCell403JSONResponse{resp} },
			NotFound:   func(resp NotFoundJSONResponse) GenUpdateCellResponse { return UpdateCell404JSONResponse{resp} },
		}); ok {
			return resp, nil
		}
		return nil, err
	}
	return GenUpdateCell200JSONResponse{
		Body:    cellToAPI(*result),
		Headers: GenUpdateCell200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// DeleteCell implements the endpoint for deleting a cell.
func (h *APIHandler) DeleteCell(ctx context.Context, req GenDeleteCellRequest) (GenDeleteCellResponse, error) {
	cp, _ := domain.PrincipalFromContext(ctx)
	principal := cp.Name
	isAdmin := cp.IsAdmin

	if err := h.notebooks.DeleteCell(ctx, principal, isAdmin, req.CellId); err != nil {
		if resp, ok := respondDomainError[GenDeleteCellResponse](err, domainErrorResponder[GenDeleteCellResponse]{
			BadRequest: func(resp BadRequestJSONResponse) GenDeleteCellResponse { return DeleteCell400JSONResponse{resp} },
			Forbidden:  func(resp ForbiddenJSONResponse) GenDeleteCellResponse { return DeleteCell403JSONResponse{resp} },
			NotFound:   func(resp NotFoundJSONResponse) GenDeleteCellResponse { return DeleteCell404JSONResponse{resp} },
		}); ok {
			return resp, nil
		}
		return nil, err
	}
	return GenDeleteCell204Response{}, nil
}

// ReorderCells implements the endpoint for reordering cells in a notebook.
func (h *APIHandler) ReorderCells(ctx context.Context, req GenReorderCellsRequest) (GenReorderCellsResponse, error) {
	domReq := domain.ReorderCellsRequest{
		CellIDs: req.Body.CellIds,
	}

	cp, _ := domain.PrincipalFromContext(ctx)
	principal := cp.Name
	isAdmin := cp.IsAdmin

	cells, err := h.notebooks.ReorderCells(ctx, principal, isAdmin, req.NotebookId, domReq)
	if err != nil {
		if resp, ok := respondDomainError[GenReorderCellsResponse](err, domainErrorResponder[GenReorderCellsResponse]{
			BadRequest: func(resp BadRequestJSONResponse) GenReorderCellsResponse { return ReorderCells400JSONResponse{resp} },
			Forbidden:  func(resp ForbiddenJSONResponse) GenReorderCellsResponse { return ReorderCells403JSONResponse{resp} },
			NotFound:   func(resp NotFoundJSONResponse) GenReorderCellsResponse { return ReorderCells404JSONResponse{resp} },
		}); ok {
			return resp, nil
		}
		return nil, err
	}

	data := make([]Cell, len(cells))
	for i, c := range cells {
		data[i] = cellToAPI(c)
	}
	return ReorderCells200JSONResponse{
		Body:    CellList{Data: data},
		Headers: ReorderCells200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// === Sessions ===

// CreateNotebookSession implements the endpoint for starting a notebook session.
func (h *APIHandler) CreateNotebookSession(ctx context.Context, req GenCreateNotebookSessionRequest) (GenCreateNotebookSessionResponse, error) {
	cp, _ := domain.PrincipalFromContext(ctx)
	principal := cp.Name

	result, err := h.sessions.CreateSessionForNotebook(ctx, req.NotebookId, principal, cp.IsAdmin)
	if err != nil {
		if resp, ok := respondDomainError[GenCreateNotebookSessionResponse](err, domainErrorResponder[GenCreateNotebookSessionResponse]{
			Forbidden: func(resp ForbiddenJSONResponse) GenCreateNotebookSessionResponse {
				return CreateNotebookSession403JSONResponse{resp}
			},
			NotFound: func(resp NotFoundJSONResponse) GenCreateNotebookSessionResponse {
				return CreateNotebookSession404JSONResponse{resp}
			},
		}); ok {
			return resp, nil
		}
		return nil, err
	}
	return GenCreateNotebookSession201JSONResponse{
		Body:    sessionToAPI(*result),
		Headers: GenCreateNotebookSession201ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// CloseNotebookSession implements the endpoint for closing a notebook session.
func (h *APIHandler) CloseNotebookSession(ctx context.Context, req GenCloseNotebookSessionRequest) (GenCloseNotebookSessionResponse, error) {
	cp, _ := domain.PrincipalFromContext(ctx)
	principal := cp.Name
	if err := h.sessions.CloseNotebookSession(ctx, req.NotebookId, req.SessionId, principal, cp.IsAdmin); err != nil {
		if resp, ok := respondDomainError[GenCloseNotebookSessionResponse](err, domainErrorResponder[GenCloseNotebookSessionResponse]{
			Forbidden: func(resp ForbiddenJSONResponse) GenCloseNotebookSessionResponse {
				return CloseNotebookSession403JSONResponse{resp}
			},
			NotFound: func(resp NotFoundJSONResponse) GenCloseNotebookSessionResponse {
				return CloseNotebookSession404JSONResponse{resp}
			},
		}); ok {
			return resp, nil
		}
		return nil, err
	}
	return GenCloseNotebookSession204Response{}, nil
}

// ExecuteCell implements the endpoint for executing a single cell in a session.
func (h *APIHandler) ExecuteCell(ctx context.Context, req GenExecuteCellRequest) (GenExecuteCellResponse, error) {
	cp, _ := domain.PrincipalFromContext(ctx)
	principal := cp.Name
	result, err := h.sessions.ExecuteNotebookCell(ctx, req.NotebookId, req.SessionId, req.CellId, principal, cp.IsAdmin)
	if err != nil {
		if resp, ok := respondDomainError[GenExecuteCellResponse](err, domainErrorResponder[GenExecuteCellResponse]{
			BadRequest: func(resp BadRequestJSONResponse) GenExecuteCellResponse { return ExecuteCell400JSONResponse{resp} },
			Forbidden:  func(resp ForbiddenJSONResponse) GenExecuteCellResponse { return ExecuteCell403JSONResponse{resp} },
			NotFound:   func(resp NotFoundJSONResponse) GenExecuteCellResponse { return ExecuteCell404JSONResponse{resp} },
		}); ok {
			return resp, nil
		}
		return nil, err
	}
	return ExecuteCell200JSONResponse{
		Body:    cellExecutionResultToAPI(*result),
		Headers: ExecuteCell200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// RunAllCells implements the endpoint for executing all SQL cells synchronously.
func (h *APIHandler) RunAllCells(ctx context.Context, req GenRunAllCellsRequest) (GenRunAllCellsResponse, error) {
	cp, _ := domain.PrincipalFromContext(ctx)
	principal := cp.Name
	result, err := h.sessions.RunAllNotebook(ctx, req.NotebookId, req.SessionId, principal, cp.IsAdmin)
	if err != nil {
		if resp, ok := respondDomainError[GenRunAllCellsResponse](err, domainErrorResponder[GenRunAllCellsResponse]{
			Forbidden: func(resp ForbiddenJSONResponse) GenRunAllCellsResponse { return RunAllCells403JSONResponse{resp} },
			NotFound:  func(resp NotFoundJSONResponse) GenRunAllCellsResponse { return RunAllCells404JSONResponse{resp} },
		}); ok {
			return resp, nil
		}
		return nil, err
	}
	return RunAllCells200JSONResponse{
		Body:    runAllResultToAPI(*result),
		Headers: RunAllCells200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// RunAllCellsAsync implements the endpoint for starting async execution of all cells.
func (h *APIHandler) RunAllCellsAsync(ctx context.Context, req GenRunAllCellsAsyncRequest) (GenRunAllCellsAsyncResponse, error) {
	cp, _ := domain.PrincipalFromContext(ctx)
	principal := cp.Name
	result, err := h.sessions.RunAllNotebookAsync(ctx, req.NotebookId, req.SessionId, principal, cp.IsAdmin)
	if err != nil {
		if resp, ok := respondDomainError[GenRunAllCellsAsyncResponse](err, domainErrorResponder[GenRunAllCellsAsyncResponse]{
			Forbidden: func(resp ForbiddenJSONResponse) GenRunAllCellsAsyncResponse {
				return RunAllCellsAsync403JSONResponse{resp}
			},
			NotFound: func(resp NotFoundJSONResponse) GenRunAllCellsAsyncResponse {
				return RunAllCellsAsync404JSONResponse{resp}
			},
		}); ok {
			return resp, nil
		}
		return nil, err
	}
	return RunAllCellsAsync202JSONResponse{
		Body:    notebookJobToAPI(*result),
		Headers: RunAllCellsAsync202ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// === Jobs ===

// ListNotebookJobs implements the endpoint for listing jobs for a notebook.
func (h *APIHandler) ListNotebookJobs(ctx context.Context, req GenListNotebookJobsRequest) (GenListNotebookJobsResponse, error) {
	page := pageFromParams(req.Params.MaxResults, req.Params.PageToken)
	cp, _ := domain.PrincipalFromContext(ctx)
	jobs, total, err := h.sessions.ListNotebookJobs(ctx, req.NotebookId, cp.Name, cp.IsAdmin, page)
	if err != nil {
		if resp, ok := respondDomainError[GenListNotebookJobsResponse](err, domainErrorResponder[GenListNotebookJobsResponse]{
			Forbidden: func(resp ForbiddenJSONResponse) GenListNotebookJobsResponse {
				return GenListNotebookJobs403JSONResponse{resp}
			},
			NotFound: func(resp NotFoundJSONResponse) GenListNotebookJobsResponse {
				return GenListNotebookJobs404JSONResponse{resp}
			},
		}); ok {
			return resp, nil
		}
		return nil, err
	}

	data := make([]NotebookJob, len(jobs))
	for i, j := range jobs {
		data[i] = notebookJobToAPI(j)
	}
	nextToken := domain.NextPageToken(page.Offset(), page.Limit(), total)
	return GenListNotebookJobs200JSONResponse{
		Body:    PaginatedNotebookJobs{Data: data, NextPageToken: optStr(nextToken)},
		Headers: GenListNotebookJobs200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// GetNotebookJob implements the endpoint for getting job status.
func (h *APIHandler) GetNotebookJob(ctx context.Context, req GenGetNotebookJobRequest) (GenGetNotebookJobResponse, error) {
	cp, _ := domain.PrincipalFromContext(ctx)
	result, err := h.sessions.GetNotebookJob(ctx, req.NotebookId, req.JobId, cp.Name, cp.IsAdmin)
	if err != nil {
		if resp, ok := respondDomainError[GenGetNotebookJobResponse](err, domainErrorResponder[GenGetNotebookJobResponse]{
			Forbidden: func(resp ForbiddenJSONResponse) GenGetNotebookJobResponse {
				return GenGetNotebookJob403JSONResponse{resp}
			},
			NotFound: func(resp NotFoundJSONResponse) GenGetNotebookJobResponse {
				return GenGetNotebookJob404JSONResponse{resp}
			},
		}); ok {
			return resp, nil
		}
		return nil, err
	}
	return GenGetNotebookJob200JSONResponse{
		Body:    notebookJobToAPI(*result),
		Headers: GenGetNotebookJob200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// === Git Repos ===

// ListGitRepos implements the endpoint for listing Git repositories.
func (h *APIHandler) ListGitRepos(ctx context.Context, req GenListGitReposRequest) (GenListGitReposResponse, error) {
	page := pageFromParams(req.Params.MaxResults, req.Params.PageToken)
	cp, _ := domain.PrincipalFromContext(ctx)
	repos, total, err := h.gitRepos.ListGitReposForPrincipal(ctx, cp.Name, cp.IsAdmin, page)
	if err != nil {
		if resp, ok := respondDomainError[GenListGitReposResponse](err, domainErrorResponder[GenListGitReposResponse]{
			Forbidden: func(resp ForbiddenJSONResponse) GenListGitReposResponse { return GenListGitRepos403JSONResponse{resp} },
		}); ok {
			return resp, nil
		}
		return nil, err
	}

	data := make([]GitRepo, len(repos))
	for i, r := range repos {
		data[i] = gitRepoToAPI(r)
	}
	nextToken := domain.NextPageToken(page.Offset(), page.Limit(), total)
	return GenListGitRepos200JSONResponse{
		Body:    PaginatedGitRepos{Data: data, NextPageToken: optStr(nextToken)},
		Headers: GenListGitRepos200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// CreateGitRepo implements the endpoint for registering a Git repository.
func (h *APIHandler) CreateGitRepo(ctx context.Context, req GenCreateGitRepoRequest) (GenCreateGitRepoResponse, error) {
	domReq := domain.CreateGitRepoRequest{
		URL:    req.Body.Url,
		Branch: req.Body.Branch,
	}
	if req.Body.AuthToken != nil {
		domReq.AuthToken = *req.Body.AuthToken
	}
	if req.Body.Path != nil {
		domReq.Path = *req.Body.Path
	}

	cp, _ := domain.PrincipalFromContext(ctx)
	principal := cp.Name
	result, err := h.gitRepos.CreateGitRepo(ctx, principal, domReq)
	if err != nil {
		if resp, ok := respondDomainError[GenCreateGitRepoResponse](err, domainErrorResponder[GenCreateGitRepoResponse]{
			BadRequest: func(resp BadRequestJSONResponse) GenCreateGitRepoResponse { return CreateGitRepo400JSONResponse{resp} },
			Conflict:   func(resp ConflictJSONResponse) GenCreateGitRepoResponse { return CreateGitRepo409JSONResponse{resp} },
		}); ok {
			return resp, nil
		}
		return CreateGitRepo400JSONResponse{badRequestErrorResponse(err)}, nil
	}
	return GenCreateGitRepo201JSONResponse{
		Body:    gitRepoToAPI(*result),
		Headers: GenCreateGitRepo201ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// GetGitRepo implements the endpoint for retrieving a Git repository.
func (h *APIHandler) GetGitRepo(ctx context.Context, req GenGetGitRepoRequest) (GenGetGitRepoResponse, error) {
	cp, _ := domain.PrincipalFromContext(ctx)
	result, err := h.gitRepos.GetGitRepoForPrincipal(ctx, cp.Name, cp.IsAdmin, req.GitRepoId)
	if err != nil {
		if resp, ok := respondDomainError[GenGetGitRepoResponse](err, domainErrorResponder[GenGetGitRepoResponse]{
			Forbidden: func(resp ForbiddenJSONResponse) GenGetGitRepoResponse { return GenGetGitRepo403JSONResponse{resp} },
			NotFound:  func(resp NotFoundJSONResponse) GenGetGitRepoResponse { return GenGetGitRepo404JSONResponse{resp} },
		}); ok {
			return resp, nil
		}
		return nil, err
	}
	return GenGetGitRepo200JSONResponse{
		Body:    gitRepoToAPI(*result),
		Headers: GenGetGitRepo200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// DeleteGitRepo implements the endpoint for deleting a Git repository.
func (h *APIHandler) DeleteGitRepo(ctx context.Context, req GenDeleteGitRepoRequest) (GenDeleteGitRepoResponse, error) {
	cp, _ := domain.PrincipalFromContext(ctx)
	principal := cp.Name
	isAdmin := cp.IsAdmin

	if err := h.gitRepos.DeleteGitRepo(ctx, principal, isAdmin, req.GitRepoId); err != nil {
		if resp, ok := respondDomainError[GenDeleteGitRepoResponse](err, domainErrorResponder[GenDeleteGitRepoResponse]{
			Forbidden: func(resp ForbiddenJSONResponse) GenDeleteGitRepoResponse { return DeleteGitRepo403JSONResponse{resp} },
			NotFound:  func(resp NotFoundJSONResponse) GenDeleteGitRepoResponse { return DeleteGitRepo404JSONResponse{resp} },
		}); ok {
			return resp, nil
		}
		return nil, err
	}
	return GenDeleteGitRepo204Response{}, nil
}

// SyncGitRepo implements the endpoint for triggering a Git sync.
func (h *APIHandler) SyncGitRepo(ctx context.Context, req GenSyncGitRepoRequest) (GenSyncGitRepoResponse, error) {
	cp, _ := domain.PrincipalFromContext(ctx)
	result, err := h.gitRepos.SyncGitRepo(ctx, cp.Name, cp.IsAdmin, req.GitRepoId)
	if err != nil {
		if resp, ok := respondDomainError[GenSyncGitRepoResponse](err, domainErrorResponder[GenSyncGitRepoResponse]{
			Forbidden: func(resp ForbiddenJSONResponse) GenSyncGitRepoResponse { return SyncGitRepo403JSONResponse{resp} },
			NotFound: func(resp NotFoundJSONResponse) GenSyncGitRepoResponse { return SyncGitRepo404JSONResponse{resp} },
			Internal: func(resp InternalErrorJSONResponse) GenSyncGitRepoResponse {
				return GenSyncGitRepo500JSONResponse{resp}
			},
		}); ok {
			return resp, nil
		}
		return nil, err
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
	return Notebook{
		Id:          &nb.ID,
		Name:        &nb.Name,
		Description: nb.Description,
		Owner:       &nb.Owner,
		CreatedAt:   formatTimePtr(&nb.CreatedAt),
		UpdatedAt:   formatTimePtr(&nb.UpdatedAt),
	}
}

func cellToAPI(c domain.Cell) Cell {
	cellType := CellCellType(c.CellType)
	pos := int32(c.Position) //nolint:gosec // positions are small ints
	var role *CellRole
	if c.Role != "" {
		r := CellRole(c.Role)
		role = &r
	}
	var disabled *bool
	if c.Disabled {
		disabledValue := true
		disabled = &disabledValue
	}
	return Cell{
		Id:         &c.ID,
		NotebookId: &c.NotebookID,
		CellType:   &cellType,
		Name:       c.Name,
		Role:       role,
		Disabled:   disabled,
		Test:       notebookCellTestConfigToAPI(c.Test),
		VisualSpec: visualSpecToAPI(c.VisualSpec),
		Content:    &c.Content,
		Position:   &pos,
		LastResult: c.LastResult,
		CreatedAt:  formatTimePtr(&c.CreatedAt),
		UpdatedAt:  formatTimePtr(&c.UpdatedAt),
	}
}

func notebookPublishModelToAPI(model *domain.NotebookPublishModel) *NotebookPublishModel {
	if model == nil {
		return nil
	}
	materialization := ModelMaterialization(model.Materialization)
	return &NotebookPublishModel{
		ProjectName:     &model.ProjectName,
		Name:            &model.Name,
		Materialization: &materialization,
		OutputCellId:    &model.OutputCellID,
	}
}

func notebookCellTestConfigFromAPI(cfg *NotebookCellTestConfig) *domain.NotebookCellTestConfig {
	if cfg == nil {
		return nil
	}
	out := &domain.NotebookCellTestConfig{}
	if cfg.Severity != nil {
		out.Severity = domain.NotebookTestSeverity(*cfg.Severity)
	}
	return out
}

func notebookCellTestConfigToAPI(cfg *domain.NotebookCellTestConfig) *NotebookCellTestConfig {
	if cfg == nil {
		return nil
	}
	out := &NotebookCellTestConfig{}
	if cfg.Severity != "" {
		severity := NotebookTestSeverity(cfg.Severity)
		out.Severity = &severity
	}
	return out
}

func sessionToAPI(s domain.NotebookSession) NotebookSession {
	state := NotebookSessionState(s.State)
	return NotebookSession{
		Id:         &s.ID,
		NotebookId: &s.NotebookID,
		Principal:  &s.Principal,
		State:      &state,
		CreatedAt:  formatTimePtr(&s.CreatedAt),
		LastUsedAt: formatTimePtr(&s.LastUsedAt),
	}
}

func cellExecutionResultToAPI(r domain.CellExecutionResult) CellExecutionResult {
	durationMs := safeInt64ToInt32(r.Duration.Milliseconds())
	rowCount := int32(r.RowCount) //nolint:gosec // row counts are small
	return CellExecutionResult{
		CellId:     &r.CellID,
		Columns:    ptrTabularColumns(tabularColumns(r.Columns, r.Rows)),
		Rows:       ptrRecords(rowsToRecords(r.Columns, r.Rows)),
		RowCount:   &rowCount,
		Error:      r.Error,
		DurationMs: &durationMs,
	}
}

func runAllResultToAPI(r domain.RunAllResult) RunAllResult {
	totalMs := safeInt64ToInt32(r.TotalDuration.Milliseconds())
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
	state := NotebookJobState(j.State)
	return NotebookJob{
		Id:         &j.ID,
		NotebookId: &j.NotebookID,
		SessionId:  &j.SessionID,
		State:      &state,
		Result:     j.Result,
		Error:      j.Error,
		CreatedAt:  formatTimePtr(&j.CreatedAt),
		UpdatedAt:  formatTimePtr(&j.UpdatedAt),
	}
}

func gitRepoToAPI(r domain.GitRepo) GitRepo {
	return GitRepo{
		Id:         &r.ID,
		Url:        &r.URL,
		Branch:     &r.Branch,
		Path:       &r.Path,
		Owner:      &r.Owner,
		LastSyncAt: formatTimePtr(r.LastSyncAt),
		LastCommit: r.LastCommit,
		CreatedAt:  formatTimePtr(&r.CreatedAt),
		UpdatedAt:  formatTimePtr(&r.UpdatedAt),
	}
}
