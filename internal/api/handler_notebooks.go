package api

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"

	"duck-demo/internal/domain"
)

// notebookService defines the notebook operations used by the API handler.
type notebookService interface {
	CreateNotebook(ctx context.Context, principal string, req domain.CreateNotebookRequest) (*domain.Notebook, error)
	GetNotebook(ctx context.Context, id string) (*domain.Notebook, []domain.Cell, error)
	GetNotebookForPrincipal(ctx context.Context, principal string, isAdmin bool, id string) (*domain.Notebook, []domain.Cell, error)
	GetNotebookContext(ctx context.Context, principal string, isAdmin bool, id string) (*domain.NotebookContext, error)
	GetPublishModel(ctx context.Context, notebookID string) (*domain.NotebookPublishModel, error)
	ListNotebooks(ctx context.Context, owner *string, page domain.PageRequest) ([]domain.Notebook, int64, error)
	ListNotebooksForPrincipal(ctx context.Context, principal string, isAdmin bool, owner *string, page domain.PageRequest) ([]domain.Notebook, int64, error)
	UpdateNotebook(ctx context.Context, principal string, isAdmin bool, id string, req domain.UpdateNotebookRequest) (*domain.Notebook, error)
	MoveNotebook(ctx context.Context, principal string, isAdmin bool, id string, req domain.MoveNotebookRequest) (*domain.Notebook, error)
	DuplicateNotebook(ctx context.Context, principal string, isAdmin bool, id string, req domain.DuplicateNotebookRequest) (*domain.Notebook, error)
	DeleteNotebook(ctx context.Context, principal string, isAdmin bool, id string) error
	ListNotebookShares(ctx context.Context, principal string, isAdmin bool, notebookID string) ([]domain.NotebookShare, error)
	ShareNotebook(ctx context.Context, principal string, isAdmin bool, notebookID string, share domain.NotebookShare) (*domain.NotebookShare, error)
	UnshareNotebook(ctx context.Context, principal string, isAdmin bool, notebookID string, principalName string) error
	CreateCell(ctx context.Context, principal string, isAdmin bool, notebookID string, req domain.CreateCellRequest) (*domain.Cell, error)
	UpdateCell(ctx context.Context, principal string, isAdmin bool, cellID string, req domain.UpdateCellRequest) (*domain.Cell, error)
	DeleteCell(ctx context.Context, principal string, isAdmin bool, cellID string) error
	ReorderCells(ctx context.Context, principal string, isAdmin bool, notebookID string, req domain.ReorderCellsRequest) ([]domain.Cell, error)
}

type notebookFolderService interface {
	CreateFolder(ctx context.Context, principal string, req domain.CreateFolderRequest) (*domain.Folder, error)
	GetFolderForPrincipal(ctx context.Context, principal string, isAdmin bool, id string) (*domain.Folder, error)
	ListFoldersForPrincipal(ctx context.Context, principal string, isAdmin bool, owner *string) ([]domain.Folder, error)
	UpdateFolder(ctx context.Context, principal string, isAdmin bool, id string, req domain.UpdateFolderRequest) (*domain.Folder, error)
	MoveFolder(ctx context.Context, principal string, isAdmin bool, id string, req domain.MoveFolderRequest) (*domain.Folder, error)
	DeleteFolder(ctx context.Context, principal string, isAdmin bool, id string) error
	ListFolderShares(ctx context.Context, principal string, isAdmin bool, folderID string) ([]domain.FolderShare, error)
	ShareFolder(ctx context.Context, principal string, isAdmin bool, folderID string, share domain.FolderShare) (*domain.FolderShare, error)
	UnshareFolder(ctx context.Context, principal string, isAdmin bool, folderID string, principalName string) error
}

type exploreService interface {
	List(ctx context.Context, principal string, isAdmin bool, filter domain.ExploreFilter) ([]domain.ExploreItem, error)
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

// ListNotebookFolders implements the endpoint for listing notebook folders.
func (h *APIHandler) ListNotebookFolders(ctx context.Context, req GenListNotebookFoldersRequest) (GenListNotebookFoldersResponse, error) {
	if h.notebookFolders == nil {
		return nil, domain.ErrNotImplemented("notebook folders are not configured")
	}
	cp, _ := domain.PrincipalFromContext(ctx)
	page := pageFromParams(req.Params.MaxResults, req.Params.PageToken)
	items, err := h.notebookFolders.ListFoldersForPrincipal(ctx, cp.Name, cp.IsAdmin, req.Params.Owner)
	if err != nil {
		if resp, ok := respondDomainErrorForOperation[GenListNotebookFoldersResponse]("listNotebookFolders", err, domainErrorResponder[GenListNotebookFoldersResponse]{
			Forbidden: func(resp ForbiddenJSONResponse) GenListNotebookFoldersResponse {
				return ListNotebookFolders403JSONResponse{resp}
			},
		}); ok {
			return resp, nil
		}
		return nil, err
	}
	total := int64(len(items))
	start := page.Offset()
	if start > len(items) {
		start = len(items)
	}
	end := start + page.Limit()
	if end > len(items) {
		end = len(items)
	}
	data := make([]Folder, 0, end-start)
	for _, item := range items[start:end] {
		data = append(data, folderToAPI(item))
	}
	nextToken := domain.NextPageToken(page.Offset(), page.Limit(), total)
	return GenListNotebookFolders200JSONResponse{
		Body:    PaginatedFolders{Data: data, NextPageToken: optStr(nextToken)},
		Headers: GenListNotebookFolders200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// CreateNotebookFolder implements the endpoint for creating a notebook folder.
func (h *APIHandler) CreateNotebookFolder(ctx context.Context, req GenCreateNotebookFolderRequest) (GenCreateNotebookFolderResponse, error) {
	if h.notebookFolders == nil {
		return nil, domain.ErrNotImplemented("notebook folders are not configured")
	}
	cp, _ := domain.PrincipalFromContext(ctx)
	result, err := h.notebookFolders.CreateFolder(ctx, cp.Name, domain.CreateFolderRequest{
		Name:                 req.Body.Name,
		ParentFolderID:       req.Body.ParentFolderId,
		GitRepoID:            req.Body.GitRepoId,
		GitRootPath:          req.Body.GitRootPath,
		DefaultProjectID:     req.Body.DefaultProjectId,
		DefaultEnvironmentID: req.Body.DefaultEnvironmentId,
	})
	if err != nil {
		if resp, ok := respondDomainErrorForOperation[GenCreateNotebookFolderResponse]("createNotebookFolder", err, domainErrorResponder[GenCreateNotebookFolderResponse]{
			BadRequest: func(resp BadRequestJSONResponse) GenCreateNotebookFolderResponse {
				return CreateNotebookFolder400JSONResponse{resp}
			},
			Conflict: func(resp ConflictJSONResponse) GenCreateNotebookFolderResponse {
				return CreateNotebookFolder409JSONResponse{resp}
			},
			Forbidden: func(resp ForbiddenJSONResponse) GenCreateNotebookFolderResponse {
				return CreateNotebookFolder403JSONResponse{resp}
			},
		}); ok {
			return resp, nil
		}
		return nil, err
	}
	return GenCreateNotebookFolder201JSONResponse{
		Body:    folderToAPI(*result),
		Headers: GenCreateNotebookFolder201ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// GetNotebookFolder implements the endpoint for retrieving a notebook folder.
func (h *APIHandler) GetNotebookFolder(ctx context.Context, req GenGetNotebookFolderRequest) (GenGetNotebookFolderResponse, error) {
	if h.notebookFolders == nil {
		return nil, domain.ErrNotImplemented("notebook folders are not configured")
	}
	cp, _ := domain.PrincipalFromContext(ctx)
	result, err := h.notebookFolders.GetFolderForPrincipal(ctx, cp.Name, cp.IsAdmin, req.FolderId)
	if err != nil {
		if resp, ok := respondDomainErrorForOperation[GenGetNotebookFolderResponse]("getNotebookFolder", err, domainErrorResponder[GenGetNotebookFolderResponse]{
			Forbidden: func(resp ForbiddenJSONResponse) GenGetNotebookFolderResponse {
				return GetNotebookFolder403JSONResponse{resp}
			},
			NotFound: func(resp NotFoundJSONResponse) GenGetNotebookFolderResponse {
				return GetNotebookFolder404JSONResponse{resp}
			},
		}); ok {
			return resp, nil
		}
		return nil, err
	}
	return GenGetNotebookFolder200JSONResponse{
		Body:    folderToAPI(*result),
		Headers: GenGetNotebookFolder200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// UpdateNotebookFolder implements the endpoint for updating a notebook folder.
func (h *APIHandler) UpdateNotebookFolder(ctx context.Context, req GenUpdateNotebookFolderRequest) (GenUpdateNotebookFolderResponse, error) {
	if h.notebookFolders == nil {
		return nil, domain.ErrNotImplemented("notebook folders are not configured")
	}
	cp, _ := domain.PrincipalFromContext(ctx)
	result, err := h.notebookFolders.UpdateFolder(ctx, cp.Name, cp.IsAdmin, req.FolderId, domain.UpdateFolderRequest{
		Name:                 req.Body.Name,
		GitRepoID:            req.Body.GitRepoId,
		GitRootPath:          req.Body.GitRootPath,
		DefaultProjectID:     req.Body.DefaultProjectId,
		DefaultEnvironmentID: req.Body.DefaultEnvironmentId,
	})
	if err != nil {
		if resp, ok := respondDomainErrorForOperation[GenUpdateNotebookFolderResponse]("updateNotebookFolder", err, domainErrorResponder[GenUpdateNotebookFolderResponse]{
			BadRequest: func(resp BadRequestJSONResponse) GenUpdateNotebookFolderResponse {
				return UpdateNotebookFolder400JSONResponse{resp}
			},
			Forbidden: func(resp ForbiddenJSONResponse) GenUpdateNotebookFolderResponse {
				return UpdateNotebookFolder403JSONResponse{resp}
			},
			NotFound: func(resp NotFoundJSONResponse) GenUpdateNotebookFolderResponse {
				return UpdateNotebookFolder404JSONResponse{resp}
			},
			Conflict: func(resp ConflictJSONResponse) GenUpdateNotebookFolderResponse {
				return UpdateNotebookFolder409JSONResponse{resp}
			},
		}); ok {
			return resp, nil
		}
		return nil, err
	}
	return GenUpdateNotebookFolder200JSONResponse{
		Body:    folderToAPI(*result),
		Headers: GenUpdateNotebookFolder200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// MoveNotebookFolder implements the endpoint for re-parenting a notebook folder subtree.
func (h *APIHandler) MoveNotebookFolder(ctx context.Context, req GenMoveNotebookFolderRequest) (GenMoveNotebookFolderResponse, error) {
	if h.notebookFolders == nil {
		return nil, domain.ErrNotImplemented("notebook folders are not configured")
	}
	cp, _ := domain.PrincipalFromContext(ctx)
	result, err := h.notebookFolders.MoveFolder(ctx, cp.Name, cp.IsAdmin, req.FolderId, domain.MoveFolderRequest{
		ParentFolderID:       req.Body.ParentFolderId,
		ConfirmLeaveGit:      req.Body.ConfirmLeaveGit != nil && *req.Body.ConfirmLeaveGit,
		ConfirmContextChange: req.Body.ConfirmContextChange != nil && *req.Body.ConfirmContextChange,
	})
	if err != nil {
		if resp, ok := respondDomainErrorForOperation[GenMoveNotebookFolderResponse]("moveNotebookFolder", err, domainErrorResponder[GenMoveNotebookFolderResponse]{
			BadRequest: func(resp BadRequestJSONResponse) GenMoveNotebookFolderResponse {
				return MoveNotebookFolder400JSONResponse{resp}
			},
			Forbidden: func(resp ForbiddenJSONResponse) GenMoveNotebookFolderResponse {
				return MoveNotebookFolder403JSONResponse{resp}
			},
			NotFound: func(resp NotFoundJSONResponse) GenMoveNotebookFolderResponse {
				return MoveNotebookFolder404JSONResponse{resp}
			},
			Conflict: func(resp ConflictJSONResponse) GenMoveNotebookFolderResponse {
				return MoveNotebookFolder409JSONResponse{resp}
			},
		}); ok {
			return resp, nil
		}
		return nil, err
	}
	return GenMoveNotebookFolder200JSONResponse{
		Body:    folderToAPI(*result),
		Headers: GenMoveNotebookFolder200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// DeleteNotebookFolder implements the endpoint for deleting a notebook folder.
func (h *APIHandler) DeleteNotebookFolder(ctx context.Context, req GenDeleteNotebookFolderRequest) (GenDeleteNotebookFolderResponse, error) {
	if h.notebookFolders == nil {
		return nil, domain.ErrNotImplemented("notebook folders are not configured")
	}
	cp, _ := domain.PrincipalFromContext(ctx)
	if err := h.notebookFolders.DeleteFolder(ctx, cp.Name, cp.IsAdmin, req.FolderId); err != nil {
		if resp, ok := respondDomainErrorForOperation[GenDeleteNotebookFolderResponse]("deleteNotebookFolder", err, domainErrorResponder[GenDeleteNotebookFolderResponse]{
			Forbidden: func(resp ForbiddenJSONResponse) GenDeleteNotebookFolderResponse {
				return DeleteNotebookFolder403JSONResponse{resp}
			},
			NotFound: func(resp NotFoundJSONResponse) GenDeleteNotebookFolderResponse {
				return DeleteNotebookFolder404JSONResponse{resp}
			},
			Conflict: func(resp ConflictJSONResponse) GenDeleteNotebookFolderResponse {
				return DeleteNotebookFolder409JSONResponse{resp}
			},
		}); ok {
			return resp, nil
		}
		return nil, err
	}
	return GenDeleteNotebookFolder204Response{}, nil
}

// ListNotebookFolderShares implements the endpoint for listing explicit folder shares.
func (h *APIHandler) ListNotebookFolderShares(ctx context.Context, req GenListNotebookFolderSharesRequest) (GenListNotebookFolderSharesResponse, error) {
	if h.notebookFolders == nil {
		return nil, domain.ErrNotImplemented("notebook folders are not configured")
	}
	cp, _ := domain.PrincipalFromContext(ctx)
	items, err := h.notebookFolders.ListFolderShares(ctx, cp.Name, cp.IsAdmin, req.FolderId)
	if err != nil {
		if resp, ok := respondDomainErrorForOperation[GenListNotebookFolderSharesResponse]("listNotebookFolderShares", err, domainErrorResponder[GenListNotebookFolderSharesResponse]{
			Forbidden: func(resp ForbiddenJSONResponse) GenListNotebookFolderSharesResponse {
				return ListNotebookFolderShares403JSONResponse{resp}
			},
			NotFound: func(resp NotFoundJSONResponse) GenListNotebookFolderSharesResponse {
				return ListNotebookFolderShares404JSONResponse{resp}
			},
		}); ok {
			return resp, nil
		}
		return nil, err
	}
	data := make([]FolderShare, 0, len(items))
	for _, item := range items {
		data = append(data, folderShareToAPI(item))
	}
	return GenListNotebookFolderShares200JSONResponse{
		Body:    data,
		Headers: GenListNotebookFolderShares200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// ShareNotebookFolder implements the endpoint for granting folder access.
func (h *APIHandler) ShareNotebookFolder(ctx context.Context, req GenShareNotebookFolderRequest) (GenShareNotebookFolderResponse, error) {
	if h.notebookFolders == nil {
		return nil, domain.ErrNotImplemented("notebook folders are not configured")
	}
	cp, _ := domain.PrincipalFromContext(ctx)
	share, err := h.notebookFolders.ShareFolder(ctx, cp.Name, cp.IsAdmin, req.FolderId, domain.FolderShare{
		PrincipalName: req.Body.PrincipalName,
		Role:          notebookShareRoleFromAPI(req.Body.Role),
	})
	if err != nil {
		if resp, ok := respondDomainErrorForOperation[GenShareNotebookFolderResponse]("shareNotebookFolder", err, domainErrorResponder[GenShareNotebookFolderResponse]{
			BadRequest: func(resp BadRequestJSONResponse) GenShareNotebookFolderResponse {
				return ShareNotebookFolder400JSONResponse{resp}
			},
			Forbidden: func(resp ForbiddenJSONResponse) GenShareNotebookFolderResponse {
				return ShareNotebookFolder403JSONResponse{resp}
			},
			NotFound: func(resp NotFoundJSONResponse) GenShareNotebookFolderResponse {
				return ShareNotebookFolder404JSONResponse{resp}
			},
			Conflict: func(resp ConflictJSONResponse) GenShareNotebookFolderResponse {
				return ShareNotebookFolder409JSONResponse{resp}
			},
		}); ok {
			return resp, nil
		}
		return nil, err
	}
	return GenShareNotebookFolder200JSONResponse{
		Body:    folderShareToAPI(*share),
		Headers: GenShareNotebookFolder200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// UnshareNotebookFolder implements the endpoint for removing a folder share.
func (h *APIHandler) UnshareNotebookFolder(ctx context.Context, req GenUnshareNotebookFolderRequest) (GenUnshareNotebookFolderResponse, error) {
	if h.notebookFolders == nil {
		return nil, domain.ErrNotImplemented("notebook folders are not configured")
	}
	cp, _ := domain.PrincipalFromContext(ctx)
	if err := h.notebookFolders.UnshareFolder(ctx, cp.Name, cp.IsAdmin, req.FolderId, req.PrincipalName); err != nil {
		if resp, ok := respondDomainErrorForOperation[GenUnshareNotebookFolderResponse]("unshareNotebookFolder", err, domainErrorResponder[GenUnshareNotebookFolderResponse]{
			Forbidden: func(resp ForbiddenJSONResponse) GenUnshareNotebookFolderResponse {
				return UnshareNotebookFolder403JSONResponse{resp}
			},
			NotFound: func(resp NotFoundJSONResponse) GenUnshareNotebookFolderResponse {
				return UnshareNotebookFolder404JSONResponse{resp}
			},
			Conflict: func(resp ConflictJSONResponse) GenUnshareNotebookFolderResponse {
				return UnshareNotebookFolder409JSONResponse{resp}
			},
		}); ok {
			return resp, nil
		}
		return nil, err
	}
	return GenUnshareNotebookFolder204Response{}, nil
}

// ListNotebooks implements the endpoint for listing notebooks.
func (h *APIHandler) ListNotebooks(ctx context.Context, req GenListNotebooksRequest) (GenListNotebooksResponse, error) {
	page := pageFromParams(req.Params.MaxResults, req.Params.PageToken)
	cp, _ := domain.PrincipalFromContext(ctx)
	nbs, total, err := h.notebooks.ListNotebooksForPrincipal(ctx, cp.Name, cp.IsAdmin, req.Params.Owner, page)
	if err != nil {
		if resp, ok := respondDomainErrorForOperation[GenListNotebooksResponse]("listNotebooks", err, domainErrorResponder[GenListNotebooksResponse]{
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
		FolderID:    req.Body.FolderId,
	}

	cp, _ := domain.PrincipalFromContext(ctx)
	principal := cp.Name
	result, err := h.notebooks.CreateNotebook(ctx, principal, domReq)
	if err != nil {
		if resp, ok := respondDomainErrorForOperation[GenCreateNotebookResponse]("createNotebook", err, domainErrorResponder[GenCreateNotebookResponse]{
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
		if resp, ok := respondDomainErrorForOperation[GenGetNotebookResponse]("getNotebook", err, domainErrorResponder[GenGetNotebookResponse]{
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
	notebookContext, err := h.notebooks.GetNotebookContext(ctx, cp.Name, cp.IsAdmin, req.NotebookId)
	if err != nil {
		if resp, ok := respondDomainErrorForOperation[GenGetNotebookResponse]("getNotebook", err, domainErrorResponder[GenGetNotebookResponse]{
			Forbidden: func(resp ForbiddenJSONResponse) GenGetNotebookResponse { return GenGetNotebook403JSONResponse{resp} },
			NotFound:  func(resp NotFoundJSONResponse) GenGetNotebookResponse { return GenGetNotebook404JSONResponse{resp} },
		}); ok {
			return resp, nil
		}
		return nil, err
	}
	shareItems, err := h.notebooks.ListNotebookShares(ctx, cp.Name, cp.IsAdmin, req.NotebookId)
	if err != nil {
		if resp, ok := respondDomainErrorForOperation[GenGetNotebookResponse]("getNotebook", err, domainErrorResponder[GenGetNotebookResponse]{
			Forbidden: func(resp ForbiddenJSONResponse) GenGetNotebookResponse { return GenGetNotebook403JSONResponse{resp} },
			NotFound:  func(resp NotFoundJSONResponse) GenGetNotebookResponse { return GenGetNotebook404JSONResponse{resp} },
		}); ok {
			return resp, nil
		}
		return nil, err
	}
	apiShares := make([]NotebookShare, 0, len(shareItems))
	for _, item := range shareItems {
		apiShares = append(apiShares, notebookShareToAPI(item))
	}
	return GenGetNotebook200JSONResponse{
		Body:    NotebookDetail{Notebook: &apiNb, Cells: &apiCells, Context: notebookContextToAPI(notebookContext), Shares: &apiShares, PublishModel: publishModel},
		Headers: GenGetNotebook200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// UpdateNotebook implements the endpoint for updating notebook metadata.
func (h *APIHandler) UpdateNotebook(ctx context.Context, req GenUpdateNotebookRequest) (GenUpdateNotebookResponse, error) {
	domReq := domain.UpdateNotebookRequest{
		Name:                  req.Body.Name,
		Description:           req.Body.Description,
		ProjectOverrideID:     req.Body.ProjectOverrideId,
		EnvironmentOverrideID: req.Body.EnvironmentOverrideId,
	}

	cp, _ := domain.PrincipalFromContext(ctx)
	principal := cp.Name
	isAdmin := cp.IsAdmin

	result, err := h.notebooks.UpdateNotebook(ctx, principal, isAdmin, req.NotebookId, domReq)
	if err != nil {
		if resp, ok := respondDomainErrorForOperation[GenUpdateNotebookResponse]("updateNotebook", err, domainErrorResponder[GenUpdateNotebookResponse]{
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

// MoveNotebook implements the endpoint for moving a notebook.
func (h *APIHandler) MoveNotebook(ctx context.Context, req GenMoveNotebookRequest) (GenMoveNotebookResponse, error) {
	cp, _ := domain.PrincipalFromContext(ctx)
	result, err := h.notebooks.MoveNotebook(ctx, cp.Name, cp.IsAdmin, req.NotebookId, domain.MoveNotebookRequest{
		FolderID:             req.Body.FolderId,
		GitPath:              req.Body.GitPath,
		ConfirmLeaveGit:      req.Body.ConfirmLeaveGit != nil && *req.Body.ConfirmLeaveGit,
		ConfirmContextChange: req.Body.ConfirmContextChange != nil && *req.Body.ConfirmContextChange,
	})
	if err != nil {
		if resp, ok := respondDomainErrorForOperation[GenMoveNotebookResponse]("moveNotebook", err, domainErrorResponder[GenMoveNotebookResponse]{
			BadRequest: func(resp BadRequestJSONResponse) GenMoveNotebookResponse { return MoveNotebook400JSONResponse{resp} },
			Forbidden:  func(resp ForbiddenJSONResponse) GenMoveNotebookResponse { return MoveNotebook403JSONResponse{resp} },
			NotFound:   func(resp NotFoundJSONResponse) GenMoveNotebookResponse { return MoveNotebook404JSONResponse{resp} },
			Conflict:   func(resp ConflictJSONResponse) GenMoveNotebookResponse { return MoveNotebook409JSONResponse{resp} },
		}); ok {
			return resp, nil
		}
		return nil, err
	}
	return GenMoveNotebook200JSONResponse{
		Body:    notebookToAPI(*result),
		Headers: GenMoveNotebook200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// DuplicateNotebook implements the endpoint for duplicating a notebook.
func (h *APIHandler) DuplicateNotebook(ctx context.Context, req GenDuplicateNotebookRequest) (GenDuplicateNotebookResponse, error) {
	cp, _ := domain.PrincipalFromContext(ctx)
	result, err := h.notebooks.DuplicateNotebook(ctx, cp.Name, cp.IsAdmin, req.NotebookId, domain.DuplicateNotebookRequest{
		FolderID: req.Body.FolderId,
		Name:     req.Body.Name,
		GitPath:  req.Body.GitPath,
	})
	if err != nil {
		if resp, ok := respondDomainErrorForOperation[GenDuplicateNotebookResponse]("duplicateNotebook", err, domainErrorResponder[GenDuplicateNotebookResponse]{
			BadRequest: func(resp BadRequestJSONResponse) GenDuplicateNotebookResponse {
				return DuplicateNotebook400JSONResponse{resp}
			},
			Forbidden: func(resp ForbiddenJSONResponse) GenDuplicateNotebookResponse {
				return DuplicateNotebook403JSONResponse{resp}
			},
			NotFound: func(resp NotFoundJSONResponse) GenDuplicateNotebookResponse {
				return DuplicateNotebook404JSONResponse{resp}
			},
			Conflict: func(resp ConflictJSONResponse) GenDuplicateNotebookResponse {
				return DuplicateNotebook409JSONResponse{resp}
			},
		}); ok {
			return resp, nil
		}
		return nil, err
	}
	return GenDuplicateNotebook201JSONResponse{
		Body:    notebookToAPI(*result),
		Headers: GenDuplicateNotebook201ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// DeleteNotebook implements the endpoint for deleting a notebook.
func (h *APIHandler) DeleteNotebook(ctx context.Context, req GenDeleteNotebookRequest) (GenDeleteNotebookResponse, error) {
	cp, _ := domain.PrincipalFromContext(ctx)
	principal := cp.Name
	isAdmin := cp.IsAdmin

	if err := h.notebooks.DeleteNotebook(ctx, principal, isAdmin, req.NotebookId); err != nil {
		if resp, ok := respondDomainErrorForOperation[GenDeleteNotebookResponse]("deleteNotebook", err, domainErrorResponder[GenDeleteNotebookResponse]{
			Forbidden: func(resp ForbiddenJSONResponse) GenDeleteNotebookResponse { return DeleteNotebook403JSONResponse{resp} },
			NotFound:  func(resp NotFoundJSONResponse) GenDeleteNotebookResponse { return DeleteNotebook404JSONResponse{resp} },
		}); ok {
			return resp, nil
		}
		return nil, err
	}
	return GenDeleteNotebook204Response{}, nil
}

// ListNotebookShares implements the endpoint for listing direct notebook shares.
func (h *APIHandler) ListNotebookShares(ctx context.Context, req GenListNotebookSharesRequest) (GenListNotebookSharesResponse, error) {
	cp, _ := domain.PrincipalFromContext(ctx)
	items, err := h.notebooks.ListNotebookShares(ctx, cp.Name, cp.IsAdmin, req.NotebookId)
	if err != nil {
		if resp, ok := respondDomainErrorForOperation[GenListNotebookSharesResponse]("listNotebookShares", err, domainErrorResponder[GenListNotebookSharesResponse]{
			Forbidden: func(resp ForbiddenJSONResponse) GenListNotebookSharesResponse {
				return ListNotebookShares403JSONResponse{resp}
			},
			NotFound: func(resp NotFoundJSONResponse) GenListNotebookSharesResponse {
				return ListNotebookShares404JSONResponse{resp}
			},
		}); ok {
			return resp, nil
		}
		return nil, err
	}
	data := make([]NotebookShare, 0, len(items))
	for _, item := range items {
		data = append(data, notebookShareToAPI(item))
	}
	return GenListNotebookShares200JSONResponse{
		Body:    data,
		Headers: GenListNotebookShares200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// ShareNotebook implements the endpoint for granting direct notebook access.
func (h *APIHandler) ShareNotebook(ctx context.Context, req GenShareNotebookRequest) (GenShareNotebookResponse, error) {
	cp, _ := domain.PrincipalFromContext(ctx)
	share, err := h.notebooks.ShareNotebook(ctx, cp.Name, cp.IsAdmin, req.NotebookId, domain.NotebookShare{
		PrincipalName: req.Body.PrincipalName,
		Role:          notebookShareRoleFromAPI(req.Body.Role),
	})
	if err != nil {
		if resp, ok := respondDomainErrorForOperation[GenShareNotebookResponse]("shareNotebook", err, domainErrorResponder[GenShareNotebookResponse]{
			BadRequest: func(resp BadRequestJSONResponse) GenShareNotebookResponse { return ShareNotebook400JSONResponse{resp} },
			Forbidden:  func(resp ForbiddenJSONResponse) GenShareNotebookResponse { return ShareNotebook403JSONResponse{resp} },
			NotFound:   func(resp NotFoundJSONResponse) GenShareNotebookResponse { return ShareNotebook404JSONResponse{resp} },
			Conflict:   func(resp ConflictJSONResponse) GenShareNotebookResponse { return ShareNotebook409JSONResponse{resp} },
		}); ok {
			return resp, nil
		}
		return nil, err
	}
	return GenShareNotebook200JSONResponse{
		Body:    notebookShareToAPI(*share),
		Headers: GenShareNotebook200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// UnshareNotebook implements the endpoint for removing a direct notebook share.
func (h *APIHandler) UnshareNotebook(ctx context.Context, req GenUnshareNotebookRequest) (GenUnshareNotebookResponse, error) {
	cp, _ := domain.PrincipalFromContext(ctx)
	if err := h.notebooks.UnshareNotebook(ctx, cp.Name, cp.IsAdmin, req.NotebookId, req.PrincipalName); err != nil {
		if resp, ok := respondDomainErrorForOperation[GenUnshareNotebookResponse]("unshareNotebook", err, domainErrorResponder[GenUnshareNotebookResponse]{
			Forbidden: func(resp ForbiddenJSONResponse) GenUnshareNotebookResponse {
				return UnshareNotebook403JSONResponse{resp}
			},
			NotFound: func(resp NotFoundJSONResponse) GenUnshareNotebookResponse {
				return UnshareNotebook404JSONResponse{resp}
			},
			Conflict: func(resp ConflictJSONResponse) GenUnshareNotebookResponse {
				return UnshareNotebook409JSONResponse{resp}
			},
		}); ok {
			return resp, nil
		}
		return nil, err
	}
	return GenUnshareNotebook204Response{}, nil
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
		if resp, ok := respondDomainErrorForOperation[GenCreateCellResponse]("createCell", err, domainErrorResponder[GenCreateCellResponse]{
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
		if resp, ok := respondDomainErrorForOperation[GenUpdateCellResponse]("updateCell", err, domainErrorResponder[GenUpdateCellResponse]{
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
		if resp, ok := respondDomainErrorForOperation[GenDeleteCellResponse]("deleteCell", err, domainErrorResponder[GenDeleteCellResponse]{
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
		if resp, ok := respondDomainErrorForOperation[GenReorderCellsResponse]("reorderCells", err, domainErrorResponder[GenReorderCellsResponse]{
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
		if resp, ok := respondDomainErrorForOperation[GenCreateNotebookSessionResponse]("createNotebookSession", err, domainErrorResponder[GenCreateNotebookSessionResponse]{
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
		if resp, ok := respondDomainErrorForOperation[GenCloseNotebookSessionResponse]("closeNotebookSession", err, domainErrorResponder[GenCloseNotebookSessionResponse]{
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
		if resp, ok := respondDomainErrorForOperation[GenExecuteCellResponse]("executeCell", err, domainErrorResponder[GenExecuteCellResponse]{
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
		if resp, ok := respondDomainErrorForOperation[GenRunAllCellsResponse]("runAllCells", err, domainErrorResponder[GenRunAllCellsResponse]{
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
		if resp, ok := respondDomainErrorForOperation[GenRunAllCellsAsyncResponse]("runAllCellsAsync", err, domainErrorResponder[GenRunAllCellsAsyncResponse]{
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
		if resp, ok := respondDomainErrorForOperation[GenListNotebookJobsResponse]("listNotebookJobs", err, domainErrorResponder[GenListNotebookJobsResponse]{
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
		if resp, ok := respondDomainErrorForOperation[GenGetNotebookJobResponse]("getNotebookJob", err, domainErrorResponder[GenGetNotebookJobResponse]{
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

// ListExploreItems implements the endpoint for browsing authored assets in folder/project context.
func (h *APIHandler) ListExploreItems(ctx context.Context, req GenListExploreItemsRequest) (GenListExploreItemsResponse, error) {
	if h.explore == nil {
		return nil, domain.ErrNotImplemented("explore service is not configured")
	}
	cp, _ := domain.PrincipalFromContext(ctx)
	page := pageFromParams(req.Params.MaxResults, req.Params.PageToken)
	items, err := h.explore.List(ctx, cp.Name, cp.IsAdmin, domain.ExploreFilter{
		FolderID: valOrEmpty(req.Params.FolderId),
		Kinds:    exploreKindsFromParam(req.Params.Kind),
		Page:     domain.PageRequest{MaxResults: domain.MaxMaxResults},
	})
	if err != nil {
		if resp, ok := respondDomainErrorForOperation[GenListExploreItemsResponse]("listExploreItems", err, domainErrorResponder[GenListExploreItemsResponse]{
			Forbidden: func(resp ForbiddenJSONResponse) GenListExploreItemsResponse {
				return ListExploreItems403JSONResponse{resp}
			},
			NotFound: func(resp NotFoundJSONResponse) GenListExploreItemsResponse {
				return ListExploreItems404JSONResponse{resp}
			},
		}); ok {
			return resp, nil
		}
		return nil, err
	}
	total := int64(len(items))
	start := page.Offset()
	if start > len(items) {
		start = len(items)
	}
	end := start + page.Limit()
	if end > len(items) {
		end = len(items)
	}
	data := make([]ExploreItem, 0, end-start)
	for _, item := range items[start:end] {
		data = append(data, exploreItemToAPI(item))
	}
	nextToken := domain.NextPageToken(page.Offset(), page.Limit(), total)
	return GenListExploreItems200JSONResponse{
		Body:    PaginatedExploreItems{Data: data, NextPageToken: optStr(nextToken)},
		Headers: GenListExploreItems200ResponseHeaders{XRateLimitLimit: defaultRateLimitLimit, XRateLimitRemaining: defaultRateLimitRemaining, XRateLimitReset: defaultRateLimitReset},
	}, nil
}

// === Git Repos ===

// ListGitRepos implements the endpoint for listing Git repositories.
func (h *APIHandler) ListGitRepos(ctx context.Context, req GenListGitReposRequest) (GenListGitReposResponse, error) {
	page := pageFromParams(req.Params.MaxResults, req.Params.PageToken)
	cp, _ := domain.PrincipalFromContext(ctx)
	repos, total, err := h.gitRepos.ListGitReposForPrincipal(ctx, cp.Name, cp.IsAdmin, page)
	if err != nil {
		if resp, ok := respondDomainErrorForOperation[GenListGitReposResponse]("listGitRepos", err, domainErrorResponder[GenListGitReposResponse]{
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
		if resp, ok := respondDomainErrorForOperation[GenCreateGitRepoResponse]("createGitRepo", err, domainErrorResponder[GenCreateGitRepoResponse]{
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
		if resp, ok := respondDomainErrorForOperation[GenGetGitRepoResponse]("getGitRepo", err, domainErrorResponder[GenGetGitRepoResponse]{
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
		if resp, ok := respondDomainErrorForOperation[GenDeleteGitRepoResponse]("deleteGitRepo", err, domainErrorResponder[GenDeleteGitRepoResponse]{
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
		if resp, ok := respondDomainErrorForOperation[GenSyncGitRepoResponse]("syncGitRepo", err, domainErrorResponder[GenSyncGitRepoResponse]{
			BadRequest: func(resp BadRequestJSONResponse) GenSyncGitRepoResponse { return SyncGitRepo400JSONResponse{resp} },
			Forbidden:  func(resp ForbiddenJSONResponse) GenSyncGitRepoResponse { return SyncGitRepo403JSONResponse{resp} },
			NotFound:   func(resp NotFoundJSONResponse) GenSyncGitRepoResponse { return SyncGitRepo404JSONResponse{resp} },
			Internal: func(resp InternalErrorJSONResponse) GenSyncGitRepoResponse {
				if httpStatusFromDomainError(err) == 501 {
					return syncGitRepo501JSONResponse{
						Body:    resp.Body,
						Headers: GenInternalErrorResponseHeaders(resp.Headers),
					}
				}
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
		Id:                    &nb.ID,
		FolderId:              optStr(nb.FolderID),
		Name:                  &nb.Name,
		Description:           nb.Description,
		Owner:                 &nb.Owner,
		GitRepoId:             nb.GitRepoID,
		GitPath:               nb.GitPath,
		ProjectOverrideId:     nb.ProjectOverrideID,
		EnvironmentOverrideId: nb.EnvironmentOverrideID,
		CreatedAt:             formatTimePtr(&nb.CreatedAt),
		UpdatedAt:             formatTimePtr(&nb.UpdatedAt),
	}
}

func folderToAPI(folder domain.Folder) Folder {
	depth := int32(folder.Depth)
	return Folder{
		Id:                   &folder.ID,
		Name:                 &folder.Name,
		Owner:                &folder.Owner,
		ParentFolderId:       folder.ParentFolderID,
		Path:                 &folder.Path,
		Depth:                &depth,
		SystemRole:           folder.SystemRole,
		GitRepoId:            folder.GitRepoID,
		GitRootPath:          folder.GitRootPath,
		DefaultProjectId:     folder.DefaultProjectID,
		DefaultEnvironmentId: folder.DefaultEnvironmentID,
		CreatedAt:            formatTimePtr(&folder.CreatedAt),
		UpdatedAt:            formatTimePtr(&folder.UpdatedAt),
	}
}

func notebookContextToAPI(ctx *domain.NotebookContext) *NotebookContext {
	if ctx == nil {
		return nil
	}
	return &NotebookContext{
		NotebookId:             optStr(ctx.NotebookID),
		FolderId:               optStr(ctx.FolderID),
		EffectiveProjectId:     ctx.EffectiveProjectID,
		EffectiveEnvironmentId: ctx.EffectiveEnvironmentID,
		EffectiveGitRepoId:     ctx.EffectiveGitRepoID,
		EffectiveGitRootPath:   ctx.EffectiveGitRootPath,
		ProjectSourceFolderId:  ctx.ProjectSourceFolderID,
		EnvironmentSourceId:    ctx.EnvironmentSourceID,
		GitSourceFolderId:      ctx.GitSourceFolderID,
	}
}

func notebookShareToAPI(share domain.NotebookShare) NotebookShare {
	return NotebookShare{
		PrincipalName: &share.PrincipalName,
		Role:          notebookShareRoleToAPI(share.Role),
	}
}

func folderShareToAPI(share domain.FolderShare) FolderShare {
	return FolderShare{
		PrincipalName: &share.PrincipalName,
		Role:          notebookShareRoleToAPI(share.Role),
	}
}

func exploreItemToAPI(item domain.ExploreItem) ExploreItem {
	return ExploreItem{
		Kind:         optStr(item.Kind),
		Scope:        optStr(item.Scope),
		Id:           optStr(item.ID),
		Name:         optStr(item.Name),
		Owner:        optStr(item.Owner),
		FolderId:     item.FolderID,
		ProjectName:  item.ProjectName,
		UpdatedAt:    formatTimePtr(&item.UpdatedAt),
		GitRepoId:    item.GitRepoID,
		Shared:       optBool(item.Shared),
		ProjectBound: optBool(item.ProjectBound),
	}
}

func optBool(value bool) *bool {
	return &value
}

func notebookShareRoleToAPI(role string) *NotebookShareRole {
	switch role {
	case domain.FolderShareRoleViewer:
		value := NotebookShareRoleViewer
		return &value
	case domain.FolderShareRoleEditor:
		value := NotebookShareRoleEditor
		return &value
	case domain.FolderShareRoleManager:
		value := NotebookShareRoleManager
		return &value
	default:
		return nil
	}
}

func notebookShareRoleFromAPI(role *NotebookShareRole) string {
	if role == nil {
		return ""
	}
	return string(*role)
}

type syncGitRepo501JSONResponse struct {
	Body    Error
	Headers GenInternalErrorResponseHeaders
}

func (response syncGitRepo501JSONResponse) VisitSyncGitRepoResponse(w http.ResponseWriter) error {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusNotImplemented)
	return json.NewEncoder(w).Encode(response.Body)
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

func exploreKindsFromParam(kind *string) []string {
	if kind == nil {
		return nil
	}
	value := strings.TrimSpace(*kind)
	if value == "" || value == domain.ExploreKindAll {
		return nil
	}
	return []string{value}
}
