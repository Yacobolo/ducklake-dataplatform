package notebook

import (
	"context"
	"fmt"

	"duck-demo/internal/domain"
)

// NotebookService provides business logic for notebook and cell operations.
type NotebookService struct {
	repo  domain.NotebookRepository
	audit domain.AuditRepository
}

// NewNotebookService creates a new NotebookService.
func NewNotebookService(repo domain.NotebookRepository, audit domain.AuditRepository) *NotebookService {
	return &NotebookService{repo: repo, audit: audit}
}

// CreateNotebook creates a new notebook owned by the given principal.
func (s *NotebookService) CreateNotebook(ctx context.Context, principal string, req domain.CreateNotebookRequest) (*domain.Notebook, error) {
	if err := req.Validate(); err != nil {
		return nil, err
	}
	nb := &domain.Notebook{
		ID:          domain.NewID(),
		Name:        req.Name,
		Description: req.Description,
		Owner:       principal,
	}
	result, err := s.repo.CreateNotebook(ctx, nb)
	if err != nil {
		return nil, fmt.Errorf("create notebook: %w", err)
	}
	_ = s.audit.Insert(ctx, &domain.AuditEntry{
		PrincipalName: principal,
		Action:        "CREATE_NOTEBOOK",
		Status:        "ALLOWED",
	})
	return result, nil
}

// GetNotebook retrieves a notebook and its cells.
func (s *NotebookService) GetNotebook(ctx context.Context, id string) (*domain.Notebook, []domain.Cell, error) {
	nb, err := s.repo.GetNotebook(ctx, id)
	if err != nil {
		return nil, nil, err
	}
	cells, err := s.repo.ListCells(ctx, id)
	if err != nil {
		return nil, nil, fmt.Errorf("list cells: %w", err)
	}
	return nb, cells, nil
}

// ListNotebooks lists notebooks, optionally filtered by owner.
func (s *NotebookService) ListNotebooks(ctx context.Context, owner *string, page domain.PageRequest) ([]domain.Notebook, int64, error) {
	return s.repo.ListNotebooks(ctx, owner, page)
}

// UpdateNotebook updates notebook metadata. Only the owner or admin can update.
func (s *NotebookService) UpdateNotebook(ctx context.Context, principal string, isAdmin bool, id string, req domain.UpdateNotebookRequest) (*domain.Notebook, error) {
	nb, err := s.repo.GetNotebook(ctx, id)
	if err != nil {
		return nil, err
	}
	if nb.Owner != principal && !isAdmin {
		return nil, domain.ErrAccessDenied("only the notebook owner or admin can update")
	}
	result, err := s.repo.UpdateNotebook(ctx, id, req)
	if err != nil {
		return nil, fmt.Errorf("update notebook: %w", err)
	}
	_ = s.audit.Insert(ctx, &domain.AuditEntry{
		PrincipalName: principal,
		Action:        "UPDATE_NOTEBOOK",
		Status:        "ALLOWED",
	})
	return result, nil
}

// DeleteNotebook deletes a notebook. Only the owner or admin can delete.
func (s *NotebookService) DeleteNotebook(ctx context.Context, principal string, isAdmin bool, id string) error {
	nb, err := s.repo.GetNotebook(ctx, id)
	if err != nil {
		return err
	}
	if nb.Owner != principal && !isAdmin {
		return domain.ErrAccessDenied("only the notebook owner or admin can delete")
	}
	if err := s.repo.DeleteNotebook(ctx, id); err != nil {
		return fmt.Errorf("delete notebook: %w", err)
	}
	_ = s.audit.Insert(ctx, &domain.AuditEntry{
		PrincipalName: principal,
		Action:        "DELETE_NOTEBOOK",
		Status:        "ALLOWED",
	})
	return nil
}

// CreateCell adds a new cell to a notebook. Owner or admin required.
func (s *NotebookService) CreateCell(ctx context.Context, principal string, isAdmin bool, notebookID string, req domain.CreateCellRequest) (*domain.Cell, error) {
	if err := req.Validate(); err != nil {
		return nil, err
	}
	nb, err := s.repo.GetNotebook(ctx, notebookID)
	if err != nil {
		return nil, err
	}
	if nb.Owner != principal && !isAdmin {
		return nil, domain.ErrAccessDenied("only the notebook owner or admin can add cells")
	}

	pos := 0
	if req.Position != nil {
		pos = *req.Position
	} else {
		maxPos, err := s.repo.GetMaxPosition(ctx, notebookID)
		if err != nil {
			return nil, fmt.Errorf("get max position: %w", err)
		}
		pos = maxPos + 1
	}

	cell := &domain.Cell{
		ID:         domain.NewID(),
		NotebookID: notebookID,
		CellType:   req.CellType,
		Content:    req.Content,
		Position:   pos,
	}
	return s.repo.CreateCell(ctx, cell)
}

// UpdateCell updates a cell's content or position. Owner or admin required.
func (s *NotebookService) UpdateCell(ctx context.Context, principal string, isAdmin bool, cellID string, req domain.UpdateCellRequest) (*domain.Cell, error) {
	cell, err := s.repo.GetCell(ctx, cellID)
	if err != nil {
		return nil, err
	}
	nb, err := s.repo.GetNotebook(ctx, cell.NotebookID)
	if err != nil {
		return nil, err
	}
	if nb.Owner != principal && !isAdmin {
		return nil, domain.ErrAccessDenied("only the notebook owner or admin can update cells")
	}
	return s.repo.UpdateCell(ctx, cellID, req)
}

// DeleteCell removes a cell. Owner or admin required.
func (s *NotebookService) DeleteCell(ctx context.Context, principal string, isAdmin bool, cellID string) error {
	cell, err := s.repo.GetCell(ctx, cellID)
	if err != nil {
		return err
	}
	nb, err := s.repo.GetNotebook(ctx, cell.NotebookID)
	if err != nil {
		return err
	}
	if nb.Owner != principal && !isAdmin {
		return domain.ErrAccessDenied("only the notebook owner or admin can delete cells")
	}
	return s.repo.DeleteCell(ctx, cellID)
}

// ReorderCells reorders cells in a notebook. Owner or admin required.
func (s *NotebookService) ReorderCells(ctx context.Context, principal string, isAdmin bool, notebookID string, req domain.ReorderCellsRequest) ([]domain.Cell, error) {
	nb, err := s.repo.GetNotebook(ctx, notebookID)
	if err != nil {
		return nil, err
	}
	if nb.Owner != principal && !isAdmin {
		return nil, domain.ErrAccessDenied("only the notebook owner or admin can reorder cells")
	}
	if err := s.repo.ReorderCells(ctx, notebookID, req.CellIDs); err != nil {
		return nil, err
	}
	return s.repo.ListCells(ctx, notebookID)
}
