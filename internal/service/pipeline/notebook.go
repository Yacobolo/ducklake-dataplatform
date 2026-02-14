package pipeline

import (
	"context"

	"duck-demo/internal/domain"
)

// Compile-time check.
var _ domain.NotebookProvider = (*DBNotebookProvider)(nil)

// DBNotebookProvider resolves notebook IDs to SQL blocks using the
// NotebookRepository. Reads all cells for a notebook, filters to
// CellTypeSQL, returns their Content in position order.
type DBNotebookProvider struct {
	repo domain.NotebookRepository
}

// NewDBNotebookProvider creates a new DBNotebookProvider.
func NewDBNotebookProvider(repo domain.NotebookRepository) *DBNotebookProvider {
	return &DBNotebookProvider{repo: repo}
}

// GetSQLBlocks returns the SQL content of all SQL cells in a notebook, ordered by position.
func (p *DBNotebookProvider) GetSQLBlocks(ctx context.Context, notebookID string) ([]string, error) {
	// 1. Verify notebook exists.
	_, err := p.repo.GetNotebook(ctx, notebookID)
	if err != nil {
		return nil, err // preserves NotFoundError
	}

	// 2. List cells (returns ordered by position).
	cells, err := p.repo.ListCells(ctx, notebookID)
	if err != nil {
		return nil, err
	}

	// 3. Filter to SQL cells.
	var blocks []string
	for _, cell := range cells {
		if cell.CellType == domain.CellTypeSQL {
			blocks = append(blocks, cell.Content)
		}
	}

	// 4. Error if no SQL cells found.
	if len(blocks) == 0 {
		return nil, domain.ErrValidation("notebook %s has no SQL cells", notebookID)
	}

	return blocks, nil
}
