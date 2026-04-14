package pipeline

import (
	"context"
	"regexp"
	"strings"

	"github.com/Yacobolo/quackstack/internal/domain"
	svcnotebook "github.com/Yacobolo/quackstack/internal/service/notebook"
)

var blockCommentPattern = regexp.MustCompile(`(?s)/\*.*?\*/`)

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
	execCells, err := p.GetExecutableCells(ctx, notebookID)
	if err != nil {
		return nil, err
	}
	blocks := make([]string, 0, len(execCells))
	for _, cell := range execCells {
		blocks = append(blocks, cell.SQL)
	}
	return blocks, nil
}

// GetExecutableCells returns compiled executable SQL cells for a notebook.
func (p *DBNotebookProvider) GetExecutableCells(ctx context.Context, notebookID string) ([]domain.NotebookExecutableCell, error) {
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

	// 3. Filter to SQL executable cells.
	exec := make([]domain.NotebookExecutableCell, 0)
	for _, cell := range cells {
		if cell.CellType != domain.CellTypeSQL || cell.Disabled {
			continue
		}
		role := cell.Role
		if role == "" {
			role = domain.CellRoleTransform
		}
		if role != domain.CellRoleTransform && role != domain.CellRoleOutput && role != domain.CellRoleTest {
			continue
		}
		if isEmptyOrCommentOnlySQL(cell.Content) {
			continue
		}

		compiled, err := svcnotebook.CompileNotebookCellSQL(cells, cell.ID, false)
		if err != nil {
			return nil, err
		}
		if !isEmptyOrCommentOnlySQL(compiled) {
			exec = append(exec, domain.NotebookExecutableCell{ID: cell.ID, SQL: compiled, Role: role, Test: cell.Test})
		}
	}

	// 4. Error if no executable SQL cells found.
	if len(exec) == 0 {
		return nil, domain.ErrValidation("notebook %s has no executable SQL cells", notebookID)
	}

	return exec, nil
}

// GetSQLBlockByCellID returns SQL content for a specific SQL cell in a notebook.
func (p *DBNotebookProvider) GetSQLBlockByCellID(ctx context.Context, notebookID, cellID string) (string, error) {
	cell, err := p.repo.GetCell(ctx, cellID)
	if err != nil {
		return "", err
	}
	if cell.NotebookID != notebookID {
		return "", domain.ErrValidation("cell %s does not belong to notebook %s", cellID, notebookID)
	}
	if cell.CellType != domain.CellTypeSQL {
		return "", domain.ErrValidation("cell %s is not a SQL cell", cellID)
	}
	if cell.Disabled {
		return "", domain.ErrValidation("cell %s is disabled", cellID)
	}
	cells, err := p.repo.ListCells(ctx, notebookID)
	if err != nil {
		return "", err
	}
	return svcnotebook.CompileNotebookCellSQL(cells, cellID, false)
}

// CompileOutputCellSQL compiles SQL for a notebook output cell with graph-aware extraction.
func (p *DBNotebookProvider) CompileOutputCellSQL(ctx context.Context, notebookID, outputCellID string) (string, error) {
	cells, err := p.repo.ListCells(ctx, notebookID)
	if err != nil {
		return "", err
	}
	return svcnotebook.CompileNotebookCellSQL(cells, outputCellID, true)
}

// ListCells returns all notebook cells ordered by position.
func (p *DBNotebookProvider) ListCells(ctx context.Context, notebookID string) ([]domain.Cell, error) {
	return p.repo.ListCells(ctx, notebookID)
}

func isEmptyOrCommentOnlySQL(sql string) bool {
	sanitized := strings.TrimSpace(sql)
	if sanitized == "" {
		return true
	}

	// Strip block comments first, then remove whole-line comments.
	sanitized = blockCommentPattern.ReplaceAllString(sanitized, "")
	for _, line := range strings.Split(sanitized, "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "--") {
			continue
		}
		return false
	}

	return true
}
