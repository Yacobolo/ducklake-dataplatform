package pipeline

import (
	"context"
	"regexp"
	"strings"

	"duck-demo/internal/domain"
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
		if cell.CellType == domain.CellTypeSQL && !isEmptyOrCommentOnlySQL(cell.Content) {
			blocks = append(blocks, cell.Content)
		}
	}

	// 4. Error if no executable SQL cells found.
	if len(blocks) == 0 {
		return nil, domain.ErrValidation("notebook %s has no executable SQL cells", notebookID)
	}

	return blocks, nil
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
