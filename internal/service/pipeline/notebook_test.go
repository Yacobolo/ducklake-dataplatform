package pipeline

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"duck-demo/internal/domain"
	"duck-demo/internal/testutil"
)

func TestDBNotebookProvider_GetSQLBlocks(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		name         string
		notebookID   string
		setupRepo    func() *testutil.MockNotebookRepo
		wantBlocks   []string
		wantErr      bool
		wantErrType  interface{}
		wantContains string // substring in error message
	}{
		{
			name:       "happy_path",
			notebookID: "nb-1",
			setupRepo: func() *testutil.MockNotebookRepo {
				return &testutil.MockNotebookRepo{
					GetNotebookFn: func(_ context.Context, id string) (*domain.Notebook, error) {
						return &domain.Notebook{ID: id, Name: "test-notebook"}, nil
					},
					ListCellsFn: func(_ context.Context, _ string) ([]domain.Cell, error) {
						return []domain.Cell{
							{ID: "c1", CellType: domain.CellTypeSQL, Content: "SELECT 1", Position: 0},
							{ID: "c2", CellType: domain.CellTypeMarkdown, Content: "# Notes", Position: 1},
							{ID: "c3", CellType: domain.CellTypeSQL, Content: "SELECT 2", Position: 2},
							{ID: "c4", CellType: domain.CellTypeSQL, Content: "SELECT 3", Position: 3},
						}, nil
					},
				}
			},
			wantBlocks: []string{"SELECT 1", "SELECT 2", "SELECT 3"},
		},
		{
			name:       "notebook_not_found",
			notebookID: "nb-missing",
			setupRepo: func() *testutil.MockNotebookRepo {
				return &testutil.MockNotebookRepo{
					GetNotebookFn: func(_ context.Context, id string) (*domain.Notebook, error) {
						return nil, domain.ErrNotFound("notebook %s not found", id)
					},
				}
			},
			wantErr:     true,
			wantErrType: &domain.NotFoundError{},
		},
		{
			name:       "no_sql_cells",
			notebookID: "nb-md-only",
			setupRepo: func() *testutil.MockNotebookRepo {
				return &testutil.MockNotebookRepo{
					GetNotebookFn: func(_ context.Context, id string) (*domain.Notebook, error) {
						return &domain.Notebook{ID: id, Name: "markdown-only"}, nil
					},
					ListCellsFn: func(_ context.Context, _ string) ([]domain.Cell, error) {
						return []domain.Cell{
							{ID: "c1", CellType: domain.CellTypeMarkdown, Content: "# Title", Position: 0},
							{ID: "c2", CellType: domain.CellTypeMarkdown, Content: "## Section", Position: 1},
						}, nil
					},
				}
			},
			wantErr:      true,
			wantErrType:  &domain.ValidationError{},
			wantContains: "has no SQL cells",
		},
		{
			name:       "mixed_cells_ordered_by_position",
			notebookID: "nb-mixed",
			setupRepo: func() *testutil.MockNotebookRepo {
				return &testutil.MockNotebookRepo{
					GetNotebookFn: func(_ context.Context, id string) (*domain.Notebook, error) {
						return &domain.Notebook{ID: id, Name: "mixed"}, nil
					},
					ListCellsFn: func(_ context.Context, _ string) ([]domain.Cell, error) {
						// Cells returned in position order by the repo.
						return []domain.Cell{
							{ID: "c1", CellType: domain.CellTypeMarkdown, Content: "# Intro", Position: 0},
							{ID: "c2", CellType: domain.CellTypeSQL, Content: "CREATE TABLE t1 (id INT)", Position: 1},
							{ID: "c3", CellType: domain.CellTypeMarkdown, Content: "## Transform", Position: 2},
							{ID: "c4", CellType: domain.CellTypeSQL, Content: "INSERT INTO t1 VALUES (1)", Position: 3},
							{ID: "c5", CellType: domain.CellTypeSQL, Content: "SELECT * FROM t1", Position: 4},
						}, nil
					},
				}
			},
			wantBlocks: []string{
				"CREATE TABLE t1 (id INT)",
				"INSERT INTO t1 VALUES (1)",
				"SELECT * FROM t1",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			repo := tt.setupRepo()
			provider := NewDBNotebookProvider(repo)

			blocks, err := provider.GetSQLBlocks(ctx, tt.notebookID)

			if tt.wantErr {
				require.Error(t, err)
				assert.IsType(t, tt.wantErrType, err)
				if tt.wantContains != "" {
					assert.Contains(t, err.Error(), tt.wantContains)
				}
				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.wantBlocks, blocks)
		})
	}
}
