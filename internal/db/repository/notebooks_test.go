package repository

import (
	"context"
	"testing"

	internaldb "duck-demo/internal/db"
	"duck-demo/internal/domain"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setupNotebookRepo(t *testing.T) *NotebookRepo {
	t.Helper()
	writeDB, _ := internaldb.OpenTestSQLite(t)
	return NewNotebookRepo(writeDB)
}

func notebookPtrStr(s string) *string { return &s }

func notebookPtrInt(i int) *int { return &i }

// === Notebook CRUD ===

func TestNotebookRepo_CreateAndGet(t *testing.T) {
	repo := setupNotebookRepo(t)
	ctx := context.Background()

	desc := "Test description"
	nb := &domain.Notebook{
		Name:        "My Notebook",
		Description: &desc,
		Owner:       "alice",
	}

	created, err := repo.CreateNotebook(ctx, nb)
	require.NoError(t, err)
	require.NotNil(t, created)
	assert.NotEmpty(t, created.ID)
	assert.Equal(t, "My Notebook", created.Name)
	assert.Equal(t, "Test description", *created.Description)
	assert.Equal(t, "alice", created.Owner)
	assert.False(t, created.CreatedAt.IsZero())

	got, err := repo.GetNotebook(ctx, created.ID)
	require.NoError(t, err)
	assert.Equal(t, created.ID, got.ID)
	assert.Equal(t, created.Name, got.Name)
}

func TestNotebookRepo_GetNotebook_NotFound(t *testing.T) {
	repo := setupNotebookRepo(t)
	ctx := context.Background()

	_, err := repo.GetNotebook(ctx, "nonexistent")
	require.Error(t, err)
	var notFound *domain.NotFoundError
	assert.ErrorAs(t, err, &notFound)
}

func TestNotebookRepo_ListNotebooks(t *testing.T) {
	repo := setupNotebookRepo(t)
	ctx := context.Background()

	// Create two notebooks with different owners
	_, err := repo.CreateNotebook(ctx, &domain.Notebook{Name: "NB1", Owner: "alice"})
	require.NoError(t, err)
	_, err = repo.CreateNotebook(ctx, &domain.Notebook{Name: "NB2", Owner: "bob"})
	require.NoError(t, err)

	t.Run("list all", func(t *testing.T) {
		nbs, total, err := repo.ListNotebooks(ctx, nil, domain.PageRequest{})
		require.NoError(t, err)
		assert.GreaterOrEqual(t, total, int64(2))
		assert.GreaterOrEqual(t, len(nbs), 2)
	})

	t.Run("filter by owner", func(t *testing.T) {
		owner := "alice"
		nbs, total, err := repo.ListNotebooks(ctx, &owner, domain.PageRequest{})
		require.NoError(t, err)
		assert.Equal(t, int64(1), total)
		assert.Len(t, nbs, 1)
		assert.Equal(t, "NB1", nbs[0].Name)
	})
}

func TestNotebookRepo_UpdateNotebook(t *testing.T) {
	repo := setupNotebookRepo(t)
	ctx := context.Background()

	nb, err := repo.CreateNotebook(ctx, &domain.Notebook{Name: "Original", Owner: "alice"})
	require.NoError(t, err)

	t.Run("update name", func(t *testing.T) {
		updated, err := repo.UpdateNotebook(ctx, nb.ID, domain.UpdateNotebookRequest{
			Name: notebookPtrStr("Updated"),
		})
		require.NoError(t, err)
		assert.Equal(t, "Updated", updated.Name)
	})

	t.Run("update description", func(t *testing.T) {
		updated, err := repo.UpdateNotebook(ctx, nb.ID, domain.UpdateNotebookRequest{
			Description: notebookPtrStr("new desc"),
		})
		require.NoError(t, err)
		assert.Equal(t, "new desc", *updated.Description)
		assert.Equal(t, "Updated", updated.Name) // name unchanged
	})

	t.Run("not found", func(t *testing.T) {
		_, err := repo.UpdateNotebook(ctx, "nonexistent", domain.UpdateNotebookRequest{
			Name: notebookPtrStr("x"),
		})
		require.Error(t, err)
		var notFound *domain.NotFoundError
		assert.ErrorAs(t, err, &notFound)
	})
}

func TestNotebookRepo_DeleteNotebook(t *testing.T) {
	repo := setupNotebookRepo(t)
	ctx := context.Background()

	nb, err := repo.CreateNotebook(ctx, &domain.Notebook{Name: "ToDelete", Owner: "alice"})
	require.NoError(t, err)

	err = repo.DeleteNotebook(ctx, nb.ID)
	require.NoError(t, err)

	_, err = repo.GetNotebook(ctx, nb.ID)
	require.Error(t, err)
	var notFound *domain.NotFoundError
	assert.ErrorAs(t, err, &notFound)
}

// === Cell CRUD ===

func TestNotebookRepo_CellCRUD(t *testing.T) {
	repo := setupNotebookRepo(t)
	ctx := context.Background()

	nb, err := repo.CreateNotebook(ctx, &domain.Notebook{Name: "CellNotebook", Owner: "alice"})
	require.NoError(t, err)

	t.Run("create and get cell", func(t *testing.T) {
		cell := &domain.Cell{
			NotebookID: nb.ID,
			CellType:   domain.CellTypeSQL,
			Content:    "SELECT 1",
			Position:   0,
		}
		created, err := repo.CreateCell(ctx, cell)
		require.NoError(t, err)
		assert.NotEmpty(t, created.ID)
		assert.Equal(t, domain.CellTypeSQL, created.CellType)
		assert.Equal(t, "SELECT 1", created.Content)

		got, err := repo.GetCell(ctx, created.ID)
		require.NoError(t, err)
		assert.Equal(t, created.ID, got.ID)
		assert.Equal(t, nb.ID, got.NotebookID)
	})

	t.Run("auto-position when zero", func(t *testing.T) {
		cell1, err := repo.CreateCell(ctx, &domain.Cell{
			NotebookID: nb.ID,
			CellType:   domain.CellTypeSQL,
			Content:    "SELECT 2",
		})
		require.NoError(t, err)

		cell2, err := repo.CreateCell(ctx, &domain.Cell{
			NotebookID: nb.ID,
			CellType:   domain.CellTypeMarkdown,
			Content:    "# Title",
		})
		require.NoError(t, err)

		// Second cell should have position > first
		assert.Greater(t, cell2.Position, cell1.Position)
	})

	t.Run("list cells", func(t *testing.T) {
		cells, err := repo.ListCells(ctx, nb.ID)
		require.NoError(t, err)
		assert.GreaterOrEqual(t, len(cells), 2)
	})

	t.Run("update cell content", func(t *testing.T) {
		cell, err := repo.CreateCell(ctx, &domain.Cell{
			NotebookID: nb.ID,
			CellType:   domain.CellTypeSQL,
			Content:    "SELECT 'old'",
		})
		require.NoError(t, err)

		updated, err := repo.UpdateCell(ctx, cell.ID, domain.UpdateCellRequest{
			Content: notebookPtrStr("SELECT 'new'"),
		})
		require.NoError(t, err)
		assert.Equal(t, "SELECT 'new'", updated.Content)
	})

	t.Run("update cell position", func(t *testing.T) {
		cell, err := repo.CreateCell(ctx, &domain.Cell{
			NotebookID: nb.ID,
			CellType:   domain.CellTypeSQL,
			Content:    "SELECT 42",
		})
		require.NoError(t, err)

		updated, err := repo.UpdateCell(ctx, cell.ID, domain.UpdateCellRequest{
			Position: notebookPtrInt(99),
		})
		require.NoError(t, err)
		assert.Equal(t, 99, updated.Position)
	})

	t.Run("delete cell", func(t *testing.T) {
		cell, err := repo.CreateCell(ctx, &domain.Cell{
			NotebookID: nb.ID,
			CellType:   domain.CellTypeSQL,
			Content:    "SELECT 'delete me'",
		})
		require.NoError(t, err)

		err = repo.DeleteCell(ctx, cell.ID)
		require.NoError(t, err)

		_, err = repo.GetCell(ctx, cell.ID)
		require.Error(t, err)
		var notFound *domain.NotFoundError
		assert.ErrorAs(t, err, &notFound)
	})

	t.Run("get cell not found", func(t *testing.T) {
		_, err := repo.GetCell(ctx, "nonexistent")
		require.Error(t, err)
		var notFound *domain.NotFoundError
		assert.ErrorAs(t, err, &notFound)
	})
}

func TestNotebookRepo_UpdateCellResult(t *testing.T) {
	repo := setupNotebookRepo(t)
	ctx := context.Background()

	nb, err := repo.CreateNotebook(ctx, &domain.Notebook{Name: "ResultNB", Owner: "alice"})
	require.NoError(t, err)

	cell, err := repo.CreateCell(ctx, &domain.Cell{
		NotebookID: nb.ID,
		CellType:   domain.CellTypeSQL,
		Content:    "SELECT 1",
	})
	require.NoError(t, err)

	result := `{"columns":["1"],"rows":[[1]]}`
	err = repo.UpdateCellResult(ctx, cell.ID, &result)
	require.NoError(t, err)

	got, err := repo.GetCell(ctx, cell.ID)
	require.NoError(t, err)
	require.NotNil(t, got.LastResult)
	assert.Equal(t, result, *got.LastResult)
}

func TestNotebookRepo_ReorderCells(t *testing.T) {
	repo := setupNotebookRepo(t)
	ctx := context.Background()

	nb, err := repo.CreateNotebook(ctx, &domain.Notebook{Name: "ReorderNB", Owner: "alice"})
	require.NoError(t, err)

	cell1, err := repo.CreateCell(ctx, &domain.Cell{
		NotebookID: nb.ID, CellType: domain.CellTypeSQL, Content: "SELECT 1", Position: 0,
	})
	require.NoError(t, err)

	cell2, err := repo.CreateCell(ctx, &domain.Cell{
		NotebookID: nb.ID, CellType: domain.CellTypeSQL, Content: "SELECT 2", Position: 1,
	})
	require.NoError(t, err)

	t.Run("reverse order", func(t *testing.T) {
		err := repo.ReorderCells(ctx, nb.ID, []string{cell2.ID, cell1.ID})
		require.NoError(t, err)

		cells, err := repo.ListCells(ctx, nb.ID)
		require.NoError(t, err)
		require.Len(t, cells, 2)
		assert.Equal(t, cell2.ID, cells[0].ID)
		assert.Equal(t, cell1.ID, cells[1].ID)
	})

	t.Run("invalid cell ID", func(t *testing.T) {
		err := repo.ReorderCells(ctx, nb.ID, []string{"bogus-id"})
		require.Error(t, err)
		var validationErr *domain.ValidationError
		assert.ErrorAs(t, err, &validationErr)
	})
}

func TestNotebookRepo_GetMaxPosition(t *testing.T) {
	repo := setupNotebookRepo(t)
	ctx := context.Background()

	nb, err := repo.CreateNotebook(ctx, &domain.Notebook{Name: "MaxPosNB", Owner: "alice"})
	require.NoError(t, err)

	t.Run("empty notebook returns -1", func(t *testing.T) {
		maxPos, err := repo.GetMaxPosition(ctx, nb.ID)
		require.NoError(t, err)
		assert.Equal(t, -1, maxPos)
	})

	t.Run("after adding cells", func(t *testing.T) {
		_, err := repo.CreateCell(ctx, &domain.Cell{
			NotebookID: nb.ID, CellType: domain.CellTypeSQL, Content: "SELECT 1", Position: 5,
		})
		require.NoError(t, err)

		maxPos, err := repo.GetMaxPosition(ctx, nb.ID)
		require.NoError(t, err)
		assert.Equal(t, 5, maxPos)
	})
}
