package repository

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	internaldb "github.com/Yacobolo/quackstack/internal/db"
	"github.com/Yacobolo/quackstack/internal/domain"
)

func TestColumnLineageRepo_ReplaceCompilationLineage_AllowsCompilationOnlyRows(t *testing.T) {
	writeDB, _ := internaldb.OpenTestSQLite(t)
	ctx := context.Background()

	workspaceRepo := NewWorkspaceRepo(writeDB)
	projectRepo := NewProjectRepo(writeDB)
	environmentRepo := NewEnvironmentRepo(writeDB)
	compilationRepo := NewCompilationRepo(writeDB)
	lineageRepo := NewColumnLineageRepo(writeDB)

	workspace, err := workspaceRepo.Create(ctx, &domain.Workspace{
		Name:      "Lineage Workspace",
		Kind:      domain.WorkspaceKindShared,
		CreatedBy: "alice",
	})
	require.NoError(t, err)

	project, err := projectRepo.Create(ctx, &domain.Project{
		WorkspaceID:   workspace.ID,
		Name:          "analytics",
		Kind:          domain.ProjectKindShared,
		DefaultBranch: "main",
		CreatedBy:     "alice",
	})
	require.NoError(t, err)

	environment, err := environmentRepo.Create(ctx, &domain.Environment{
		ProjectID:     project.ID,
		ProjectName:   project.Name,
		Name:          "dev",
		Kind:          domain.EnvironmentKindDevelopment,
		TargetCatalog: "e2e",
		TargetSchema:  "app_dev",
		CreatedBy:     "alice",
	})
	require.NoError(t, err)

	compilation, err := compilationRepo.Create(ctx, &domain.Compilation{
		ProjectID:       project.ID,
		EnvironmentID:   environment.ID,
		GitRef:          "refs/heads/main",
		TargetCatalog:   "e2e",
		TargetSchema:    "app_dev",
		CompileManifest: `{"version":2,"models":[]}`,
		CreatedBy:       "alice",
	})
	require.NoError(t, err)

	err = lineageRepo.ReplaceCompilationLineage(ctx, compilation.ID, []domain.CompiledColumnLineage{{
		CompilationID: compilation.ID,
		ProjectName:   project.Name,
		ModelName:     "analytics.fct_orders",
		TargetCatalog: "e2e",
		TargetSchema:  "app_dev",
		TargetTable:   "fct_orders",
		TargetColumn:  "amount",
		TransformType: domain.TransformExpression,
		Partial:       true,
	}})
	require.NoError(t, err)

	items, err := lineageRepo.ListCompilationLineage(ctx, compilation.ID)
	require.NoError(t, err)
	require.Len(t, items, 1)
	assert.Equal(t, compilation.ID, items[0].CompilationID)
	assert.Equal(t, "amount", items[0].TargetColumn)
	assert.Empty(t, items[0].BuildID)
}
