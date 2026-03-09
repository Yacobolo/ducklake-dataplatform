package ui

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	gomponents "maragu.dev/gomponents"

	"duck-demo/internal/domain"
	"duck-demo/internal/service/query"
)

func TestNotebookDetailPage_RendersComputeControls(t *testing.T) {
	page := notebookDetailPage(notebookDetailPageData{
		Principal:       domain.ContextPrincipal{Name: "alice", Type: "user"},
		NotebookID:      "nb-1",
		Name:            "Notebook",
		Owner:           "alice",
		SelectedCatalog: "lake",
		SelectedSchema:  "main",
		BrowserRuntime:  query.DefaultManifestBrowserRuntimeSpec(),
		ComputeRequest: domain.ComputeExecutionRequest{
			Mode:         domain.ComputeModeByocLocal,
			WorkloadType: domain.ComputeWorkloadInteractive,
		},
		RunAllURL:     "/ui/notebooks/nb-1/run-all",
		RunAllAsyncURL: "/ui/notebooks/nb-1/run-all-async",
		JobsURL:       "/ui/notebooks/nb-1/jobs",
		GitRepoURL:    "/ui/notebooks/git-repos",
		NewCellURL:    "/ui/notebooks/nb-1/cells/new",
		EditURL:       "/ui/notebooks/nb-1/edit",
		DeleteURL:     "/ui/notebooks/nb-1/delete",
		PromoteURL:    "/ui/models/promote",
		ReorderURL:    "/ui/notebooks/nb-1/cells/reorder",
		CSRFFieldFunc: func() gomponents.Node { return nil },
	})

	var buf bytes.Buffer
	require.NoError(t, page.Render(&buf))
	html := buf.String()

	assert.Contains(t, html, "data-notebook-browser-runtime")
	assert.Contains(t, html, "id=\"notebook-compute-mode\"")
	assert.Contains(t, html, "id=\"notebook-reset-local-runtime\"")
	assert.Contains(t, html, "id=\"notebook-run-all\"")
}
