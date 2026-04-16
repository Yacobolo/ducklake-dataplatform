package notebooks

import (
	"bytes"
	"testing"

	"github.com/Yacobolo/quackstack/internal/domain"
	"github.com/Yacobolo/quackstack/internal/service/query"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	gomponents "maragu.dev/gomponents"
)

func TestNotebookDetailPage_UsesApplicationLayout(t *testing.T) {
	page := notebookDetailPage(notebookDetailPageData{
		Principal:       domain.ContextPrincipal{Name: "alice", Type: "user"},
		NotebookID:      "nb-123",
		Name:            "Revenue Workbook",
		Owner:           "alice",
		SelectedCatalog: "live_catalog",
		SelectedSchema:  "analytics",
		BrowserRuntime:  query.DefaultManifestBrowserRuntimeSpec(),
		ComputeRequest:  domain.ComputeExecutionRequest{Mode: domain.ComputeModeAuto, WorkloadType: domain.ComputeWorkloadInteractive},
		NewCellURL:      "/ui/notebooks/nb-123/cells/new",
		RunAllURL:       "/ui/notebooks/nb-123/run-all",
		RunAllAsyncURL:  "/ui/notebooks/nb-123/run-all-async",
		EditURL:         "/ui/notebooks/nb-123/edit",
		MoveURL:         "/ui/notebooks/nb-123/move",
		DuplicateURL:    "/ui/notebooks/nb-123/duplicate",
		DeleteURL:       "/ui/notebooks/nb-123/delete",
		ShareURL:        "/ui/notebooks/nb-123/share",
		PromoteURL:      "/ui/notebooks/nb-123/promote",
		ReorderURL:      "/ui/notebooks/nb-123/cells/reorder",
		JobsURL:         "/ui/notebooks/nb-123/jobs",
		GitRepoURL:      "/ui/explore/git-repos",
		CSRFFieldFunc: func() gomponents.Node {
			return nil
		},
	})

	html := renderNotebookNode(t, page)

	assert.Contains(t, html, "app-layout-shell")
	assert.Contains(t, html, "data-shell-compact-locked=\"true\"")
	assert.Contains(t, html, "app-layout-center")
	assert.Contains(t, html, "app-primary-rail")
	assert.Contains(t, html, "app-secondary-aside")
	assert.Contains(t, html, "notebook-aside")
	assert.Contains(t, html, "workspace-aside-plain")
	assert.Contains(t, html, "app-layout-main-region")
	assert.Contains(t, html, "app-layout-footer")
	assert.Contains(t, html, "NOTEBOOK")
	assert.Contains(t, html, "live_catalog / analytics")
	assert.Contains(t, html, "/ui/static/js/sql-editor.")
	assert.Contains(t, html, "/ui/static/js/notebook.")
	assert.NotContains(t, html, "class=\"workspace-layout")
	assert.NotContains(t, html, "id=\"sidebar-toggle\"")
}

func renderNotebookNode(t *testing.T, node gomponents.Node) string {
	t.Helper()
	var buf bytes.Buffer
	require.NoError(t, node.Render(&buf))
	return buf.String()
}
