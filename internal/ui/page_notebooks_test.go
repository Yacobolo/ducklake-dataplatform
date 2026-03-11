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
		RunAllURL:      "/ui/notebooks/nb-1/run-all",
		RunAllAsyncURL: "/ui/notebooks/nb-1/run-all-async",
		JobsURL:        "/ui/notebooks/nb-1/jobs",
		GitRepoURL:     "/ui/notebooks/git-repos",
		NewCellURL:     "/ui/notebooks/nb-1/cells/new",
		EditURL:        "/ui/notebooks/nb-1/edit",
		DeleteURL:      "/ui/notebooks/nb-1/delete",
		PromoteURL:     "/ui/models/promote",
		ReorderURL:     "/ui/notebooks/nb-1/cells/reorder",
		CSRFFieldFunc:  func() gomponents.Node { return nil },
	})

	var buf bytes.Buffer
	require.NoError(t, page.Render(&buf))
	html := buf.String()

	assert.Contains(t, html, "data-notebook-browser-runtime")
	assert.Contains(t, html, "id=\"notebook-compute-mode\"")
	assert.Contains(t, html, "id=\"notebook-reset-local-runtime\"")
	assert.Contains(t, html, "id=\"notebook-run-all\"")
}

func TestNotebookResultNode_RendersVisualModes(t *testing.T) {
	t.Run("metric keeps data fallback", func(t *testing.T) {
		node := notebookResultNode(notebookCellRowData{
			CellType:    string(domain.CellTypeSQL),
			DownloadURL: "/ui/notebooks/nb-1/cells/cell-1/download.csv",
			VisualSpec: &domain.VisualSpec{
				Kind: domain.VisualOutputMetric,
				Encodings: domain.VisualEncodings{
					Value: &domain.VisualFieldBinding{Field: "revenue"},
				},
			},
			LastResult: &notebookCellResultData{
				Columns:  []string{"revenue"},
				RawRows:  [][]interface{}{{42}},
				Rows:     [][]string{{"42"}},
				RowCount: 1,
			},
		})
		var buf bytes.Buffer
		require.NoError(t, node.Render(&buf))
		html := buf.String()
		assert.Contains(t, html, "View data")
		assert.Contains(t, html, "Download result CSV")
		assert.Contains(t, html, "42")
	})

	t.Run("chart renders host and data fallback", func(t *testing.T) {
		chartType := domain.VisualChartBar
		node := notebookResultNode(notebookCellRowData{
			CellType:    string(domain.CellTypeSQL),
			DownloadURL: "/ui/notebooks/nb-1/cells/cell-1/download.csv",
			VisualSpec: &domain.VisualSpec{
				Kind:      domain.VisualOutputChart,
				ChartType: &chartType,
				Encodings: domain.VisualEncodings{
					X: &domain.VisualFieldBinding{Field: "region"},
					Y: &domain.VisualFieldBinding{Field: "revenue"},
				},
			},
			LastResult: &notebookCellResultData{
				Columns:  []string{"region", "revenue"},
				RawRows:  [][]interface{}{{"APAC", 42}},
				Rows:     [][]string{{"APAC", "42"}},
				RowCount: 1,
			},
		})
		var buf bytes.Buffer
		require.NoError(t, node.Render(&buf))
		html := buf.String()
		assert.Contains(t, html, "data-chart-payload")
		assert.Contains(t, html, "View data")
		assert.Contains(t, html, "Download result CSV")
	})
}
