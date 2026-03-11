package discovery

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"duck-demo/pkg/cli/gen"
)

func TestCorpus_Search_PrioritizesCommandThenOperation(t *testing.T) {
	corpus := NewCorpus([]CommandInfo{
		{Path: "catalog schemas create", Short: "Create a schema"},
	}, gen.ReferenceIndex{
		Docs: []gen.ReferenceDoc{
			{ID: "how-to/schemas", Path: "how-to/schemas.md", Section: "how-to", Title: "Create Schemas", Description: "Create and manage schemas safely.", DocKind: "task", ProductAreas: []string{"catalogs"}, Tasks: []string{"create schemas"}},
		},
		Operations: []gen.ReferenceOperation{
			{OperationID: "createSchema", Path: "/catalogs/{catalogName}/schemas", Summary: "Create a schema", CLICommand: "catalog schemas create"},
		},
		Links: []gen.ReferenceLink{
			{SourceKind: "operation", SourceID: "createSchema", TargetKind: "command", TargetID: "catalog schemas create", Reason: "x-cli-command", Confidence: 100},
			{SourceKind: "operation", SourceID: "createSchema", TargetKind: "doc", TargetID: "how-to/schemas", Reason: "keyword-match", Confidence: 80},
			{SourceKind: "doc", SourceID: "how-to/schemas", TargetKind: "operation", TargetID: "createSchema", Reason: "keyword-match", Confidence: 80},
		},
	})

	results := corpus.Search("schema create", SearchOptions{Limit: 10})
	require.NotEmpty(t, results)
	assert.Equal(t, "command", results[0].Kind)
	assert.Equal(t, "catalog schemas create", results[0].Title)

	var sawOperation bool
	for _, result := range results {
		if result.Kind == "operation" && result.ID == "createSchema" {
			sawOperation = true
			break
		}
	}
	assert.True(t, sawOperation)
}

func TestCorpus_ListDocs_AppliesFilters(t *testing.T) {
	corpus := NewCorpus(nil, gen.ReferenceIndex{
		Docs: []gen.ReferenceDoc{
			{ID: "how-to/authentication", Path: "how-to/authentication.md", Section: "how-to", Title: "Access the Platform", Audiences: []string{"ai-agents"}, ProductAreas: []string{"auth"}, Surfaces: []string{"api"}, Tasks: []string{"authenticate"}, DocKind: "task"},
			{ID: "reference/api", Path: "reference/api.md", Section: "reference", Title: "Advanced API Reference", Audiences: []string{"builders"}, ProductAreas: []string{"api"}, Surfaces: []string{"api"}, Tasks: []string{"inspect contracts"}, DocKind: "reference"},
		},
	})

	docs := corpus.ListDocs(SearchOptions{Audience: "ai-agents", ProductArea: "auth", DocKind: "task"})
	require.Len(t, docs, 1)
	assert.Equal(t, "how-to/authentication", docs[0].ID)
}

func TestCorpus_Search_UsesFuzzyFallback(t *testing.T) {
	corpus := NewCorpus([]CommandInfo{
		{Path: "catalog schemas create", Short: "Create a schema"},
	}, gen.ReferenceIndex{})

	results := corpus.Search("creat schema", SearchOptions{Limit: 5})
	require.NotEmpty(t, results)
	assert.Equal(t, "command", results[0].Kind)
	assert.Contains(t, results[0].Title, "create")
}

func TestCorpus_FindDoc_ByIDPathAndTitle(t *testing.T) {
	corpus := NewCorpus(nil, gen.ReferenceIndex{
		Docs: []gen.ReferenceDoc{
			{ID: "how-to/authentication", Path: "how-to/authentication.md", Section: "how-to", Title: "Access the Platform"},
		},
	})

	doc, ok := corpus.FindDoc("how-to/authentication")
	require.True(t, ok)
	assert.Equal(t, "Access the Platform", doc.Title)

	doc, ok = corpus.FindDoc("how-to/authentication.md")
	require.True(t, ok)
	assert.Equal(t, "Access the Platform", doc.Title)

	doc, ok = corpus.FindDoc("Access the Platform")
	require.True(t, ok)
	assert.Equal(t, "how-to/authentication", doc.ID)
}
