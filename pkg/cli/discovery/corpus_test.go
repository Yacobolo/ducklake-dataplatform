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
			{ID: "build/register-source-data", Path: "build/register-source-data.md", Section: "build", Title: "Register Source Data", Description: "Register and manage source data safely."},
		},
		Operations: []gen.ReferenceOperation{
			{OperationID: "createSchema", Path: "/catalogs/{catalogName}/schemas", Summary: "Create a schema", CLICommand: "catalog schemas create"},
		},
		Links: []gen.ReferenceLink{
			{SourceKind: "operation", SourceID: "createSchema", TargetKind: "command", TargetID: "catalog schemas create", Reason: "x-cli-command", Confidence: 100},
			{SourceKind: "operation", SourceID: "createSchema", TargetKind: "doc", TargetID: "build/register-source-data", Reason: "keyword-match", Confidence: 80},
			{SourceKind: "doc", SourceID: "build/register-source-data", TargetKind: "operation", TargetID: "createSchema", Reason: "keyword-match", Confidence: 80},
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
			{ID: "govern/authentication-and-identities", Path: "govern/authentication-and-identities.md", Section: "govern", Title: "Authentication And Identities"},
		},
	})

	doc, ok := corpus.FindDoc("govern/authentication-and-identities")
	require.True(t, ok)
	assert.Equal(t, "Authentication And Identities", doc.Title)

	doc, ok = corpus.FindDoc("govern/authentication-and-identities.md")
	require.True(t, ok)
	assert.Equal(t, "Authentication And Identities", doc.Title)

	doc, ok = corpus.FindDoc("Authentication And Identities")
	require.True(t, ok)
	assert.Equal(t, "govern/authentication-and-identities", doc.ID)
}
