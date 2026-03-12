package cli

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	clipkg "duck-demo/pkg/cli/discovery"
)

func TestDocsCoverage_CommandGroupsHaveDocs(t *testing.T) {
	rootCmd := newRootCmd()
	corpus := loadDiscoveryCorpus(rootCmd)
	docs := corpus.ListDocs(clipkg.SearchOptions{})

	coveredGroups := map[string]struct{}{}
	coversAll := false
	for _, doc := range docs {
		for _, group := range doc.CommandGroups {
			if group == "*" {
				coversAll = true
				break
			}
			coveredGroups[group] = struct{}{}
		}
		if coversAll {
			break
		}
	}

	require.True(t, coversAll || len(coveredGroups) > 0)
	for _, entry := range walkCommands(rootCmd, "") {
		if coversAll {
			break
		}
		_, ok := coveredGroups[entry.Group]
		assert.Truef(t, ok, "missing docs coverage for command group %q", entry.Group)
	}
}

func TestDocsRetrievalEval_TopResultsContainExpectedGuide(t *testing.T) {
	rootCmd := newRootCmd()
	corpus := loadDiscoveryCorpus(rootCmd)

	cases := []struct {
		name       string
		query      string
		expectedID string
	}{
		{name: "auth", query: "authenticate with api key", expectedID: "how-to/authentication"},
		{name: "query", query: "run first secure query", expectedID: "start-here/quickstart"},
		{name: "governance", query: "grant access with row filters", expectedID: "how-to/access-control"},
		{name: "declarative", query: "plan and apply declarative config", expectedID: "how-to/declarative-workflows"},
		{name: "models", query: "create model and pipeline run", expectedID: "how-to/build-and-run-model-pipelines"},
		{name: "notebooks", query: "publish dashboard from notebook analysis", expectedID: "how-to/author-notebooks-and-publish-dashboards"},
		{name: "semantic", query: "semantic metrics and metric queries", expectedID: "how-to/define-semantic-metrics"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			results := corpus.Search(tc.query, clipkg.SearchOptions{Kind: "doc", Limit: 5})
			require.NotEmpty(t, results)

			found := false
			for _, result := range results {
				if result.ID == tc.expectedID {
					found = true
					break
				}
			}
			assert.Truef(t, found, "expected %q in top results for query %q", tc.expectedID, tc.query)
		})
	}
}
