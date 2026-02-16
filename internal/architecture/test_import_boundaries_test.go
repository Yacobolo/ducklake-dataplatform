package architecture_test

import (
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTestImportBoundaries(t *testing.T) {
	t.Helper()

	files, err := collectGoFiles(internalRootDir())
	require.NoError(t, err)

	violations := make([]string, 0)
	for _, file := range files {
		if !isTestFile(file) || shouldSkipGeneratedFile(file) {
			continue
		}

		sourcePkg := packageImportPath(file)
		rule, ok := findRule(sourcePkg)
		if !ok {
			continue
		}

		relPath := relToRepoRoot(file)
		isIntegrationTest := hasIntegrationBuildTag(file)
		for _, importPath := range parseImports(t, file) {
			if !strings.HasPrefix(importPath, modulePath+"/") {
				continue
			}
			matchedForbidden := matchingForbiddenPrefix(importPath, rule.forbidden)
			if matchedForbidden == "" {
				continue
			}
			if isIntegrationTest {
				continue
			}

			violations = append(violations,
				"governance: test "+sourcePkg+" imports "+importPath+" via "+relPath+"; allowed direction: "+rule.hint,
			)
		}
	}

	if len(violations) > 0 {
		sort.Strings(violations)
		t.Fatalf("%s", strings.Join(violations, "\n"))
	}
}
