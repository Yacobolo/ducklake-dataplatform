package architecture_test

import (
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIntegrationTestsRequireBuildTag(t *testing.T) {
	t.Helper()

	files, err := collectGoFiles(filepath.Join(repoRootDir(), "test", "integration"))
	require.NoError(t, err)

	violations := make([]string, 0)
	for _, file := range files {
		content, readErr := os.ReadFile(file)
		require.NoErrorf(t, readErr, "read %s", file)

		if strings.Contains(string(content), "//go:build integration") {
			continue
		}

		violations = append(violations, "governance: missing //go:build integration in "+relToRepoRoot(file))
	}

	if len(violations) > 0 {
		sort.Strings(violations)
		t.Fatalf("%s", strings.Join(violations, "\n"))
	}
}
