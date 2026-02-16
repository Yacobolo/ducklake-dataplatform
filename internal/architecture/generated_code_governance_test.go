package architecture_test

import (
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGeneratedFileSkipPatterns(t *testing.T) {
	t.Helper()

	assert.True(t, shouldSkipGeneratedFile("internal/api/types.gen.go"))
	assert.True(t, shouldSkipGeneratedFile("internal/duckdbsql/catalog/functions_gen.go"))
	assert.True(t, shouldSkipGeneratedFile("internal/db/dbstore/catalog.sql.go"))

	assert.False(t, shouldSkipGeneratedFile("internal/service/security/grant.go"))
	assert.False(t, shouldSkipGeneratedFile("internal/service/security/grant_test.go"))
}

func TestDepguardGeneratedExclusionsArePresentAndScoped(t *testing.T) {
	t.Helper()

	b, err := os.ReadFile(repoRootDir() + "/.golangci.yml")
	require.NoError(t, err)
	content := string(b)

	required := []string{
		"- path: \\.gen\\.go",
		"- path: internal/db/dbstore/.*\\.sql\\.go",
		"- path: internal/duckdbsql/catalog/.*_gen\\.go",
	}
	for _, pattern := range required {
		if strings.Contains(content, pattern) {
			continue
		}
		t.Fatalf("governance: missing depguard generated exclusion pattern %q", pattern)
	}

	forbiddenBroad := []string{
		"- path: internal/.*\\.go",
		"- path: internal/.*_gen\\.go",
	}
	for _, pattern := range forbiddenBroad {
		if strings.Contains(content, pattern) {
			t.Fatalf("governance: generated exclusions are too broad: %q", pattern)
		}
	}
}
