package architecture_test

import (
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

const modulePath = "duck-demo"

type layerRule struct {
	sourcePrefix string
	forbidden    []string
	hint         string
}

var architectureRules = []layerRule{
	{
		sourcePrefix: modulePath + "/internal/domain",
		forbidden: []string{
			modulePath + "/internal/api",
			modulePath + "/internal/service",
			modulePath + "/internal/db",
			modulePath + "/internal/engine",
			modulePath + "/internal/middleware",
			modulePath + "/internal/declarative",
			modulePath + "/cmd",
			modulePath + "/pkg/cli",
		},
		hint: "domain may only import domain",
	},
	{
		sourcePrefix: modulePath + "/internal/service",
		forbidden: []string{
			modulePath + "/internal/api",
			modulePath + "/internal/db",
			modulePath + "/internal/engine",
			modulePath + "/internal/middleware",
			modulePath + "/cmd",
			modulePath + "/pkg/cli",
		},
		hint: "service should depend on domain and service-local packages",
	},
	{
		sourcePrefix: modulePath + "/internal/api",
		forbidden: []string{
			modulePath + "/internal/db",
			modulePath + "/internal/engine",
			modulePath + "/internal/declarative",
			modulePath + "/cmd",
			modulePath + "/pkg/cli",
		},
		hint: "api should depend on service/domain/api packages",
	},
	{
		sourcePrefix: modulePath + "/internal/db",
		forbidden: []string{
			modulePath + "/internal/api",
			modulePath + "/internal/service",
			modulePath + "/internal/engine",
			modulePath + "/internal/middleware",
			modulePath + "/cmd",
			modulePath + "/pkg/cli",
		},
		hint: "db should depend on domain and db-local packages",
	},
	{
		sourcePrefix: modulePath + "/internal/engine",
		forbidden: []string{
			modulePath + "/internal/api",
			modulePath + "/internal/service",
			modulePath + "/cmd",
			modulePath + "/pkg/cli",
		},
		hint: "engine should depend on domain and engine-local packages",
	},
	{
		sourcePrefix: modulePath + "/internal/middleware",
		forbidden: []string{
			modulePath + "/internal/service",
			modulePath + "/internal/db",
			modulePath + "/internal/engine",
		},
		hint: "middleware should depend on domain and middleware-local packages",
	},
}

func collectGoFiles(root string) ([]string, error) {
	files := make([]string, 0)
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		if strings.HasSuffix(path, ".go") {
			files = append(files, filepath.ToSlash(path))
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return files, nil
}

func repoRootDir() string {
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		return "."
	}
	return filepath.Clean(filepath.Join(filepath.Dir(file), "..", ".."))
}

func internalRootDir() string {
	return filepath.Join(repoRootDir(), "internal")
}

func findRule(sourcePkg string) (layerRule, bool) {
	for _, rule := range architectureRules {
		if hasPathPrefix(sourcePkg, rule.sourcePrefix) {
			return rule, true
		}
	}
	return layerRule{}, false
}

func violatesRule(importPath string, forbidden []string) bool {
	for _, prefix := range forbidden {
		if hasPathPrefix(importPath, prefix) {
			return true
		}
	}
	return false
}

func matchingForbiddenPrefix(importPath string, forbidden []string) string {
	for _, prefix := range forbidden {
		if hasPathPrefix(importPath, prefix) {
			return prefix
		}
	}
	return ""
}

func hasPathPrefix(value string, prefix string) bool {
	return value == prefix || strings.HasPrefix(value, prefix+"/")
}

func packageImportPath(file string) string {
	path := filepath.ToSlash(file)
	idx := strings.Index(path, "/internal/")
	if idx >= 0 {
		return modulePath + path[idx:]
	}
	dir := filepath.Dir(path)
	return modulePath + "/" + dir
}

func shouldSkipGeneratedFile(path string) bool {
	base := filepath.Base(path)
	if strings.HasSuffix(base, ".gen.go") || strings.HasSuffix(base, "_gen.go") || strings.HasSuffix(base, ".sql.go") {
		return true
	}
	return false
}

func isTestFile(path string) bool {
	base := filepath.Base(path)
	return strings.HasSuffix(base, "_test.go")
}

func shouldSkipProductionGovernanceFile(path string) bool {
	if isTestFile(path) {
		return true
	}
	return shouldSkipGeneratedFile(path)
}

func parseImports(t *testing.T, file string) []string {
	t.Helper()

	fset := token.NewFileSet()
	parsed, err := parser.ParseFile(fset, file, nil, parser.ImportsOnly)
	require.NoErrorf(t, err, "parse imports for %s", file)

	imports := make([]string, 0, len(parsed.Imports))
	for _, imp := range parsed.Imports {
		imports = append(imports, strings.Trim(imp.Path.Value, "\""))
	}
	return imports
}

func relToRepoRoot(path string) string {
	rel, err := filepath.Rel(repoRootDir(), path)
	if err != nil {
		return filepath.ToSlash(path)
	}
	return filepath.ToSlash(rel)
}

func architectureRuleMap() map[string][]string {
	m := make(map[string][]string, len(architectureRules))
	for _, r := range architectureRules {
		forbidden := append([]string(nil), r.forbidden...)
		sort.Strings(forbidden)
		m[r.sourcePrefix] = forbidden
	}
	return m
}

func hasIntegrationBuildTag(filePath string) bool {
	content, err := os.ReadFile(filePath)
	if err != nil {
		return false
	}
	return strings.Contains(string(content), "//go:build integration")
}
