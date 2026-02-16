package architecture_test

import (
	"go/parser"
	"go/token"
	"path/filepath"
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

var rules = []layerRule{
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

var allowedViolations = map[string]map[string]string{
	modulePath + "/internal/service/catalog": {
		modulePath + "/internal/engine": "governance: temporary relaxation; catalog registration currently calls engine.SetDefaultCatalog",
	},
	modulePath + "/internal/service/ingestion": {
		modulePath + "/internal/service/query": "governance: temporary relaxation; ingestion currently uses query presigner contracts",
	},
}

func TestImportBoundaries(t *testing.T) {
	t.Helper()

	files, err := filepath.Glob("internal/**/*.go")
	require.NoError(t, err)

	violations := make([]string, 0)
	fset := token.NewFileSet()

	for _, file := range files {
		if shouldSkipFile(file) {
			continue
		}

		sourcePkg := packageImportPath(file)
		rule, ok := findRule(sourcePkg)
		if !ok {
			continue
		}

		parsed, parseErr := parser.ParseFile(fset, file, nil, parser.ImportsOnly)
		require.NoErrorf(t, parseErr, "parse imports for %s", file)

		for _, imp := range parsed.Imports {
			importPath := strings.Trim(imp.Path.Value, "\"")
			if !strings.HasPrefix(importPath, modulePath+"/") {
				continue
			}
			if isAllowedViolation(sourcePkg, importPath) {
				continue
			}
			if violatesRule(importPath, rule.forbidden) {
				violations = append(violations,
					"governance: "+sourcePkg+" imports "+importPath+" via "+file+"; allowed direction: "+rule.hint,
				)
			}
		}
	}

	if len(violations) > 0 {
		sort.Strings(violations)
		t.Fatalf("%s", strings.Join(violations, "\n"))
	}
}

func shouldSkipFile(path string) bool {
	base := filepath.Base(path)
	if strings.HasSuffix(base, "_test.go") {
		return true
	}
	if strings.HasSuffix(base, ".gen.go") || strings.HasSuffix(base, "_gen.go") || strings.HasSuffix(base, ".sql.go") {
		return true
	}
	return false
}

func packageImportPath(file string) string {
	dir := filepath.Dir(filepath.ToSlash(file))
	return modulePath + "/" + dir
}

func findRule(sourcePkg string) (layerRule, bool) {
	for _, rule := range rules {
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

func isAllowedViolation(sourcePkg string, importPath string) bool {
	allowedBySource, ok := allowedViolations[sourcePkg]
	if !ok {
		return false
	}
	_, ok = allowedBySource[importPath]
	return ok
}

func hasPathPrefix(value string, prefix string) bool {
	return value == prefix || strings.HasPrefix(value, prefix+"/")
}
