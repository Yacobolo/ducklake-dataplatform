package architecture_test

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

type methodExpectation struct {
	file       string
	method     string
	snippets   []string
	anySnippet []string
}

func TestAuthorizationCoverage_CriticalMutatingMethods(t *testing.T) {
	t.Helper()

	expects := []methodExpectation{
		{file: "internal/service/catalog/catalog.go", method: "CreateSchema", anySnippet: []string{"CheckPrivilege(", "requirePrivilege("}},
		{file: "internal/service/catalog/catalog.go", method: "UpdateSchema", anySnippet: []string{"CheckPrivilege(", "requirePrivilege("}},
		{file: "internal/service/catalog/catalog.go", method: "DeleteSchema", anySnippet: []string{"CheckPrivilege(", "requirePrivilege("}},
		{file: "internal/service/catalog/catalog.go", method: "CreateTable", anySnippet: []string{"CheckPrivilege(", "requirePrivilege("}},
		{file: "internal/service/catalog/catalog.go", method: "DeleteTable", anySnippet: []string{"CheckPrivilege(", "requirePrivilege("}},
		{file: "internal/service/catalog/catalog.go", method: "UpdateTable", anySnippet: []string{"CheckPrivilege(", "requirePrivilege("}},
		{file: "internal/service/catalog/catalog.go", method: "UpdateColumn", anySnippet: []string{"CheckPrivilege(", "requirePrivilege("}},
		{file: "internal/service/catalog/views.go", method: "CreateView", anySnippet: []string{"CheckPrivilege(", "requirePrivilege("}},
		{file: "internal/service/catalog/views.go", method: "DeleteView", anySnippet: []string{"CheckPrivilege(", "requirePrivilege("}},
		{file: "internal/service/catalog/views.go", method: "UpdateView", anySnippet: []string{"CheckPrivilege(", "requirePrivilege("}},
		{file: "internal/service/storage/credential.go", method: "Create", anySnippet: []string{"CheckPrivilege(", "requirePrivilege("}},
		{file: "internal/service/storage/credential.go", method: "Update", anySnippet: []string{"CheckPrivilege(", "requirePrivilege("}},
		{file: "internal/service/storage/credential.go", method: "Delete", anySnippet: []string{"CheckPrivilege(", "requirePrivilege("}},
		{file: "internal/service/storage/external_location.go", method: "Create", anySnippet: []string{"CheckPrivilege(", "requirePrivilege("}},
		{file: "internal/service/storage/external_location.go", method: "Update", anySnippet: []string{"CheckPrivilege(", "requirePrivilege("}},
		{file: "internal/service/storage/external_location.go", method: "Delete", anySnippet: []string{"CheckPrivilege(", "requirePrivilege("}},
		{file: "internal/service/storage/volume.go", method: "Create", anySnippet: []string{"CheckPrivilege(", "requirePrivilege("}},
		{file: "internal/service/storage/volume.go", method: "Update", anySnippet: []string{"CheckPrivilege(", "requirePrivilege("}},
		{file: "internal/service/storage/volume.go", method: "Delete", anySnippet: []string{"CheckPrivilege(", "requirePrivilege("}},
		{file: "internal/service/compute/endpoint.go", method: "Create", anySnippet: []string{"CheckPrivilege(", "requirePrivilege("}},
		{file: "internal/service/compute/endpoint.go", method: "Update", anySnippet: []string{"CheckPrivilege(", "requirePrivilege("}},
		{file: "internal/service/compute/endpoint.go", method: "Delete", anySnippet: []string{"CheckPrivilege(", "requirePrivilege("}},
		{file: "internal/service/compute/endpoint.go", method: "UpdateStatus", anySnippet: []string{"CheckPrivilege(", "requirePrivilege("}},
		{file: "internal/service/security/grant.go", method: "Grant", anySnippet: []string{"requireAdmin("}},
		{file: "internal/service/security/grant.go", method: "Revoke", anySnippet: []string{"requireAdmin("}},
	}

	for _, exp := range expects {
		body := methodBody(t, exp.file, exp.method)
		if len(exp.anySnippet) > 0 {
			require.Truef(t, containsAny(body, exp.anySnippet), "governance: %s.%s must include one of %v", exp.file, exp.method, exp.anySnippet)
		}
		for _, snippet := range exp.snippets {
			require.Containsf(t, body, snippet, "governance: %s.%s must contain %q", exp.file, exp.method, snippet)
		}

		if strings.Contains(body, "CheckPrivilege(") {
			require.Containsf(t, body, "logAuditDenied(", "governance: %s.%s has direct CheckPrivilege but no denied audit log", exp.file, exp.method)
		}
	}
}

func TestAuthorizationMatrix_CriticalPrivilegeAndScopeMapping(t *testing.T) {
	t.Helper()

	expects := []methodExpectation{
		{file: "internal/service/catalog/catalog.go", method: "CreateSchema", snippets: []string{"domain.SecurableCatalog", "domain.PrivCreateSchema"}},
		{file: "internal/service/catalog/catalog.go", method: "UpdateSchema", snippets: []string{"domain.SecurableSchema", "domain.PrivModify"}},
		{file: "internal/service/catalog/catalog.go", method: "DeleteSchema", snippets: []string{"domain.SecurableSchema", "domain.PrivManage"}},
		{file: "internal/service/catalog/catalog.go", method: "CreateTable", snippets: []string{"domain.SecurableSchema", "domain.PrivCreateTable"}},
		{file: "internal/service/catalog/catalog.go", method: "DeleteTable", snippets: []string{"domain.SecurableTable", "domain.PrivManage"}},
		{file: "internal/service/catalog/catalog.go", method: "UpdateTable", snippets: []string{"domain.SecurableTable", "domain.PrivModify"}},
		{file: "internal/service/catalog/catalog.go", method: "UpdateColumn", snippets: []string{"domain.SecurableTable", "domain.PrivModify"}},
		{file: "internal/service/catalog/views.go", method: "CreateView", snippets: []string{"domain.SecurableSchema", "domain.PrivCreateView"}},
		{file: "internal/service/catalog/views.go", method: "DeleteView", snippets: []string{"domain.SecurableSchema", "domain.PrivManage"}},
		{file: "internal/service/catalog/views.go", method: "UpdateView", snippets: []string{"domain.SecurableSchema", "domain.PrivModify"}},
		{file: "internal/service/storage/credential.go", method: "Create", snippets: []string{"domain.SecurableCatalog", "domain.PrivCreateStorageCredential"}},
		{file: "internal/service/storage/credential.go", method: "Update", snippets: []string{"domain.SecurableStorageCredential", "domain.PrivManage"}},
		{file: "internal/service/storage/credential.go", method: "Delete", snippets: []string{"domain.SecurableStorageCredential", "domain.PrivManage"}},
		{file: "internal/service/storage/external_location.go", method: "Create", snippets: []string{"domain.SecurableCatalog", "domain.PrivCreateExternalLocation"}},
		{file: "internal/service/storage/external_location.go", method: "Update", snippets: []string{"domain.SecurableExternalLocation", "domain.PrivManage"}},
		{file: "internal/service/storage/external_location.go", method: "Delete", snippets: []string{"domain.SecurableExternalLocation", "domain.PrivManage"}},
		{file: "internal/service/storage/volume.go", method: "Create", snippets: []string{"domain.SecurableCatalog", "domain.PrivCreateVolume"}},
		{file: "internal/service/storage/volume.go", method: "Update", snippets: []string{"domain.SecurableVolume", "domain.PrivManage"}},
		{file: "internal/service/storage/volume.go", method: "Delete", snippets: []string{"domain.SecurableVolume", "domain.PrivManage"}},
		{file: "internal/service/compute/endpoint.go", method: "requirePrivilege", snippets: []string{"domain.SecurableCatalog", "domain.CatalogID"}},
		{file: "internal/service/compute/endpoint.go", method: "Create", snippets: []string{"domain.PrivManageCompute"}},
		{file: "internal/service/compute/endpoint.go", method: "Update", snippets: []string{"domain.PrivManageCompute"}},
		{file: "internal/service/compute/endpoint.go", method: "Delete", snippets: []string{"domain.PrivManageCompute"}},
		{file: "internal/service/compute/endpoint.go", method: "UpdateStatus", snippets: []string{"domain.PrivManageCompute"}},
	}

	for _, exp := range expects {
		body := methodBody(t, exp.file, exp.method)
		for _, snippet := range exp.snippets {
			require.Containsf(t, body, snippet, "governance: %s.%s no longer matches expected auth matrix snippet %q", exp.file, exp.method, snippet)
		}
	}
}

func methodBody(t *testing.T, relPath, method string) string {
	t.Helper()

	absPath := filepath.Join(repoRootDir(), relPath)
	src, err := os.ReadFile(absPath)
	require.NoErrorf(t, err, "read %s", relPath)

	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, absPath, src, parser.ParseComments)
	require.NoErrorf(t, err, "parse %s", relPath)

	for _, decl := range file.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || fn.Body == nil || fn.Name == nil {
			continue
		}
		if fn.Name.Name != method {
			continue
		}
		start := fset.Position(fn.Body.Pos()).Offset
		end := fset.Position(fn.Body.End()).Offset
		if start < 0 || end > len(src) || start >= end {
			require.Failf(t, "invalid function body offsets", "%s.%s", relPath, method)
		}
		return string(src[start:end])
	}

	require.Failf(t, "method not found", "%s.%s", relPath, method)
	return ""
}

func containsAny(value string, snippets []string) bool {
	for _, s := range snippets {
		if strings.Contains(value, s) {
			return true
		}
	}
	return false
}
