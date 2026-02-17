package architecture_test

import (
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

var auditMutationPrefixes = []string{
	"Create",
	"Update",
	"Delete",
	"Cancel",
	"Close",
	"Purge",
	"Register",
	"Profile",
	"Bind",
	"Unbind",
	"Assign",
	"Unassign",
	"Trigger",
	"Reorder",
	"Execute",
	"Run",
	"Set",
	"Attach",
}

// Explicit exceptions for methods that are intentionally non-audited.
// Key format: "path/to/file.go:Receiver.Method".
var auditRuleExceptions = map[string]string{
	"internal/service/catalog/registration.go:CatalogRegistrationService.AttachAll": "startup reconciliation path; audit policy handled at caller/system level",
	"internal/service/notebook/session.go:SessionManager.ExecuteCell":               "high-volume cell execution path; auditing policy handled at run/job level",
	"internal/service/notebook/session.go:SessionManager.RunAll":                    "delegates execution to ExecuteCell; avoid duplicate per-run noise",
}

func TestServiceMutations_AreAudited(t *testing.T) {
	t.Helper()

	serviceRoot := filepath.Join(repoRootDir(), "internal", "service")
	files, err := collectGoFiles(serviceRoot)
	require.NoError(t, err)

	violations := make([]string, 0)

	for _, file := range files {
		if shouldSkipProductionGovernanceFile(file) {
			continue
		}

		fset := token.NewFileSet()
		parsed, parseErr := parser.ParseFile(fset, file, nil, 0)
		require.NoErrorf(t, parseErr, "parse file for audit rules: %s", file)

		relPath := relToRepoRoot(file)
		for _, decl := range parsed.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Recv == nil || fn.Body == nil {
				continue
			}

			receiver := receiverTypeName(fn)
			if !isAuditedReceiver(receiver) {
				continue
			}
			if !isMutatingMethod(fn.Name.Name) {
				continue
			}
			if !hasContextParam(fn) {
				continue
			}

			key := relPath + ":" + receiver + "." + fn.Name.Name
			if _, ok := auditRuleExceptions[key]; ok {
				continue
			}

			if !containsAuditCall(fn.Body) {
				violations = append(violations, key)
			}
		}
	}

	sort.Strings(violations)
	require.Empty(t, violations,
		"service mutating methods must emit audit logs (add logAudit/audit.Insert or add explicit exception):\n%s",
		strings.Join(violations, "\n"),
	)
}

func receiverTypeName(fn *ast.FuncDecl) string {
	if fn.Recv == nil || len(fn.Recv.List) == 0 {
		return ""
	}

	switch rt := fn.Recv.List[0].Type.(type) {
	case *ast.StarExpr:
		if id, ok := rt.X.(*ast.Ident); ok {
			return id.Name
		}
	case *ast.Ident:
		return rt.Name
	}

	return ""
}

func isAuditedReceiver(receiver string) bool {
	if receiver == "" {
		return false
	}
	return strings.HasSuffix(receiver, "Service") || strings.HasSuffix(receiver, "Manager")
}

func isMutatingMethod(name string) bool {
	for _, prefix := range auditMutationPrefixes {
		if strings.HasPrefix(name, prefix) {
			return true
		}
	}
	return false
}

func hasContextParam(fn *ast.FuncDecl) bool {
	if fn.Type == nil || fn.Type.Params == nil {
		return false
	}

	for _, field := range fn.Type.Params.List {
		t, ok := field.Type.(*ast.SelectorExpr)
		if !ok {
			continue
		}

		pkg, ok := t.X.(*ast.Ident)
		if ok && pkg.Name == "context" && t.Sel.Name == "Context" {
			return true
		}
	}

	return false
}

func containsAuditCall(body *ast.BlockStmt) bool {
	found := false
	ast.Inspect(body, func(n ast.Node) bool {
		if found {
			return false
		}

		call, ok := n.(*ast.CallExpr)
		if !ok {
			return true
		}

		switch fun := call.Fun.(type) {
		case *ast.Ident:
			if fun.Name == "logAudit" {
				found = true
				return false
			}
		case *ast.SelectorExpr:
			if fun.Sel.Name == "logAudit" {
				found = true
				return false
			}
			if fun.Sel.Name == "Insert" && expressionContainsAudit(fun.X) {
				found = true
				return false
			}
		}

		return true
	})

	return found
}

func expressionContainsAudit(expr ast.Expr) bool {
	if expr == nil {
		return false
	}

	switch v := expr.(type) {
	case *ast.Ident:
		return strings.Contains(strings.ToLower(v.Name), "audit")
	case *ast.SelectorExpr:
		if strings.Contains(strings.ToLower(v.Sel.Name), "audit") {
			return true
		}
		return expressionContainsAudit(v.X)
	}

	return false
}
