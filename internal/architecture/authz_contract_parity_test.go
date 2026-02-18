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

	"duck-demo/pkg/cli/gen"
)

type authzContractExpectation struct {
	mode                 string
	securableType        string
	privilege            string
	securableIDSource    string
	serviceFile          string
	serviceMethod        string
	serviceBodySnippets  []string
	requiresLookupBefore bool
}

func TestAuthzContractParity_CriticalEndpoints(t *testing.T) {
	endpointByOperation := make(map[string]gen.APIEndpoint, len(gen.APIEndpoints))
	for _, endpoint := range gen.APIEndpoints {
		endpointByOperation[endpoint.OperationID] = endpoint
	}

	expects := map[string]authzContractExpectation{
		"createSchema": {
			mode:                "privilege",
			securableType:       "catalog",
			privilege:           "CREATE_SCHEMA",
			securableIDSource:   "catalog_name_param",
			serviceFile:         "internal/service/catalog/catalog.go",
			serviceMethod:       "CreateSchema",
			serviceBodySnippets: []string{"domain.SecurableCatalog", "domain.PrivCreateSchema"},
		},
		"updateSchema": {
			mode:                "privilege",
			securableType:       "catalog",
			privilege:           "CREATE_SCHEMA",
			securableIDSource:   "catalog_sentinel",
			serviceFile:         "internal/service/catalog/catalog.go",
			serviceMethod:       "UpdateSchema",
			serviceBodySnippets: []string{"domain.SecurableCatalog", "domain.PrivCreateSchema"},
		},
		"deleteSchema": {
			mode:                "privilege",
			securableType:       "catalog",
			privilege:           "CREATE_SCHEMA",
			securableIDSource:   "catalog_sentinel",
			serviceFile:         "internal/service/catalog/catalog.go",
			serviceMethod:       "DeleteSchema",
			serviceBodySnippets: []string{"domain.SecurableCatalog", "domain.PrivCreateSchema"},
		},
		"createTable": {
			mode:                "privilege",
			securableType:       "catalog",
			privilege:           "CREATE_TABLE",
			securableIDSource:   "catalog_sentinel",
			serviceFile:         "internal/service/catalog/catalog.go",
			serviceMethod:       "CreateTable",
			serviceBodySnippets: []string{"domain.SecurableCatalog", "domain.PrivCreateTable"},
		},
		"updateTable": {
			mode:                "privilege",
			securableType:       "catalog",
			privilege:           "CREATE_TABLE",
			securableIDSource:   "catalog_sentinel",
			serviceFile:         "internal/service/catalog/catalog.go",
			serviceMethod:       "UpdateTable",
			serviceBodySnippets: []string{"domain.SecurableCatalog", "domain.PrivCreateTable"},
		},
		"deleteTable": {
			mode:                "privilege",
			securableType:       "catalog",
			privilege:           "CREATE_TABLE",
			securableIDSource:   "catalog_sentinel",
			serviceFile:         "internal/service/catalog/catalog.go",
			serviceMethod:       "DeleteTable",
			serviceBodySnippets: []string{"domain.SecurableCatalog", "domain.PrivCreateTable"},
		},
		"updateColumn": {
			mode:                "privilege",
			securableType:       "catalog",
			privilege:           "CREATE_TABLE",
			securableIDSource:   "catalog_sentinel",
			serviceFile:         "internal/service/catalog/catalog.go",
			serviceMethod:       "UpdateColumn",
			serviceBodySnippets: []string{"domain.SecurableCatalog", "domain.PrivCreateTable"},
		},
		"createView": {
			mode:                "privilege",
			securableType:       "catalog",
			privilege:           "CREATE_TABLE",
			securableIDSource:   "catalog_sentinel",
			serviceFile:         "internal/service/catalog/views.go",
			serviceMethod:       "CreateView",
			serviceBodySnippets: []string{"domain.SecurableCatalog", "domain.PrivCreateTable"},
		},
		"updateView": {
			mode:                "privilege",
			securableType:       "catalog",
			privilege:           "CREATE_TABLE",
			securableIDSource:   "catalog_sentinel",
			serviceFile:         "internal/service/catalog/views.go",
			serviceMethod:       "UpdateView",
			serviceBodySnippets: []string{"domain.SecurableCatalog", "domain.PrivCreateTable"},
		},
		"deleteView": {
			mode:                "privilege",
			securableType:       "catalog",
			privilege:           "CREATE_TABLE",
			securableIDSource:   "catalog_sentinel",
			serviceFile:         "internal/service/catalog/views.go",
			serviceMethod:       "DeleteView",
			serviceBodySnippets: []string{"domain.SecurableCatalog", "domain.PrivCreateTable"},
		},
		"createVolume": {
			mode:                "privilege",
			securableType:       "catalog",
			privilege:           "CREATE_VOLUME",
			securableIDSource:   "catalog_sentinel",
			serviceFile:         "internal/service/storage/volume.go",
			serviceMethod:       "Create",
			serviceBodySnippets: []string{"requirePrivilege(", "domain.PrivCreateVolume"},
		},
		"updateVolume": {
			mode:                "privilege",
			securableType:       "catalog",
			privilege:           "CREATE_VOLUME",
			securableIDSource:   "catalog_sentinel",
			serviceFile:         "internal/service/storage/volume.go",
			serviceMethod:       "Update",
			serviceBodySnippets: []string{"requirePrivilege(", "domain.PrivCreateVolume"},
		},
		"deleteVolume": {
			mode:                "privilege",
			securableType:       "catalog",
			privilege:           "CREATE_VOLUME",
			securableIDSource:   "catalog_sentinel",
			serviceFile:         "internal/service/storage/volume.go",
			serviceMethod:       "Delete",
			serviceBodySnippets: []string{"requirePrivilege(", "domain.PrivCreateVolume"},
		},
		"createStorageCredential": {
			mode:                "privilege",
			securableType:       "catalog",
			privilege:           "CREATE_STORAGE_CREDENTIAL",
			securableIDSource:   "catalog_sentinel",
			serviceFile:         "internal/service/storage/credential.go",
			serviceMethod:       "Create",
			serviceBodySnippets: []string{"requirePrivilege(", "domain.PrivCreateStorageCredential"},
		},
		"updateStorageCredential": {
			mode:                 "privilege",
			securableType:        "catalog",
			privilege:            "CREATE_STORAGE_CREDENTIAL",
			securableIDSource:    "catalog_sentinel",
			serviceFile:          "internal/service/storage/credential.go",
			serviceMethod:        "Update",
			serviceBodySnippets:  []string{"requirePrivilege(", "domain.PrivCreateStorageCredential"},
			requiresLookupBefore: true,
		},
		"deleteStorageCredential": {
			mode:                 "privilege",
			securableType:        "catalog",
			privilege:            "CREATE_STORAGE_CREDENTIAL",
			securableIDSource:    "catalog_sentinel",
			serviceFile:          "internal/service/storage/credential.go",
			serviceMethod:        "Delete",
			serviceBodySnippets:  []string{"requirePrivilege(", "domain.PrivCreateStorageCredential"},
			requiresLookupBefore: true,
		},
		"createExternalLocation": {
			mode:                "privilege",
			securableType:       "catalog",
			privilege:           "CREATE_EXTERNAL_LOCATION",
			securableIDSource:   "catalog_sentinel",
			serviceFile:         "internal/service/storage/external_location.go",
			serviceMethod:       "Create",
			serviceBodySnippets: []string{"requirePrivilege(", "domain.PrivCreateExternalLocation"},
		},
		"updateExternalLocation": {
			mode:                 "privilege",
			securableType:        "catalog",
			privilege:            "CREATE_EXTERNAL_LOCATION",
			securableIDSource:    "catalog_sentinel",
			serviceFile:          "internal/service/storage/external_location.go",
			serviceMethod:        "Update",
			serviceBodySnippets:  []string{"requirePrivilege(", "domain.PrivCreateExternalLocation"},
			requiresLookupBefore: true,
		},
		"deleteExternalLocation": {
			mode:                 "privilege",
			securableType:        "catalog",
			privilege:            "CREATE_EXTERNAL_LOCATION",
			securableIDSource:    "catalog_sentinel",
			serviceFile:          "internal/service/storage/external_location.go",
			serviceMethod:        "Delete",
			serviceBodySnippets:  []string{"requirePrivilege(", "domain.PrivCreateExternalLocation"},
			requiresLookupBefore: true,
		},
		"createComputeEndpoint": {
			mode:                "privilege",
			securableType:       "catalog",
			privilege:           "MANAGE_COMPUTE",
			securableIDSource:   "catalog_sentinel",
			serviceFile:         "internal/service/compute/endpoint.go",
			serviceMethod:       "Create",
			serviceBodySnippets: []string{"domain.PrivManageCompute"},
		},
		"updateComputeEndpoint": {
			mode:                "privilege",
			securableType:       "catalog",
			privilege:           "MANAGE_COMPUTE",
			securableIDSource:   "catalog_sentinel",
			serviceFile:         "internal/service/compute/endpoint.go",
			serviceMethod:       "Update",
			serviceBodySnippets: []string{"domain.PrivManageCompute"},
		},
		"deleteComputeEndpoint": {
			mode:                "privilege",
			securableType:       "catalog",
			privilege:           "MANAGE_COMPUTE",
			securableIDSource:   "catalog_sentinel",
			serviceFile:         "internal/service/compute/endpoint.go",
			serviceMethod:       "Delete",
			serviceBodySnippets: []string{"domain.PrivManageCompute"},
		},
		"createGrant": {
			mode:                "admin_only",
			serviceFile:         "internal/service/security/grant.go",
			serviceMethod:       "Grant",
			serviceBodySnippets: []string{"requireAdmin("},
		},
		"deleteGrant": {
			mode:                "admin_only",
			serviceFile:         "internal/service/security/grant.go",
			serviceMethod:       "Revoke",
			serviceBodySnippets: []string{"requireAdmin("},
		},
	}

	for opID, exp := range expects {
		endpoint, ok := endpointByOperation[opID]
		require.Truef(t, ok, "missing generated endpoint for %s", opID)
		require.Equalf(t, exp.mode, endpoint.Authz.Mode, "authz mode drift for %s", opID)

		if exp.mode == "privilege" || exp.mode == "owner_or_privilege" {
			require.NotEmptyf(t, endpoint.Authz.Checks, "authz checks missing for %s", opID)
			require.Truef(t, hasMatchingCheck(endpoint.Authz.Checks, exp), "authz check drift for %s", opID)
		}

		body := authzMethodBody(t, exp.serviceFile, exp.serviceMethod)
		for _, snippet := range exp.serviceBodySnippets {
			require.Containsf(t, body, snippet, "service check drift for %s (%s.%s)", opID, exp.serviceFile, exp.serviceMethod)
		}
		if exp.requiresLookupBefore {
			require.Truef(t, containsAnySnippet(body, []string{"GetByName(", "GetTable(", "GetSchema("}), "runtime lookup missing for %s", opID)
		}
	}
}

func hasMatchingCheck(checks []gen.APIAuthzCheck, exp authzContractExpectation) bool {
	for _, check := range checks {
		if check.SecurableType == exp.securableType && check.Privilege == exp.privilege && check.SecurableIDSource == exp.securableIDSource {
			return true
		}
	}
	return false
}

func authzMethodBody(t *testing.T, relPath, method string) string {
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

func containsAnySnippet(value string, snippets []string) bool {
	for _, snippet := range snippets {
		if strings.Contains(value, snippet) {
			return true
		}
	}
	return false
}
