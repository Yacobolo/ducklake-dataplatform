package architecture_test

import (
	"encoding/json"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/getkin/kin-openapi/openapi3"
	"github.com/stretchr/testify/require"

	"duck-demo/pkg/cli/gen"
)

type apiAuthz struct {
	Mode   string
	Checks []apiAuthzCheck
}

type apiAuthzCheck struct {
	SecurableType     string
	Privilege         string
	SecurableIDSource string
}

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
	authzByOperation := loadAuthzByOperation(t)

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
			mode:                 "privilege",
			securableType:        "schema",
			privilege:            "CREATE_SCHEMA",
			securableIDSource:    "runtime_resolved_object_id",
			serviceFile:          "internal/service/catalog/catalog.go",
			serviceMethod:        "UpdateSchema",
			serviceBodySnippets:  []string{"domain.SecurableSchema", "domain.PrivCreateSchema"},
			requiresLookupBefore: true,
		},
		"deleteSchema": {
			mode:                 "privilege",
			securableType:        "schema",
			privilege:            "CREATE_SCHEMA",
			securableIDSource:    "runtime_resolved_object_id",
			serviceFile:          "internal/service/catalog/catalog.go",
			serviceMethod:        "DeleteSchema",
			serviceBodySnippets:  []string{"domain.SecurableSchema", "domain.PrivCreateSchema"},
			requiresLookupBefore: true,
		},
		"createTable": {
			mode:                 "privilege",
			securableType:        "schema",
			privilege:            "CREATE_TABLE",
			securableIDSource:    "runtime_resolved_object_id",
			serviceFile:          "internal/service/catalog/catalog.go",
			serviceMethod:        "CreateTable",
			serviceBodySnippets:  []string{"domain.SecurableSchema", "domain.PrivCreateTable"},
			requiresLookupBefore: true,
		},
		"updateTable": {
			mode:                 "privilege",
			securableType:        "table",
			privilege:            "CREATE_TABLE",
			securableIDSource:    "runtime_resolved_object_id",
			serviceFile:          "internal/service/catalog/catalog.go",
			serviceMethod:        "UpdateTable",
			serviceBodySnippets:  []string{"domain.SecurableTable", "domain.PrivCreateTable"},
			requiresLookupBefore: true,
		},
		"deleteTable": {
			mode:                 "privilege",
			securableType:        "table",
			privilege:            "CREATE_TABLE",
			securableIDSource:    "runtime_resolved_object_id",
			serviceFile:          "internal/service/catalog/catalog.go",
			serviceMethod:        "DeleteTable",
			serviceBodySnippets:  []string{"domain.SecurableTable", "domain.PrivCreateTable"},
			requiresLookupBefore: true,
		},
		"updateColumn": {
			mode:                 "privilege",
			securableType:        "table",
			privilege:            "CREATE_TABLE",
			securableIDSource:    "runtime_resolved_object_id",
			serviceFile:          "internal/service/catalog/catalog.go",
			serviceMethod:        "UpdateColumn",
			serviceBodySnippets:  []string{"domain.SecurableTable", "domain.PrivCreateTable"},
			requiresLookupBefore: true,
		},
		"createView": {
			mode:                 "privilege",
			securableType:        "schema",
			privilege:            "CREATE_TABLE",
			securableIDSource:    "runtime_resolved_object_id",
			serviceFile:          "internal/service/catalog/views.go",
			serviceMethod:        "CreateView",
			serviceBodySnippets:  []string{"domain.SecurableSchema", "domain.PrivCreateTable"},
			requiresLookupBefore: true,
		},
		"updateView": {
			mode:                 "privilege",
			securableType:        "schema",
			privilege:            "CREATE_TABLE",
			securableIDSource:    "runtime_resolved_object_id",
			serviceFile:          "internal/service/catalog/views.go",
			serviceMethod:        "UpdateView",
			serviceBodySnippets:  []string{"domain.SecurableSchema", "domain.PrivCreateTable"},
			requiresLookupBefore: true,
		},
		"deleteView": {
			mode:                 "privilege",
			securableType:        "schema",
			privilege:            "CREATE_TABLE",
			securableIDSource:    "runtime_resolved_object_id",
			serviceFile:          "internal/service/catalog/views.go",
			serviceMethod:        "DeleteView",
			serviceBodySnippets:  []string{"domain.SecurableSchema", "domain.PrivCreateTable"},
			requiresLookupBefore: true,
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
			mode:                 "privilege",
			securableType:        "volume",
			privilege:            "CREATE_VOLUME",
			securableIDSource:    "runtime_resolved_object_id",
			serviceFile:          "internal/service/storage/volume.go",
			serviceMethod:        "Update",
			serviceBodySnippets:  []string{"domain.SecurableVolume", "domain.PrivCreateVolume"},
			requiresLookupBefore: true,
		},
		"deleteVolume": {
			mode:                 "privilege",
			securableType:        "volume",
			privilege:            "CREATE_VOLUME",
			securableIDSource:    "runtime_resolved_object_id",
			serviceFile:          "internal/service/storage/volume.go",
			serviceMethod:        "Delete",
			serviceBodySnippets:  []string{"domain.SecurableVolume", "domain.PrivCreateVolume"},
			requiresLookupBefore: true,
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
			securableType:        "storage_credential",
			privilege:            "CREATE_STORAGE_CREDENTIAL",
			securableIDSource:    "runtime_resolved_object_id",
			serviceFile:          "internal/service/storage/credential.go",
			serviceMethod:        "Update",
			serviceBodySnippets:  []string{"domain.SecurableStorageCredential", "domain.PrivCreateStorageCredential"},
			requiresLookupBefore: true,
		},
		"deleteStorageCredential": {
			mode:                 "privilege",
			securableType:        "storage_credential",
			privilege:            "CREATE_STORAGE_CREDENTIAL",
			securableIDSource:    "runtime_resolved_object_id",
			serviceFile:          "internal/service/storage/credential.go",
			serviceMethod:        "Delete",
			serviceBodySnippets:  []string{"domain.SecurableStorageCredential", "domain.PrivCreateStorageCredential"},
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
			securableType:        "external_location",
			privilege:            "CREATE_EXTERNAL_LOCATION",
			securableIDSource:    "runtime_resolved_object_id",
			serviceFile:          "internal/service/storage/external_location.go",
			serviceMethod:        "Update",
			serviceBodySnippets:  []string{"domain.SecurableExternalLocation", "domain.PrivCreateExternalLocation"},
			requiresLookupBefore: true,
		},
		"deleteExternalLocation": {
			mode:                 "privilege",
			securableType:        "external_location",
			privilege:            "CREATE_EXTERNAL_LOCATION",
			securableIDSource:    "runtime_resolved_object_id",
			serviceFile:          "internal/service/storage/external_location.go",
			serviceMethod:        "Delete",
			serviceBodySnippets:  []string{"domain.SecurableExternalLocation", "domain.PrivCreateExternalLocation"},
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
			mode:                 "privilege",
			securableType:        "compute_endpoint",
			privilege:            "MANAGE_COMPUTE",
			securableIDSource:    "runtime_resolved_object_id",
			serviceFile:          "internal/service/compute/endpoint.go",
			serviceMethod:        "Update",
			serviceBodySnippets:  []string{"requireEndpointPrivilege(", "domain.PrivManageCompute"},
			requiresLookupBefore: true,
		},
		"deleteComputeEndpoint": {
			mode:                 "privilege",
			securableType:        "compute_endpoint",
			privilege:            "MANAGE_COMPUTE",
			securableIDSource:    "runtime_resolved_object_id",
			serviceFile:          "internal/service/compute/endpoint.go",
			serviceMethod:        "Delete",
			serviceBodySnippets:  []string{"requireEndpointPrivilege(", "domain.PrivManageCompute"},
			requiresLookupBefore: true,
		},
		"createComputeAssignment": {
			mode:                 "privilege",
			securableType:        "compute_endpoint",
			privilege:            "MANAGE_COMPUTE",
			securableIDSource:    "runtime_resolved_object_id",
			serviceFile:          "internal/service/compute/endpoint.go",
			serviceMethod:        "Assign",
			serviceBodySnippets:  []string{"requireEndpointPrivilege(", "domain.PrivManageCompute"},
			requiresLookupBefore: true,
		},
		"deleteComputeAssignment": {
			mode:                "privilege",
			securableType:       "catalog",
			privilege:           "MANAGE_COMPUTE",
			securableIDSource:   "catalog_sentinel",
			serviceFile:         "internal/service/compute/endpoint.go",
			serviceMethod:       "Unassign",
			serviceBodySnippets: []string{"requirePrivilege(", "domain.PrivManageCompute"},
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
		_, ok := endpointByOperation[opID]
		require.Truef(t, ok, "missing generated endpoint for %s", opID)
		authz, ok := authzByOperation[opID]
		require.Truef(t, ok, "missing x-authz metadata for %s", opID)
		require.Equalf(t, exp.mode, authz.Mode, "authz mode drift for %s", opID)

		if exp.mode == "privilege" || exp.mode == "owner_or_privilege" {
			require.NotEmptyf(t, authz.Checks, "authz checks missing for %s", opID)
			require.Truef(t, hasMatchingCheck(authz.Checks, exp), "authz check drift for %s", opID)
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

func hasMatchingCheck(checks []apiAuthzCheck, exp authzContractExpectation) bool {
	for _, check := range checks {
		if check.SecurableType == exp.securableType && check.Privilege == exp.privilege && check.SecurableIDSource == exp.securableIDSource {
			return true
		}
	}
	return false
}

func loadAuthzByOperation(t *testing.T) map[string]apiAuthz {
	t.Helper()

	specPath := filepath.Join(repoRootDir(), "internal", "api", "openapi.bundled.yaml")
	loader := openapi3.NewLoader()
	doc, err := loader.LoadFromFile(specPath)
	require.NoErrorf(t, err, "load openapi spec %s", specPath)

	result := make(map[string]apiAuthz)
	for _, pathItem := range doc.Paths.Map() {
		for _, operation := range pathItem.Operations() {
			if operation == nil || operation.OperationID == "" {
				continue
			}
			authz, ok := parseOperationAuthz(operation)
			if !ok {
				continue
			}
			result[operation.OperationID] = authz
		}
	}

	return result
}

func parseOperationAuthz(operation *openapi3.Operation) (apiAuthz, bool) {
	if operation == nil || len(operation.Extensions) == 0 {
		return apiAuthz{}, false
	}

	raw, ok := operation.Extensions["x-authz"]
	if !ok {
		return apiAuthz{}, false
	}

	m, ok := toStringAnyMap(raw)
	if !ok {
		return apiAuthz{}, false
	}

	authz := apiAuthz{Mode: stringField(m, "mode")}
	items, ok := m["checks"].([]interface{})
	if !ok {
		return authz, true
	}
	for _, item := range items {
		checkMap, ok := item.(map[string]interface{})
		if !ok {
			continue
		}
		authz.Checks = append(authz.Checks, apiAuthzCheck{
			SecurableType:     stringField(checkMap, "securable_type"),
			Privilege:         stringField(checkMap, "privilege"),
			SecurableIDSource: stringField(checkMap, "securable_id_source"),
		})
	}

	return authz, true
}

func toStringAnyMap(value interface{}) (map[string]interface{}, bool) {
	switch typed := value.(type) {
	case map[string]interface{}:
		return typed, true
	case map[interface{}]interface{}:
		out := make(map[string]interface{}, len(typed))
		for key, val := range typed {
			s, ok := key.(string)
			if !ok {
				continue
			}
			out[s] = val
		}
		return out, true
	case []byte:
		var out map[string]interface{}
		if err := json.Unmarshal(typed, &out); err != nil {
			return nil, false
		}
		return out, true
	case json.RawMessage:
		var out map[string]interface{}
		if err := json.Unmarshal(typed, &out); err != nil {
			return nil, false
		}
		return out, true
	default:
		return nil, false
	}
}

func stringField(m map[string]interface{}, key string) string {
	v, ok := m[key]
	if !ok {
		return ""
	}
	s, ok := v.(string)
	if !ok {
		return ""
	}
	return s
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
