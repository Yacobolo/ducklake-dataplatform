package cli

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"duck-demo/internal/declarative"
	"duck-demo/pkg/cli/gen"
)

// execCapture stores method, path, body, and query for assertion in execute tests.
type execCapture struct {
	Method string
	Path   string
	Body   map[string]interface{}
	Query  map[string][]string
}

// newTestExecuteClient creates an APIStateClient backed by an httptest server
// that captures all requests and responds with 201 + {"id":"generated-uuid-123"}.
func newTestExecuteClient(t *testing.T, captured *[]execCapture) *APIStateClient {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ec := execCapture{
			Method: r.Method,
			Path:   r.URL.Path,
			Query:  r.URL.Query(),
		}
		if r.Body != nil {
			data, _ := io.ReadAll(r.Body)
			if len(data) > 0 {
				var m map[string]interface{}
				_ = json.Unmarshal(data, &m)
				ec.Body = m
			}
		}
		*captured = append(*captured, ec)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(`{"id":"generated-uuid-123"}`))
	}))
	t.Cleanup(srv.Close)

	client := gen.NewClient(srv.URL, "", "test-token")
	sc := NewAPIStateClient(client)
	sc.index = newResourceIndex()
	return sc
}

// withTestIndex pre-populates the resourceIndex for resolution tests.
func withTestIndex(sc *APIStateClient) *APIStateClient {
	sc.index.principalIDByName["alice"] = "principal-id-alice"
	sc.index.principalIDByName["bob"] = "principal-id-bob"
	sc.index.groupIDByName["admins"] = "group-id-admins"
	sc.index.groupIDByName["analysts"] = "group-id-analysts"
	sc.index.catalogIDByName["demo"] = "catalog-id-demo"
	sc.index.schemaIDByPath["demo.titanic"] = "schema-id-titanic"
	sc.index.tableIDByPath["demo.titanic.passengers"] = "table-id-passengers"
	sc.index.tagIDByKey["pii"] = "tag-id-pii"
	sc.index.tagIDByKey["pii:email"] = "tag-id-pii-email"
	sc.index.rowFilterIDByPath["demo.titanic.passengers/first_class"] = "rf-id-first"
	sc.index.columnMaskIDByPath["demo.titanic.passengers/mask_name"] = "cm-id-name"
	sc.index.notebookIDByName["nb1"] = "notebook-id-1"
	sc.index.pipelineIDByName["pipe1"] = "pipeline-id-1"
	sc.index.jobIDByPath["pipe1/job1"] = "job-id-1"
	sc.index.computeIDByName["local"] = "compute-id-local"
	return sc
}

func bodyStr(ec execCapture, key string) string {
	v, _ := ec.Body[key].(string)
	return v
}

func bodyBool(ec execCapture, key string) bool {
	v, _ := ec.Body[key].(bool)
	return v
}

func queryStr(ec execCapture, key string) string {
	vals := ec.Query[key]
	if len(vals) > 0 {
		return vals[0]
	}
	return ""
}

// === Catalog execution tests (#129) ===

func TestExecuteCatalog_CreateSendsName(t *testing.T) {
	var captured []execCapture
	sc := newTestExecuteClient(t, &captured)

	action := declarative.Action{
		Operation:    declarative.OpCreate,
		ResourceKind: declarative.KindCatalogRegistration,
		ResourceName: "demo",
		Desired: declarative.CatalogResource{
			CatalogName: "demo",
			Spec: declarative.CatalogSpec{
				MetastoreType: "sqlite",
				DSN:           "/tmp/meta.sqlite",
				DataPath:      "/tmp/data/",
				IsDefault:     true,
				Comment:       "test catalog",
			},
		},
	}

	err := sc.Execute(context.Background(), action)
	require.NoError(t, err)
	require.Len(t, captured, 1)

	req := captured[0]
	assert.Equal(t, http.MethodPost, req.Method)
	assert.Contains(t, req.Path, "/catalogs")
	assert.Equal(t, "demo", bodyStr(req, "name"))
	assert.Equal(t, "sqlite", bodyStr(req, "metastore_type"))
	assert.Equal(t, "/tmp/meta.sqlite", bodyStr(req, "dsn"))
	assert.Equal(t, "/tmp/data/", bodyStr(req, "data_path"))
	assert.True(t, bodyBool(req, "is_default"))
	assert.Equal(t, "test catalog", bodyStr(req, "comment"))
}

func TestExecuteCatalog_Update(t *testing.T) {
	var captured []execCapture
	sc := newTestExecuteClient(t, &captured)

	action := declarative.Action{
		Operation:    declarative.OpUpdate,
		ResourceKind: declarative.KindCatalogRegistration,
		ResourceName: "demo",
		Desired: declarative.CatalogResource{
			CatalogName: "demo",
			Spec: declarative.CatalogSpec{
				MetastoreType: "sqlite",
				DSN:           "/tmp/meta2.sqlite",
				DataPath:      "/tmp/data2/",
			},
		},
	}

	err := sc.Execute(context.Background(), action)
	require.NoError(t, err)
	require.Len(t, captured, 1)

	req := captured[0]
	assert.Equal(t, http.MethodPatch, req.Method)
	assert.Contains(t, req.Path, "/catalogs/demo")
	assert.Equal(t, "sqlite", bodyStr(req, "metastore_type"))
}

// === Grant execution tests (#130) ===

func TestExecuteGrant_ResolvesNamesToIDs(t *testing.T) {
	var captured []execCapture
	sc := withTestIndex(newTestExecuteClient(t, &captured))

	action := declarative.Action{
		Operation:    declarative.OpCreate,
		ResourceKind: declarative.KindPrivilegeGrant,
		ResourceName: "admins->schema:demo.titanic:ALL_PRIVILEGES",
		Desired: declarative.GrantSpec{
			Principal:     "admins",
			PrincipalType: "group",
			SecurableType: "schema",
			Securable:     "demo.titanic",
			Privilege:     "ALL_PRIVILEGES",
		},
	}

	err := sc.Execute(context.Background(), action)
	require.NoError(t, err)
	require.Len(t, captured, 1)

	req := captured[0]
	assert.Equal(t, http.MethodPost, req.Method)
	assert.Contains(t, req.Path, "/grants")
	assert.Equal(t, "group-id-admins", bodyStr(req, "principal_id"))
	assert.Equal(t, "group", bodyStr(req, "principal_type"))
	assert.Equal(t, "schema-id-titanic", bodyStr(req, "securable_id"))
	assert.Equal(t, "schema", bodyStr(req, "securable_type"))
	assert.Equal(t, "ALL_PRIVILEGES", bodyStr(req, "privilege"))
}

func TestExecuteGrant_Delete(t *testing.T) {
	var captured []execCapture
	sc := withTestIndex(newTestExecuteClient(t, &captured))

	action := declarative.Action{
		Operation:    declarative.OpDelete,
		ResourceKind: declarative.KindPrivilegeGrant,
		ResourceName: "admins->schema:demo.titanic:ALL_PRIVILEGES",
		Actual: declarative.GrantSpec{
			Principal:     "admins",
			PrincipalType: "group",
			SecurableType: "schema",
			Securable:     "demo.titanic",
			Privilege:     "ALL_PRIVILEGES",
		},
	}

	err := sc.Execute(context.Background(), action)
	require.NoError(t, err)
	require.Len(t, captured, 1)

	req := captured[0]
	assert.Equal(t, http.MethodDelete, req.Method)
	assert.Equal(t, "group-id-admins", bodyStr(req, "principal_id"))
}

// === Group membership execution tests (#128) ===

func TestExecuteGroupMembership_Create(t *testing.T) {
	var captured []execCapture
	sc := withTestIndex(newTestExecuteClient(t, &captured))

	action := declarative.Action{
		Operation:    declarative.OpCreate,
		ResourceKind: declarative.KindGroupMembership,
		ResourceName: "analysts/alice(user)",
		Desired: declarative.MemberRef{
			Name: "alice",
			Type: "user",
		},
	}

	err := sc.Execute(context.Background(), action)
	require.NoError(t, err)
	require.Len(t, captured, 1)

	req := captured[0]
	assert.Equal(t, http.MethodPost, req.Method)
	assert.Contains(t, req.Path, "/groups/group-id-analysts/members")
	assert.Equal(t, "principal-id-alice", bodyStr(req, "member_id"))
	assert.Equal(t, "user", bodyStr(req, "member_type"))
}

func TestExecuteGroupMembership_Delete(t *testing.T) {
	var captured []execCapture
	sc := withTestIndex(newTestExecuteClient(t, &captured))

	action := declarative.Action{
		Operation:    declarative.OpDelete,
		ResourceKind: declarative.KindGroupMembership,
		ResourceName: "analysts/alice(user)",
		Actual: declarative.MemberRef{
			Name: "alice",
			Type: "user",
		},
	}

	err := sc.Execute(context.Background(), action)
	require.NoError(t, err)
	require.Len(t, captured, 1)

	req := captured[0]
	assert.Equal(t, http.MethodDelete, req.Method)
	assert.Contains(t, req.Path, "/groups/group-id-analysts/members")
	assert.Equal(t, "principal-id-alice", queryStr(req, "member_id"))
	assert.Equal(t, "user", queryStr(req, "member_type"))
}

// === Tag execution tests (#128) ===

func TestExecuteTag_Create(t *testing.T) {
	var captured []execCapture
	sc := newTestExecuteClient(t, &captured)

	value := "email"
	action := declarative.Action{
		Operation:    declarative.OpCreate,
		ResourceKind: declarative.KindTag,
		ResourceName: "pii:email",
		Desired: declarative.TagSpec{
			Key:   "pii",
			Value: &value,
		},
	}

	err := sc.Execute(context.Background(), action)
	require.NoError(t, err)
	require.Len(t, captured, 1)

	req := captured[0]
	assert.Equal(t, http.MethodPost, req.Method)
	assert.Contains(t, req.Path, "/tags")
	assert.Equal(t, "pii", bodyStr(req, "key"))
	assert.Equal(t, "email", bodyStr(req, "value"))

	// The tag ID should be registered in the index.
	assert.Equal(t, "generated-uuid-123", sc.index.tagIDByKey["pii:email"])
}

func TestExecuteTag_Delete(t *testing.T) {
	var captured []execCapture
	sc := withTestIndex(newTestExecuteClient(t, &captured))

	action := declarative.Action{
		Operation:    declarative.OpDelete,
		ResourceKind: declarative.KindTag,
		ResourceName: "pii",
	}

	err := sc.Execute(context.Background(), action)
	require.NoError(t, err)
	require.Len(t, captured, 1)

	req := captured[0]
	assert.Equal(t, http.MethodDelete, req.Method)
	assert.Contains(t, req.Path, "/tags/tag-id-pii")
}

// === Tag assignment execution tests (#128) ===

func TestExecuteTagAssignment_Create(t *testing.T) {
	var captured []execCapture
	sc := withTestIndex(newTestExecuteClient(t, &captured))

	action := declarative.Action{
		Operation:    declarative.OpCreate,
		ResourceKind: declarative.KindTagAssignment,
		ResourceName: "pii:email on table.demo.titanic.passengers",
		Desired: declarative.TagAssignmentSpec{
			Tag:           "pii:email",
			SecurableType: "table",
			Securable:     "demo.titanic.passengers",
		},
	}

	err := sc.Execute(context.Background(), action)
	require.NoError(t, err)
	require.Len(t, captured, 1)

	req := captured[0]
	assert.Equal(t, http.MethodPost, req.Method)
	assert.Contains(t, req.Path, "/tags/tag-id-pii-email/assignments")
	assert.Equal(t, "table-id-passengers", bodyStr(req, "securable_id"))
	assert.Equal(t, "table", bodyStr(req, "securable_type"))
}

// === Row filter execution tests (#128) ===

func TestExecuteRowFilter_Create(t *testing.T) {
	var captured []execCapture
	sc := withTestIndex(newTestExecuteClient(t, &captured))

	action := declarative.Action{
		Operation:    declarative.OpCreate,
		ResourceKind: declarative.KindRowFilter,
		ResourceName: "demo.titanic.passengers/first_class",
		Desired: declarative.RowFilterSpec{
			Name:        "first_class",
			FilterSQL:   `"Pclass" = 1`,
			Description: "First class only",
		},
	}

	err := sc.Execute(context.Background(), action)
	require.NoError(t, err)
	require.Len(t, captured, 1)

	req := captured[0]
	assert.Equal(t, http.MethodPost, req.Method)
	assert.Contains(t, req.Path, "/tables/table-id-passengers/row-filters")
	assert.Equal(t, `"Pclass" = 1`, bodyStr(req, "filter_sql"))
	assert.Equal(t, "First class only", bodyStr(req, "description"))

	// Filter ID should be registered for binding actions.
	assert.Equal(t, "generated-uuid-123", sc.index.rowFilterIDByPath["demo.titanic.passengers/first_class"])
}

func TestExecuteRowFilter_Delete(t *testing.T) {
	var captured []execCapture
	sc := withTestIndex(newTestExecuteClient(t, &captured))

	action := declarative.Action{
		Operation:    declarative.OpDelete,
		ResourceKind: declarative.KindRowFilter,
		ResourceName: "demo.titanic.passengers/first_class",
	}

	err := sc.Execute(context.Background(), action)
	require.NoError(t, err)
	require.Len(t, captured, 1)

	req := captured[0]
	assert.Equal(t, http.MethodDelete, req.Method)
	assert.Contains(t, req.Path, "/row-filters/rf-id-first")
}

// === Row filter binding execution tests (#128) ===

func TestExecuteRowFilterBinding_Create(t *testing.T) {
	var captured []execCapture
	sc := withTestIndex(newTestExecuteClient(t, &captured))

	action := declarative.Action{
		Operation:    declarative.OpCreate,
		ResourceKind: declarative.KindRowFilterBinding,
		ResourceName: "demo.titanic.passengers/first_class->user:alice",
		Desired: declarative.FilterBindingRef{
			Principal:     "alice",
			PrincipalType: "user",
		},
	}

	err := sc.Execute(context.Background(), action)
	require.NoError(t, err)
	require.Len(t, captured, 1)

	req := captured[0]
	assert.Equal(t, http.MethodPost, req.Method)
	assert.Contains(t, req.Path, "/row-filters/rf-id-first/bindings")
	assert.Equal(t, "principal-id-alice", bodyStr(req, "principal_id"))
	assert.Equal(t, "user", bodyStr(req, "principal_type"))
}

func TestExecuteRowFilterBinding_Delete(t *testing.T) {
	var captured []execCapture
	sc := withTestIndex(newTestExecuteClient(t, &captured))

	action := declarative.Action{
		Operation:    declarative.OpDelete,
		ResourceKind: declarative.KindRowFilterBinding,
		ResourceName: "demo.titanic.passengers/first_class->user:alice",
		Actual: declarative.FilterBindingRef{
			Principal:     "alice",
			PrincipalType: "user",
		},
	}

	err := sc.Execute(context.Background(), action)
	require.NoError(t, err)
	require.Len(t, captured, 1)

	req := captured[0]
	assert.Equal(t, http.MethodDelete, req.Method)
	assert.Contains(t, req.Path, "/row-filters/rf-id-first/bindings")
	assert.Equal(t, "principal-id-alice", queryStr(req, "principal_id"))
	assert.Equal(t, "user", queryStr(req, "principal_type"))
}

// === Column mask execution tests (#128) ===

func TestExecuteColumnMask_Create(t *testing.T) {
	var captured []execCapture
	sc := withTestIndex(newTestExecuteClient(t, &captured))

	action := declarative.Action{
		Operation:    declarative.OpCreate,
		ResourceKind: declarative.KindColumnMask,
		ResourceName: "demo.titanic.passengers/mask_name",
		Desired: declarative.ColumnMaskSpec{
			Name:           "mask_name",
			ColumnName:     "Name",
			MaskExpression: "'***'",
			Description:    "Mask PII names",
		},
	}

	err := sc.Execute(context.Background(), action)
	require.NoError(t, err)
	require.Len(t, captured, 1)

	req := captured[0]
	assert.Equal(t, http.MethodPost, req.Method)
	assert.Contains(t, req.Path, "/tables/table-id-passengers/column-masks")
	assert.Equal(t, "Name", bodyStr(req, "column_name"))
	assert.Equal(t, "'***'", bodyStr(req, "mask_expression"))
	assert.Equal(t, "Mask PII names", bodyStr(req, "description"))

	// Mask ID should be registered for binding actions.
	assert.Equal(t, "generated-uuid-123", sc.index.columnMaskIDByPath["demo.titanic.passengers/mask_name"])
}

func TestExecuteColumnMask_Delete(t *testing.T) {
	var captured []execCapture
	sc := withTestIndex(newTestExecuteClient(t, &captured))

	action := declarative.Action{
		Operation:    declarative.OpDelete,
		ResourceKind: declarative.KindColumnMask,
		ResourceName: "demo.titanic.passengers/mask_name",
	}

	err := sc.Execute(context.Background(), action)
	require.NoError(t, err)
	require.Len(t, captured, 1)

	req := captured[0]
	assert.Equal(t, http.MethodDelete, req.Method)
	assert.Contains(t, req.Path, "/column-masks/cm-id-name")
}

// === Column mask binding execution tests (#128) ===

func TestExecuteColumnMaskBinding_Create(t *testing.T) {
	var captured []execCapture
	sc := withTestIndex(newTestExecuteClient(t, &captured))

	action := declarative.Action{
		Operation:    declarative.OpCreate,
		ResourceKind: declarative.KindColumnMaskBinding,
		ResourceName: "demo.titanic.passengers/mask_name->user:alice",
		Desired: declarative.MaskBindingRef{
			Principal:     "alice",
			PrincipalType: "user",
			SeeOriginal:   true,
		},
	}

	err := sc.Execute(context.Background(), action)
	require.NoError(t, err)
	require.Len(t, captured, 1)

	req := captured[0]
	assert.Equal(t, http.MethodPost, req.Method)
	assert.Contains(t, req.Path, "/column-masks/cm-id-name/bindings")
	assert.Equal(t, "principal-id-alice", bodyStr(req, "principal_id"))
	assert.Equal(t, "user", bodyStr(req, "principal_type"))
	assert.True(t, bodyBool(req, "see_original"))
}

func TestExecuteColumnMaskBinding_Delete(t *testing.T) {
	var captured []execCapture
	sc := withTestIndex(newTestExecuteClient(t, &captured))

	action := declarative.Action{
		Operation:    declarative.OpDelete,
		ResourceKind: declarative.KindColumnMaskBinding,
		ResourceName: "demo.titanic.passengers/mask_name->user:alice",
		Actual: declarative.MaskBindingRef{
			Principal:     "alice",
			PrincipalType: "user",
		},
	}

	err := sc.Execute(context.Background(), action)
	require.NoError(t, err)
	require.Len(t, captured, 1)

	req := captured[0]
	assert.Equal(t, http.MethodDelete, req.Method)
	assert.Contains(t, req.Path, "/column-masks/cm-id-name/bindings")
	assert.Equal(t, "principal-id-alice", queryStr(req, "principal_id"))
	assert.Equal(t, "user", queryStr(req, "principal_type"))
}

// === Principal execution tests (ID capture) ===

func TestExecutePrincipal_CreateCapturesID(t *testing.T) {
	var captured []execCapture
	sc := newTestExecuteClient(t, &captured)

	action := declarative.Action{
		Operation:    declarative.OpCreate,
		ResourceKind: declarative.KindPrincipal,
		ResourceName: "alice",
		Desired: declarative.PrincipalSpec{
			Name:    "alice",
			Type:    "user",
			IsAdmin: false,
		},
	}

	err := sc.Execute(context.Background(), action)
	require.NoError(t, err)

	assert.Equal(t, "generated-uuid-123", sc.index.principalIDByName["alice"])
}

// === Group execution tests (ID capture) ===

func TestExecuteGroup_CreateCapturesID(t *testing.T) {
	var captured []execCapture
	sc := newTestExecuteClient(t, &captured)

	action := declarative.Action{
		Operation:    declarative.OpCreate,
		ResourceKind: declarative.KindGroup,
		ResourceName: "analysts",
		Desired: declarative.GroupSpec{
			Name:        "analysts",
			Description: "Data analysts",
		},
	}

	err := sc.Execute(context.Background(), action)
	require.NoError(t, err)
	require.Len(t, captured, 1)

	req := captured[0]
	assert.Equal(t, "analysts", bodyStr(req, "name"))
	assert.Equal(t, "Data analysts", bodyStr(req, "description"))
	assert.Equal(t, "generated-uuid-123", sc.index.groupIDByName["analysts"])
}

// === Resolver error tests ===

func TestExecuteGrant_FailsWithoutIndex(t *testing.T) {
	var captured []execCapture
	sc := newTestExecuteClient(t, &captured)
	sc.index = nil // simulate no ReadState call

	action := declarative.Action{
		Operation:    declarative.OpCreate,
		ResourceKind: declarative.KindPrivilegeGrant,
		Desired: declarative.GrantSpec{
			Principal:     "admins",
			PrincipalType: "group",
			SecurableType: "schema",
			Securable:     "demo.titanic",
			Privilege:     "SELECT",
		},
	}

	err := sc.Execute(context.Background(), action)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "resource index not populated")
}

func TestExecuteGrant_FailsUnknownPrincipal(t *testing.T) {
	var captured []execCapture
	sc := newTestExecuteClient(t, &captured)

	action := declarative.Action{
		Operation:    declarative.OpCreate,
		ResourceKind: declarative.KindPrivilegeGrant,
		Desired: declarative.GrantSpec{
			Principal:     "nonexistent",
			PrincipalType: "group",
			SecurableType: "schema",
			Securable:     "demo.titanic",
			Privilege:     "SELECT",
		},
	}

	err := sc.Execute(context.Background(), action)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not found in index")
}

// === Principal update and delete execution tests (#138) ===

func TestExecutePrincipal_UpdateSendsAdminEndpoint(t *testing.T) {
	var captured []execCapture
	sc := withTestIndex(newTestExecuteClient(t, &captured))

	action := declarative.Action{
		Operation:    declarative.OpUpdate,
		ResourceKind: declarative.KindPrincipal,
		ResourceName: "alice",
		Desired: declarative.PrincipalSpec{
			Name:    "alice",
			Type:    "user",
			IsAdmin: true,
		},
	}

	err := sc.Execute(context.Background(), action)
	require.NoError(t, err)
	require.Len(t, captured, 1)

	req := captured[0]
	assert.Equal(t, http.MethodPut, req.Method)
	assert.Contains(t, req.Path, "/principals/principal-id-alice/admin")
	assert.True(t, bodyBool(req, "is_admin"))
}

func TestExecutePrincipal_DeleteResolvesID(t *testing.T) {
	var captured []execCapture
	sc := withTestIndex(newTestExecuteClient(t, &captured))

	action := declarative.Action{
		Operation:    declarative.OpDelete,
		ResourceKind: declarative.KindPrincipal,
		ResourceName: "bob",
		Actual: declarative.PrincipalSpec{
			Name: "bob",
			Type: "user",
		},
	}

	err := sc.Execute(context.Background(), action)
	require.NoError(t, err)
	require.Len(t, captured, 1)

	req := captured[0]
	assert.Equal(t, http.MethodDelete, req.Method)
	assert.Contains(t, req.Path, "/principals/principal-id-bob")
}

// === Schema execution tests (#137) ===

func TestExecuteSchema_CreateUsesNestedPath(t *testing.T) {
	var captured []execCapture
	sc := newTestExecuteClient(t, &captured)

	action := declarative.Action{
		Operation:    declarative.OpCreate,
		ResourceKind: declarative.KindSchema,
		ResourceName: "demo.analytics",
		Desired: declarative.SchemaResource{
			CatalogName: "demo",
			SchemaName:  "analytics",
			Spec: declarative.SchemaSpec{
				Comment: "analytics schema",
			},
		},
	}

	err := sc.Execute(context.Background(), action)
	require.NoError(t, err)
	require.Len(t, captured, 1)

	req := captured[0]
	assert.Equal(t, http.MethodPost, req.Method)
	assert.Contains(t, req.Path, "/catalogs/demo/schemas")
	assert.Equal(t, "analytics", bodyStr(req, "name"))
	assert.Equal(t, "analytics schema", bodyStr(req, "comment"))

	// Schema ID should be captured in the index.
	assert.Equal(t, "generated-uuid-123", sc.index.schemaIDByPath["demo.analytics"])
}

func TestExecuteSchema_UpdateUsesNestedPath(t *testing.T) {
	var captured []execCapture
	sc := newTestExecuteClient(t, &captured)

	action := declarative.Action{
		Operation:    declarative.OpUpdate,
		ResourceKind: declarative.KindSchema,
		ResourceName: "demo.analytics",
		Desired: declarative.SchemaResource{
			CatalogName: "demo",
			SchemaName:  "analytics",
			Spec: declarative.SchemaSpec{
				Comment: "updated comment",
			},
		},
	}

	err := sc.Execute(context.Background(), action)
	require.NoError(t, err)
	require.Len(t, captured, 1)

	req := captured[0]
	assert.Equal(t, http.MethodPatch, req.Method)
	assert.Contains(t, req.Path, "/catalogs/demo/schemas/analytics")
	assert.Equal(t, "updated comment", bodyStr(req, "comment"))
}

func TestExecuteSchema_DeleteUsesNestedPath(t *testing.T) {
	var captured []execCapture
	sc := newTestExecuteClient(t, &captured)

	action := declarative.Action{
		Operation:    declarative.OpDelete,
		ResourceKind: declarative.KindSchema,
		ResourceName: "demo.analytics",
	}

	err := sc.Execute(context.Background(), action)
	require.NoError(t, err)
	require.Len(t, captured, 1)

	req := captured[0]
	assert.Equal(t, http.MethodDelete, req.Method)
	assert.Contains(t, req.Path, "/catalogs/demo/schemas/analytics")
}

// === Table execution tests (#137) ===

func TestExecuteTable_CreateUsesNestedPath(t *testing.T) {
	var captured []execCapture
	sc := newTestExecuteClient(t, &captured)

	action := declarative.Action{
		Operation:    declarative.OpCreate,
		ResourceKind: declarative.KindTable,
		ResourceName: "demo.analytics.orders",
		Desired: declarative.TableResource{
			CatalogName: "demo",
			SchemaName:  "analytics",
			TableName:   "orders",
			Spec: declarative.TableSpec{
				TableType: "MANAGED",
				Comment:   "order table",
				Columns: []declarative.ColumnDef{
					{Name: "id", Type: "INTEGER"},
					{Name: "amount", Type: "DOUBLE", Comment: "order amount"},
				},
			},
		},
	}

	err := sc.Execute(context.Background(), action)
	require.NoError(t, err)
	require.Len(t, captured, 1)

	req := captured[0]
	assert.Equal(t, http.MethodPost, req.Method)
	assert.Contains(t, req.Path, "/catalogs/demo/schemas/analytics/tables")
	assert.Equal(t, "orders", bodyStr(req, "name"))
	assert.Equal(t, "MANAGED", bodyStr(req, "table_type"))
	assert.Equal(t, "order table", bodyStr(req, "comment"))

	// Columns should be present.
	cols, ok := req.Body["columns"].([]interface{})
	require.True(t, ok, "columns should be an array")
	assert.Len(t, cols, 2)

	// Table ID should be captured in the index.
	assert.Equal(t, "generated-uuid-123", sc.index.tableIDByPath["demo.analytics.orders"])
}

func TestExecuteTable_UpdateUsesNestedPath(t *testing.T) {
	var captured []execCapture
	sc := newTestExecuteClient(t, &captured)

	action := declarative.Action{
		Operation:    declarative.OpUpdate,
		ResourceKind: declarative.KindTable,
		ResourceName: "demo.analytics.orders",
		Desired: declarative.TableResource{
			CatalogName: "demo",
			SchemaName:  "analytics",
			TableName:   "orders",
			Spec: declarative.TableSpec{
				Comment: "updated comment",
			},
		},
	}

	err := sc.Execute(context.Background(), action)
	require.NoError(t, err)
	require.Len(t, captured, 1)

	req := captured[0]
	assert.Equal(t, http.MethodPatch, req.Method)
	assert.Contains(t, req.Path, "/catalogs/demo/schemas/analytics/tables/orders")
}

func TestExecuteTable_DeleteUsesNestedPath(t *testing.T) {
	var captured []execCapture
	sc := newTestExecuteClient(t, &captured)

	action := declarative.Action{
		Operation:    declarative.OpDelete,
		ResourceKind: declarative.KindTable,
		ResourceName: "demo.analytics.orders",
	}

	err := sc.Execute(context.Background(), action)
	require.NoError(t, err)
	require.Len(t, captured, 1)

	req := captured[0]
	assert.Equal(t, http.MethodDelete, req.Method)
	assert.Contains(t, req.Path, "/catalogs/demo/schemas/analytics/tables/orders")
}

// === View execution tests (#137) ===

func TestExecuteView_CreateUsesNestedPath(t *testing.T) {
	var captured []execCapture
	sc := newTestExecuteClient(t, &captured)

	action := declarative.Action{
		Operation:    declarative.OpCreate,
		ResourceKind: declarative.KindView,
		ResourceName: "demo.analytics.order_summary",
		Desired: declarative.ViewResource{
			CatalogName: "demo",
			SchemaName:  "analytics",
			ViewName:    "order_summary",
			Spec: declarative.ViewSpec{
				ViewDefinition: "SELECT * FROM orders",
				Comment:        "summary view",
			},
		},
	}

	err := sc.Execute(context.Background(), action)
	require.NoError(t, err)
	require.Len(t, captured, 1)

	req := captured[0]
	assert.Equal(t, http.MethodPost, req.Method)
	assert.Contains(t, req.Path, "/catalogs/demo/schemas/analytics/views")
	assert.Equal(t, "order_summary", bodyStr(req, "name"))
	assert.Equal(t, "SELECT * FROM orders", bodyStr(req, "view_definition"))
	assert.Equal(t, "summary view", bodyStr(req, "comment"))
}

func TestExecuteView_UpdateUsesNestedPath(t *testing.T) {
	var captured []execCapture
	sc := newTestExecuteClient(t, &captured)

	action := declarative.Action{
		Operation:    declarative.OpUpdate,
		ResourceKind: declarative.KindView,
		ResourceName: "demo.analytics.order_summary",
		Desired: declarative.ViewResource{
			CatalogName: "demo",
			SchemaName:  "analytics",
			ViewName:    "order_summary",
			Spec: declarative.ViewSpec{
				ViewDefinition: "SELECT id, amount FROM orders",
				Comment:        "updated view",
			},
		},
	}

	err := sc.Execute(context.Background(), action)
	require.NoError(t, err)
	require.Len(t, captured, 1)

	req := captured[0]
	assert.Equal(t, http.MethodPatch, req.Method)
	assert.Contains(t, req.Path, "/catalogs/demo/schemas/analytics/views/order_summary")
	assert.Equal(t, "SELECT id, amount FROM orders", bodyStr(req, "view_definition"))
}

func TestExecuteView_DeleteUsesNestedPath(t *testing.T) {
	var captured []execCapture
	sc := newTestExecuteClient(t, &captured)

	action := declarative.Action{
		Operation:    declarative.OpDelete,
		ResourceKind: declarative.KindView,
		ResourceName: "demo.analytics.order_summary",
	}

	err := sc.Execute(context.Background(), action)
	require.NoError(t, err)
	require.Len(t, captured, 1)

	req := captured[0]
	assert.Equal(t, http.MethodDelete, req.Method)
	assert.Contains(t, req.Path, "/catalogs/demo/schemas/analytics/views/order_summary")
}

// === Unimplemented resource kind test ===

func TestExecute_UnimplementedKindReturnsError(t *testing.T) {
	var captured []execCapture
	sc := newTestExecuteClient(t, &captured)

	action := declarative.Action{
		Operation:    declarative.OpCreate,
		ResourceKind: declarative.KindVolume,
		ResourceName: "demo.analytics.stage",
	}

	err := sc.Execute(context.Background(), action)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not yet implemented")
}

func TestExecuteNotebook_CreateCreatesCells(t *testing.T) {
	var captured []execCapture
	sc := newTestExecuteClient(t, &captured)

	action := declarative.Action{
		Operation:    declarative.OpCreate,
		ResourceKind: declarative.KindNotebook,
		ResourceName: "kpi_walkthrough",
		Desired: declarative.NotebookResource{
			Name: "kpi_walkthrough",
			Spec: declarative.NotebookSpec{
				Description: "KPI notebook",
				Cells: []declarative.CellSpec{
					{Type: "markdown", Content: "# Header"},
					{Type: "sql", Content: "SELECT 1"},
				},
			},
		},
	}

	err := sc.Execute(context.Background(), action)
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(captured), 4)

	assert.Equal(t, http.MethodPost, captured[0].Method)
	assert.Contains(t, captured[0].Path, "/notebooks")
	assert.Equal(t, "kpi_walkthrough", bodyStr(captured[0], "name"))

	assert.Equal(t, http.MethodPost, captured[2].Method)
	assert.Contains(t, captured[2].Path, "/notebooks/generated-uuid-123/cells")
	assert.Equal(t, "markdown", bodyStr(captured[2], "cell_type"))

	assert.Equal(t, http.MethodPost, captured[3].Method)
	assert.Contains(t, captured[3].Path, "/notebooks/generated-uuid-123/cells")
	assert.Equal(t, "sql", bodyStr(captured[3], "cell_type"))
}

func TestExecutePipelineJob_CreateResolvesNotebookAndComputeIDs(t *testing.T) {
	var captured []execCapture
	sc := withTestIndex(newTestExecuteClient(t, &captured))

	timeout := 300
	retries := 2
	order := 1
	action := declarative.Action{
		Operation:    declarative.OpCreate,
		ResourceKind: declarative.KindPipelineJob,
		ResourceName: "pipe1/job1",
		Desired: declarative.PipelineJobSpec{
			Name:            "job1",
			Notebook:        "nb1",
			ComputeEndpoint: "local",
			DependsOn:       []string{"job0"},
			TimeoutSeconds:  &timeout,
			RetryCount:      &retries,
			Order:           &order,
		},
	}

	err := sc.Execute(context.Background(), action)
	require.NoError(t, err)
	require.Len(t, captured, 1)

	req := captured[0]
	assert.Equal(t, http.MethodPost, req.Method)
	assert.Contains(t, req.Path, "/pipelines/pipe1/jobs")
	assert.Equal(t, "notebook-id-1", bodyStr(req, "notebook_id"))
	assert.Equal(t, "compute-id-local", bodyStr(req, "compute_endpoint_id"))
	assert.Equal(t, "job1", bodyStr(req, "name"))
}

func TestExecuteMacro_CreateUpdateDelete(t *testing.T) {
	var captured []execCapture
	sc := newTestExecuteClient(t, &captured)

	create := declarative.Action{
		Operation:    declarative.OpCreate,
		ResourceKind: declarative.KindMacro,
		ResourceName: "fmt_money",
		Desired: declarative.MacroResource{
			Name: "fmt_money",
			Spec: declarative.MacroSpec{
				MacroType:   "SCALAR",
				Parameters:  []string{"amount"},
				Body:        "amount / 100.0",
				CatalogName: "main",
				ProjectName: "analytics",
				Visibility:  "catalog_global",
				Owner:       "data-team",
				Properties:  map[string]string{"team": "finance"},
				Tags:        []string{"finance"},
				Status:      "ACTIVE",
			},
		},
	}

	require.NoError(t, sc.Execute(context.Background(), create))
	require.Len(t, captured, 1)
	assert.Equal(t, http.MethodPost, captured[0].Method)
	assert.Contains(t, captured[0].Path, "/macros")
	assert.Equal(t, "fmt_money", bodyStr(captured[0], "name"))
	assert.Equal(t, "catalog_global", bodyStr(captured[0], "visibility"))

	update := create
	update.Operation = declarative.OpUpdate
	update.Desired = declarative.MacroResource{
		Name: "fmt_money",
		Spec: declarative.MacroSpec{
			Body:       "amount / 100",
			Parameters: []string{"amount"},
			Status:     "DEPRECATED",
		},
	}
	require.NoError(t, sc.Execute(context.Background(), update))
	require.Len(t, captured, 2)
	assert.Equal(t, http.MethodPatch, captured[1].Method)
	assert.Contains(t, captured[1].Path, "/macros/fmt_money")
	assert.Equal(t, "DEPRECATED", bodyStr(captured[1], "status"))

	deleteAction := declarative.Action{
		Operation:    declarative.OpDelete,
		ResourceKind: declarative.KindMacro,
		ResourceName: "fmt_money",
	}
	require.NoError(t, sc.Execute(context.Background(), deleteAction))
	require.Len(t, captured, 3)
	assert.Equal(t, http.MethodDelete, captured[2].Method)
	assert.Contains(t, captured[2].Path, "/macros/fmt_money")
}

func TestExecuteModel_CreateWithTestReconcile(t *testing.T) {
	t.Parallel()

	var (
		mu       sync.Mutex
		captured []execCapture
	)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()

		ec := execCapture{Method: r.Method, Path: r.URL.Path, Query: r.URL.Query()}
		if r.Body != nil {
			data, _ := io.ReadAll(r.Body)
			if len(data) > 0 {
				var m map[string]interface{}
				_ = json.Unmarshal(data, &m)
				ec.Body = m
			}
		}
		captured = append(captured, ec)

		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/models/analytics/stg_orders/tests":
			_, _ = w.Write([]byte(`{"data":[]}`))
		default:
			w.WriteHeader(http.StatusCreated)
			_, _ = w.Write([]byte(`{"id":"generated-uuid-123"}`))
		}
	}))
	t.Cleanup(srv.Close)

	sc := NewAPIStateClient(gen.NewClient(srv.URL, "", "test-token"))
	sc.index = newResourceIndex()

	action := declarative.Action{
		Operation:    declarative.OpCreate,
		ResourceKind: declarative.KindModel,
		ResourceName: "analytics.stg_orders",
		Desired: declarative.ModelResource{
			ProjectName: "analytics",
			ModelName:   "stg_orders",
			Spec: declarative.ModelSpec{
				Materialization: "INCREMENTAL",
				SQL:             "SELECT 1",
				Config: &declarative.ModelConfigSpec{
					UniqueKey:           []string{"order_id"},
					IncrementalStrategy: "delete+insert",
					OnSchemaChange:      "fail",
				},
				Tests: []declarative.TestSpec{{Name: "not_null_order_id", Type: "not_null", Column: "order_id"}},
			},
		},
	}

	require.NoError(t, sc.Execute(context.Background(), action))

	mu.Lock()
	defer mu.Unlock()
	require.Len(t, captured, 3)
	assert.Equal(t, http.MethodPost, captured[0].Method)
	assert.Equal(t, "/v1/models", captured[0].Path)
	config, ok := captured[0].Body["config"].(map[string]interface{})
	require.True(t, ok)
	assert.Equal(t, "delete_insert", config["incremental_strategy"])
	assert.Equal(t, "fail", config["on_schema_change"])

	assert.Equal(t, http.MethodGet, captured[1].Method)
	assert.Equal(t, "/v1/models/analytics/stg_orders/tests", captured[1].Path)

	assert.Equal(t, http.MethodPost, captured[2].Method)
	assert.Equal(t, "/v1/models/analytics/stg_orders/tests", captured[2].Path)
	assert.Equal(t, "not_null_order_id", bodyStr(captured[2], "name"))
}

func TestExecuteModel_UpdateReconcilesTests(t *testing.T) {
	t.Parallel()

	var (
		mu       sync.Mutex
		captured []execCapture
	)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()

		ec := execCapture{Method: r.Method, Path: r.URL.Path, Query: r.URL.Query()}
		if r.Body != nil {
			data, _ := io.ReadAll(r.Body)
			if len(data) > 0 {
				var m map[string]interface{}
				_ = json.Unmarshal(data, &m)
				ec.Body = m
			}
		}
		captured = append(captured, ec)

		w.Header().Set("Content-Type", "application/json")
		if r.Method == http.MethodGet && r.URL.Path == "/v1/models/analytics/stg_orders/tests" {
			_, _ = w.Write([]byte(`{"data":[{"id":"t1","name":"not_null_order_id","test_type":"not_null","column":"id"},{"id":"t2","name":"legacy_unique","test_type":"unique","column":"legacy_id"}]}`))
			return
		}
		if r.Method == http.MethodDelete {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"id":"generated-uuid-123"}`))
	}))
	t.Cleanup(srv.Close)

	sc := NewAPIStateClient(gen.NewClient(srv.URL, "", "test-token"))
	sc.index = newResourceIndex()

	action := declarative.Action{
		Operation:    declarative.OpUpdate,
		ResourceKind: declarative.KindModel,
		ResourceName: "analytics.stg_orders",
		Desired: declarative.ModelResource{
			ProjectName: "analytics",
			ModelName:   "stg_orders",
			Spec: declarative.ModelSpec{
				Materialization: "TABLE",
				SQL:             "SELECT 2",
				Tests: []declarative.TestSpec{
					{Name: "not_null_order_id", Type: "not_null", Column: "order_id"},
					{Name: "accepted_status", Type: "accepted_values", Column: "status", Values: []string{"active", "pending"}},
				},
			},
		},
	}

	require.NoError(t, sc.Execute(context.Background(), action))

	mu.Lock()
	defer mu.Unlock()
	require.GreaterOrEqual(t, len(captured), 6)
	assert.Equal(t, http.MethodPatch, captured[0].Method)
	assert.Equal(t, "/v1/models/analytics/stg_orders", captured[0].Path)
	assert.Equal(t, http.MethodGet, captured[1].Method)
	assert.Equal(t, "/v1/models/analytics/stg_orders/tests", captured[1].Path)

	paths := make([]string, 0, len(captured))
	for _, c := range captured {
		paths = append(paths, c.Method+" "+c.Path)
	}
	assert.Contains(t, paths, "DELETE /v1/models/analytics/stg_orders/tests/t1")
	assert.Contains(t, paths, "DELETE /v1/models/analytics/stg_orders/tests/t2")
	assert.Contains(t, paths, "POST /v1/models/analytics/stg_orders/tests")
}

func TestExecuteModel_SkipsTestsWhenEndpointUnavailable(t *testing.T) {
	t.Parallel()

	var (
		mu       sync.Mutex
		captured []execCapture
	)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()

		ec := execCapture{Method: r.Method, Path: r.URL.Path, Query: r.URL.Query()}
		if r.Body != nil {
			data, _ := io.ReadAll(r.Body)
			if len(data) > 0 {
				var m map[string]interface{}
				_ = json.Unmarshal(data, &m)
				ec.Body = m
			}
		}
		captured = append(captured, ec)

		w.Header().Set("Content-Type", "application/json")
		if r.Method == http.MethodGet && r.URL.Path == "/v1/models/analytics/stg_orders/tests" {
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte(`{"code":"NOT_FOUND","message":"tests endpoint disabled"}`))
			return
		}
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(`{"id":"generated-uuid-123"}`))
	}))
	t.Cleanup(srv.Close)

	sc := NewAPIStateClient(gen.NewClient(srv.URL, "", "test-token"))
	sc.index = newResourceIndex()

	action := declarative.Action{
		Operation:    declarative.OpCreate,
		ResourceKind: declarative.KindModel,
		ResourceName: "analytics.stg_orders",
		Desired: declarative.ModelResource{
			ProjectName: "analytics",
			ModelName:   "stg_orders",
			Spec: declarative.ModelSpec{
				Materialization: "INCREMENTAL",
				SQL:             "SELECT 1",
				Tests: []declarative.TestSpec{
					{Name: "not_null_order_id", Type: "not_null", Column: "order_id"},
				},
			},
		},
	}

	require.NoError(t, sc.Execute(context.Background(), action))

	mu.Lock()
	defer mu.Unlock()
	require.Len(t, captured, 2)
	assert.Equal(t, http.MethodPost, captured[0].Method)
	assert.Equal(t, "/v1/models", captured[0].Path)
	assert.Equal(t, http.MethodGet, captured[1].Method)
	assert.Equal(t, "/v1/models/analytics/stg_orders/tests", captured[1].Path)
}

func TestReadState_ModelsAndMacros(t *testing.T) {
	t.Parallel()

	mux := http.NewServeMux()
	mux.HandleFunc("/v1/models", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"data":[{"project_name":"analytics","name":"stg_orders","sql":"SELECT 1","materialization":"INCREMENTAL","description":"orders","tags":["finance"],"config":{"unique_key":["order_id"],"incremental_strategy":"merge","on_schema_change":"ignore"},"contract":{"enforce":true,"columns":[{"name":"order_id","type":"BIGINT","nullable":false}]},"freshness_policy":{"max_lag_seconds":300,"cron_schedule":"*/5 * * * *"}}]}`))
	})
	mux.HandleFunc("/v1/models/analytics/stg_orders/tests", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"data":[{"id":"t1","name":"accepted_status","test_type":"accepted_values","column":"status","config":{"values":["active","pending"]}}]}`))
	})
	mux.HandleFunc("/v1/macros", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"data":[{"name":"fmt_money","macro_type":"SCALAR","parameters":["amount"],"body":"amount/100.0","catalog_name":"main","project_name":"analytics","visibility":"project","owner":"data-team","properties":{"team":"finance"},"tags":["finance"],"status":"ACTIVE"}]}`))
	})
	mux.HandleFunc("/", emptyListHandler())

	sc := setupReadStateClient(t, mux)
	state, err := sc.ReadState(context.Background())
	require.NoError(t, err)

	require.Len(t, state.Models, 1)
	model := state.Models[0]
	assert.Equal(t, "analytics", model.ProjectName)
	assert.Equal(t, "stg_orders", model.ModelName)
	assert.Equal(t, "ignore", model.Spec.Config.OnSchemaChange)
	require.Len(t, model.Spec.Tests, 1)
	assert.Equal(t, "accepted_values", model.Spec.Tests[0].Type)

	require.Len(t, state.Macros, 1)
	macro := state.Macros[0]
	assert.Equal(t, "fmt_money", macro.Name)
	assert.Equal(t, "analytics", macro.Spec.ProjectName)
	assert.Equal(t, "project", macro.Spec.Visibility)
	assert.Equal(t, "ACTIVE", macro.Spec.Status)
}

func TestReadState_OptionalModelAndMacroEndpoints(t *testing.T) {
	t.Parallel()

	mux := http.NewServeMux()
	mux.HandleFunc("/v1/models", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte(`{"code":"NOT_FOUND","message":"models disabled"}`))
	})
	mux.HandleFunc("/v1/macros", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte(`{"code":"NOT_FOUND","message":"macros disabled"}`))
	})
	mux.HandleFunc("/", emptyListHandler())

	sc := setupReadStateClient(t, mux)
	state, err := sc.ReadState(context.Background())
	require.NoError(t, err)
	assert.Empty(t, state.Models)
	assert.Empty(t, state.Macros)
	assert.Len(t, sc.OptionalReadWarnings(), 2)
}

func TestReadState_ConnectionErrorsStrictModeAreNotOptional(t *testing.T) {
	t.Parallel()

	sc := NewAPIStateClientWithOptions(gen.NewClient("http://127.0.0.1:1", "", "test-token"), APIStateClientOptions{
		CompatibilityMode: CapabilityCompatibilityStrict,
	})

	_, err := sc.ReadState(context.Background())
	require.Error(t, err)
}

func TestReadState_ConnectionErrorsLegacyModeAreOptionalForModelMacro(t *testing.T) {
	t.Parallel()

	mux := http.NewServeMux()
	mux.HandleFunc("/v1/principals", emptyListHandler())
	mux.HandleFunc("/v1/groups", emptyListHandler())
	mux.HandleFunc("/v1/api-keys", emptyListHandler())
	mux.HandleFunc("/v1/catalogs", emptyListHandler())
	mux.HandleFunc("/v1/storage-credentials", emptyListHandler())
	mux.HandleFunc("/v1/external-locations", emptyListHandler())
	mux.HandleFunc("/v1/grants", emptyListHandler())
	mux.HandleFunc("/v1/compute-endpoints", emptyListHandler())
	mux.HandleFunc("/v1/tags", emptyListHandler())
	mux.HandleFunc("/v1/notebooks", emptyListHandler())
	mux.HandleFunc("/v1/pipelines", emptyListHandler())
	mux.HandleFunc("/v1/models", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(`eof`))
	})
	mux.HandleFunc("/v1/macros", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(`broken pipe`))
	})

	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	sc := NewAPIStateClientWithOptions(gen.NewClient(srv.URL, "", "test-token"), APIStateClientOptions{
		CompatibilityMode: CapabilityCompatibilityLegacy,
	})

	state, err := sc.ReadState(context.Background())
	require.NoError(t, err)
	assert.Empty(t, state.Models)
	assert.Empty(t, state.Macros)
	assert.Len(t, sc.OptionalReadWarnings(), 2)
}

func TestValidateApplyCapabilities_ModelEndpointRequired(t *testing.T) {
	t.Parallel()

	mux := http.NewServeMux()
	mux.HandleFunc("/v1/models", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte(`{"code":"NOT_FOUND"}`))
	})
	mux.HandleFunc("/v1/macros", emptyListHandler())
	mux.HandleFunc("/", emptyListHandler())

	sc := setupReadStateClient(t, mux)
	err := sc.ValidateApplyCapabilities(context.Background(), []declarative.Action{{
		Operation:    declarative.OpCreate,
		ResourceKind: declarative.KindModel,
		ResourceName: "analytics.stg_orders",
	}})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "/models endpoint is unavailable")
}

// === ReadState helper ===

// setupReadStateClient creates an APIStateClient backed by a test server with the given mux.
func setupReadStateClient(t *testing.T, handler http.Handler) *APIStateClient {
	t.Helper()
	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)
	client := gen.NewClient(srv.URL, "", "test-token")
	return NewAPIStateClient(client)
}

// emptyListHandler returns an HTTP handler that always responds with an empty paginated list.
func emptyListHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"data":[]}`))
	}
}

// === ReadState tests ===

func TestReadState_EmptyState(t *testing.T) {
	t.Parallel()

	mux := http.NewServeMux()
	// All endpoints return empty arrays.
	mux.HandleFunc("/", emptyListHandler())

	sc := setupReadStateClient(t, mux)
	state, err := sc.ReadState(context.Background())

	require.NoError(t, err)
	require.NotNil(t, state)
	assert.Empty(t, state.Principals)
	assert.Empty(t, state.Groups)
	assert.Empty(t, state.Catalogs)
	assert.Empty(t, state.Schemas)
	assert.Empty(t, state.Tags)
	assert.Empty(t, state.StorageCredentials)
	assert.Empty(t, state.ExternalLocations)
	assert.Empty(t, state.ComputeEndpoints)
	assert.Empty(t, state.Notebooks)
	assert.Empty(t, state.Pipelines)
	assert.Empty(t, state.APIKeys)
}

func TestReadState_Principals(t *testing.T) {
	t.Parallel()

	mux := http.NewServeMux()
	mux.HandleFunc("/v1/principals", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		resp := map[string]interface{}{
			"data": []map[string]interface{}{
				{"id": "p-1", "name": "alice", "type": "user", "is_admin": false},
				{"id": "p-2", "name": "bob", "type": "user", "is_admin": true},
				{"id": "p-3", "name": "svc1", "type": "service_principal", "is_admin": false},
			},
		}
		_ = json.NewEncoder(w).Encode(resp)
	})
	// Other endpoints return empty.
	mux.HandleFunc("/", emptyListHandler())

	sc := setupReadStateClient(t, mux)
	state, err := sc.ReadState(context.Background())

	require.NoError(t, err)
	require.Len(t, state.Principals, 3)

	assert.Equal(t, "alice", state.Principals[0].Name)
	assert.Equal(t, "user", state.Principals[0].Type)
	assert.False(t, state.Principals[0].IsAdmin)

	assert.Equal(t, "bob", state.Principals[1].Name)
	assert.True(t, state.Principals[1].IsAdmin)

	assert.Equal(t, "svc1", state.Principals[2].Name)
	assert.Equal(t, "service_principal", state.Principals[2].Type)

	// Verify resource index was populated.
	assert.Equal(t, "p-1", sc.index.principalIDByName["alice"])
	assert.Equal(t, "p-2", sc.index.principalIDByName["bob"])
	assert.Equal(t, "p-3", sc.index.principalIDByName["svc1"])
}

func TestReadState_GroupsWithMembers(t *testing.T) {
	t.Parallel()

	mux := http.NewServeMux()
	// Principals endpoint provides names for reverse-lookup of group members.
	mux.HandleFunc("/v1/principals", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		resp := map[string]interface{}{
			"data": []map[string]interface{}{
				{"id": "p-alice", "name": "alice", "type": "user", "is_admin": false},
				{"id": "p-bob", "name": "bob", "type": "user", "is_admin": false},
				{"id": "p-charlie", "name": "charlie", "type": "user", "is_admin": false},
			},
		}
		_ = json.NewEncoder(w).Encode(resp)
	})
	mux.HandleFunc("/v1/groups", func(w http.ResponseWriter, r *http.Request) {
		// Only respond to the top-level groups list, not /groups/{id}/members.
		if r.URL.Path != "/v1/groups" {
			emptyListHandler()(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		resp := map[string]interface{}{
			"data": []map[string]interface{}{
				{"id": "g-1", "name": "admins", "description": "Administrators"},
				{"id": "g-2", "name": "analysts", "description": "Data analysts"},
			},
		}
		_ = json.NewEncoder(w).Encode(resp)
	})
	mux.HandleFunc("/v1/groups/g-1/members", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		resp := map[string]interface{}{
			"data": []map[string]interface{}{
				{"member_id": "p-alice", "member_type": "user"},
				{"member_id": "p-bob", "member_type": "user"},
			},
		}
		_ = json.NewEncoder(w).Encode(resp)
	})
	mux.HandleFunc("/v1/groups/g-2/members", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		resp := map[string]interface{}{
			"data": []map[string]interface{}{
				{"member_id": "p-charlie", "member_type": "user"},
			},
		}
		_ = json.NewEncoder(w).Encode(resp)
	})
	// Other endpoints return empty.
	mux.HandleFunc("/", emptyListHandler())

	sc := setupReadStateClient(t, mux)
	state, err := sc.ReadState(context.Background())

	require.NoError(t, err)
	require.Len(t, state.Groups, 2)

	// First group: admins with 2 members.
	assert.Equal(t, "admins", state.Groups[0].Name)
	assert.Equal(t, "Administrators", state.Groups[0].Description)
	require.Len(t, state.Groups[0].Members, 2)
	assert.Equal(t, "alice", state.Groups[0].Members[0].Name)
	assert.Equal(t, "user", state.Groups[0].Members[0].Type)
	assert.Equal(t, "p-alice", state.Groups[0].Members[0].MemberID)
	assert.Equal(t, "bob", state.Groups[0].Members[1].Name)

	// Second group: analysts with 1 member.
	assert.Equal(t, "analysts", state.Groups[1].Name)
	require.Len(t, state.Groups[1].Members, 1)
	assert.Equal(t, "charlie", state.Groups[1].Members[0].Name)
	assert.Equal(t, "p-charlie", state.Groups[1].Members[0].MemberID)

	// Verify resource index.
	assert.Equal(t, "g-1", sc.index.groupIDByName["admins"])
	assert.Equal(t, "g-2", sc.index.groupIDByName["analysts"])
}

func TestReadState_Pagination(t *testing.T) {
	t.Parallel()

	var mu sync.Mutex
	requestCount := 0

	mux := http.NewServeMux()
	mux.HandleFunc("/v1/principals", func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		requestCount++
		mu.Unlock()

		w.Header().Set("Content-Type", "application/json")
		pageToken := r.URL.Query().Get("page_token")

		switch pageToken {
		case "":
			// First page.
			resp := map[string]interface{}{
				"data": []map[string]interface{}{
					{"id": "p-1", "name": "alice", "type": "user", "is_admin": false},
					{"id": "p-2", "name": "bob", "type": "user", "is_admin": false},
				},
				"next_page_token": "page2",
			}
			_ = json.NewEncoder(w).Encode(resp)
		case "page2":
			// Second page.
			resp := map[string]interface{}{
				"data": []map[string]interface{}{
					{"id": "p-3", "name": "charlie", "type": "user", "is_admin": true},
				},
			}
			_ = json.NewEncoder(w).Encode(resp)
		}
	})
	mux.HandleFunc("/", emptyListHandler())

	sc := setupReadStateClient(t, mux)
	state, err := sc.ReadState(context.Background())

	require.NoError(t, err)
	require.Len(t, state.Principals, 3)
	assert.Equal(t, "alice", state.Principals[0].Name)
	assert.Equal(t, "bob", state.Principals[1].Name)
	assert.Equal(t, "charlie", state.Principals[2].Name)
	assert.True(t, state.Principals[2].IsAdmin)

	// Verify two pages were fetched.
	mu.Lock()
	assert.Equal(t, 2, requestCount)
	mu.Unlock()
}

func TestReadState_APIError4xx(t *testing.T) {
	t.Parallel()

	// fetchAllPages returns errors for non-2xx responses.
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/principals", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusForbidden)
		_, _ = w.Write([]byte(`{"code":"FORBIDDEN","message":"access denied"}`))
	})
	mux.HandleFunc("/", emptyListHandler())

	sc := setupReadStateClient(t, mux)
	_, err := sc.ReadState(context.Background())

	require.Error(t, err)
	assert.Contains(t, err.Error(), "HTTP 403")
}

func TestReadState_ServerErrorReturnsError(t *testing.T) {
	t.Parallel()

	mux := http.NewServeMux()
	mux.HandleFunc("/v1/principals", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(`{"message":"internal error"}`))
	})
	mux.HandleFunc("/", emptyListHandler())

	sc := setupReadStateClient(t, mux)
	_, err := sc.ReadState(context.Background())

	require.Error(t, err)
	assert.Contains(t, err.Error(), "HTTP 500")
}

func TestReadState_CatalogsWithSchemasAndTables(t *testing.T) {
	t.Parallel()

	mux := http.NewServeMux()
	mux.HandleFunc("/v1/catalogs", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		resp := map[string]interface{}{
			"data": []map[string]interface{}{
				{
					"id":             "cat-1",
					"name":           "demo",
					"metastore_type": "sqlite",
					"dsn":            "/tmp/meta.sqlite",
					"data_path":      "/tmp/data",
					"is_default":     true,
					"comment":        "Demo catalog",
				},
			},
		}
		_ = json.NewEncoder(w).Encode(resp)
	})
	mux.HandleFunc("/v1/catalogs/demo/schemas", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		resp := map[string]interface{}{
			"data": []map[string]interface{}{
				{
					"id":      "sch-1",
					"name":    "analytics",
					"comment": "Analytics schema",
					"owner":   "alice",
				},
			},
		}
		_ = json.NewEncoder(w).Encode(resp)
	})
	mux.HandleFunc("/v1/catalogs/demo/schemas/analytics/tables", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		resp := map[string]interface{}{
			"data": []map[string]interface{}{
				{
					"id":         "tbl-1",
					"name":       "orders",
					"table_type": "MANAGED",
					"comment":    "Order data",
					"owner":      "alice",
					"columns": []map[string]interface{}{
						{"name": "id", "type": "INTEGER", "comment": "PK"},
						{"name": "amount", "type": "DOUBLE", "comment": ""},
					},
				},
			},
		}
		_ = json.NewEncoder(w).Encode(resp)
	})
	mux.HandleFunc("/", emptyListHandler())

	sc := setupReadStateClient(t, mux)
	state, err := sc.ReadState(context.Background())

	require.NoError(t, err)

	// Catalogs
	require.Len(t, state.Catalogs, 1)
	assert.Equal(t, "demo", state.Catalogs[0].CatalogName)
	assert.Equal(t, "sqlite", state.Catalogs[0].Spec.MetastoreType)
	assert.Equal(t, "/tmp/meta.sqlite", state.Catalogs[0].Spec.DSN)
	assert.True(t, state.Catalogs[0].Spec.IsDefault)
	assert.Equal(t, "Demo catalog", state.Catalogs[0].Spec.Comment)
	assert.Equal(t, "cat-1", sc.index.catalogIDByName["demo"])

	// Schemas
	require.Len(t, state.Schemas, 1)
	assert.Equal(t, "demo", state.Schemas[0].CatalogName)
	assert.Equal(t, "analytics", state.Schemas[0].SchemaName)
	assert.Equal(t, "Analytics schema", state.Schemas[0].Spec.Comment)
	assert.Equal(t, "alice", state.Schemas[0].Spec.Owner)
	assert.Equal(t, "sch-1", sc.index.schemaIDByPath["demo.analytics"])

	// Tables
	require.Len(t, state.Tables, 1)
	assert.Equal(t, "demo", state.Tables[0].CatalogName)
	assert.Equal(t, "analytics", state.Tables[0].SchemaName)
	assert.Equal(t, "orders", state.Tables[0].TableName)
	assert.Equal(t, "MANAGED", state.Tables[0].Spec.TableType)
	require.Len(t, state.Tables[0].Spec.Columns, 2)
	assert.Equal(t, "id", state.Tables[0].Spec.Columns[0].Name)
	assert.Equal(t, "INTEGER", state.Tables[0].Spec.Columns[0].Type)
	assert.Equal(t, "PK", state.Tables[0].Spec.Columns[0].Comment)
	assert.Equal(t, "tbl-1", sc.index.tableIDByPath["demo.analytics.orders"])
}

func TestReadState_Tags(t *testing.T) {
	t.Parallel()

	mux := http.NewServeMux()
	piiValue := "email"
	mux.HandleFunc("/v1/tags", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		resp := map[string]interface{}{
			"data": []map[string]interface{}{
				{"id": "tag-1", "key": "pii", "value": nil},
				{"id": "tag-2", "key": "pii", "value": piiValue},
			},
		}
		_ = json.NewEncoder(w).Encode(resp)
	})
	mux.HandleFunc("/", emptyListHandler())

	sc := setupReadStateClient(t, mux)
	state, err := sc.ReadState(context.Background())

	require.NoError(t, err)
	require.Len(t, state.Tags, 2)
	assert.Equal(t, "pii", state.Tags[0].Key)
	assert.Nil(t, state.Tags[0].Value)
	assert.Equal(t, "pii", state.Tags[1].Key)
	require.NotNil(t, state.Tags[1].Value)
	assert.Equal(t, "email", *state.Tags[1].Value)

	assert.Equal(t, "tag-1", sc.index.tagIDByKey["pii"])
	assert.Equal(t, "tag-2", sc.index.tagIDByKey["pii:email"])
}

func TestReadState_StorageCredentials(t *testing.T) {
	t.Parallel()

	mux := http.NewServeMux()
	mux.HandleFunc("/v1/storage-credentials", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		resp := map[string]interface{}{
			"data": []map[string]interface{}{
				{"name": "aws-creds", "credential_type": "S3", "comment": "AWS access"},
			},
		}
		_ = json.NewEncoder(w).Encode(resp)
	})
	mux.HandleFunc("/", emptyListHandler())

	sc := setupReadStateClient(t, mux)
	state, err := sc.ReadState(context.Background())

	require.NoError(t, err)
	require.Len(t, state.StorageCredentials, 1)
	assert.Equal(t, "aws-creds", state.StorageCredentials[0].Name)
	assert.Equal(t, "S3", state.StorageCredentials[0].CredentialType)
	assert.Equal(t, "AWS access", state.StorageCredentials[0].Comment)
}

func TestReadState_ExternalLocations(t *testing.T) {
	t.Parallel()

	mux := http.NewServeMux()
	mux.HandleFunc("/v1/external-locations", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		resp := map[string]interface{}{
			"data": []map[string]interface{}{
				{
					"name":            "s3-data",
					"url":             "s3://my-bucket/data",
					"credential_name": "aws-creds",
					"storage_type":    "S3",
					"comment":         "External S3",
					"read_only":       true,
				},
			},
		}
		_ = json.NewEncoder(w).Encode(resp)
	})
	mux.HandleFunc("/", emptyListHandler())

	sc := setupReadStateClient(t, mux)
	state, err := sc.ReadState(context.Background())

	require.NoError(t, err)
	require.Len(t, state.ExternalLocations, 1)
	assert.Equal(t, "s3-data", state.ExternalLocations[0].Name)
	assert.Equal(t, "s3://my-bucket/data", state.ExternalLocations[0].URL)
	assert.Equal(t, "aws-creds", state.ExternalLocations[0].CredentialName)
	assert.True(t, state.ExternalLocations[0].ReadOnly)
}

func TestReadState_ComputeEndpointsWithAssignments(t *testing.T) {
	t.Parallel()

	mux := http.NewServeMux()
	mux.HandleFunc("/v1/compute-endpoints", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/compute-endpoints" {
			emptyListHandler()(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		resp := map[string]interface{}{
			"data": []map[string]interface{}{
				{"name": "local", "url": "", "type": "LOCAL", "size": "small"},
			},
		}
		_ = json.NewEncoder(w).Encode(resp)
	})
	mux.HandleFunc("/v1/compute-endpoints/local/assignments", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		resp := map[string]interface{}{
			"data": []map[string]interface{}{
				{
					"endpoint":       "local",
					"principal":      "alice",
					"principal_type": "user",
					"is_default":     true,
					"fallback_local": false,
				},
			},
		}
		_ = json.NewEncoder(w).Encode(resp)
	})
	mux.HandleFunc("/", emptyListHandler())

	sc := setupReadStateClient(t, mux)
	state, err := sc.ReadState(context.Background())

	require.NoError(t, err)
	require.Len(t, state.ComputeEndpoints, 1)
	assert.Equal(t, "local", state.ComputeEndpoints[0].Name)
	assert.Equal(t, "LOCAL", state.ComputeEndpoints[0].Type)

	require.Len(t, state.ComputeAssignments, 1)
	assert.Equal(t, "local", state.ComputeAssignments[0].Endpoint)
	assert.Equal(t, "alice", state.ComputeAssignments[0].Principal)
	assert.True(t, state.ComputeAssignments[0].IsDefault)
}

func TestReadState_NotebooksAndPipelines(t *testing.T) {
	t.Parallel()

	mux := http.NewServeMux()
	mux.HandleFunc("/v1/notebooks", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		resp := map[string]interface{}{
			"data": []map[string]interface{}{
				{"id": "nb-id-1", "name": "nb1", "description": "My notebook", "owner": "alice"},
			},
		}
		_ = json.NewEncoder(w).Encode(resp)
	})
	mux.HandleFunc("/v1/notebooks/nb-id-1", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		resp := map[string]interface{}{
			"notebook": map[string]interface{}{
				"id":          "nb-id-1",
				"name":        "nb1",
				"description": "My notebook",
				"owner":       "alice",
			},
			"cells": []map[string]interface{}{
				{"id": "cell-1", "cell_type": "markdown", "content": "# Intro", "position": 0},
				{"id": "cell-2", "cell_type": "sql", "content": "SELECT 1", "position": 1},
			},
		}
		_ = json.NewEncoder(w).Encode(resp)
	})
	mux.HandleFunc("/v1/pipelines", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		resp := map[string]interface{}{
			"data": []map[string]interface{}{
				{
					"id":                "pipe-id-1",
					"name":              "pipe1",
					"description":       "ETL pipeline",
					"schedule_cron":     "0 0 * * *",
					"is_paused":         true,
					"concurrency_limit": 1,
				},
			},
		}
		_ = json.NewEncoder(w).Encode(resp)
	})
	mux.HandleFunc("/v1/pipelines/pipe1/jobs", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		resp := map[string]interface{}{
			"data": []map[string]interface{}{
				{
					"id":                  "job-id-1",
					"name":                "daily-kpi",
					"notebook_id":         "nb-id-1",
					"compute_endpoint_id": "",
					"depends_on":          []string{},
					"timeout_seconds":     300,
					"retry_count":         1,
					"job_order":           0,
				},
			},
		}
		_ = json.NewEncoder(w).Encode(resp)
	})
	mux.HandleFunc("/", emptyListHandler())

	sc := setupReadStateClient(t, mux)
	state, err := sc.ReadState(context.Background())

	require.NoError(t, err)

	require.Len(t, state.Notebooks, 1)
	assert.Equal(t, "nb1", state.Notebooks[0].Name)
	assert.Equal(t, "My notebook", state.Notebooks[0].Spec.Description)
	assert.Equal(t, "alice", state.Notebooks[0].Spec.Owner)
	require.Len(t, state.Notebooks[0].Spec.Cells, 2)
	assert.Equal(t, "markdown", state.Notebooks[0].Spec.Cells[0].Type)
	assert.Equal(t, "SELECT 1", state.Notebooks[0].Spec.Cells[1].Content)

	require.Len(t, state.Pipelines, 1)
	assert.Equal(t, "pipe1", state.Pipelines[0].Name)
	assert.Equal(t, "ETL pipeline", state.Pipelines[0].Spec.Description)
	assert.Equal(t, "0 0 * * *", state.Pipelines[0].Spec.ScheduleCron)
	assert.True(t, state.Pipelines[0].Spec.IsPaused)
	require.NotNil(t, state.Pipelines[0].Spec.ConcurrencyLimit)
	assert.Equal(t, 1, *state.Pipelines[0].Spec.ConcurrencyLimit)
	require.Len(t, state.Pipelines[0].Spec.Jobs, 1)
	assert.Equal(t, "daily-kpi", state.Pipelines[0].Spec.Jobs[0].Name)
	assert.Equal(t, "nb1", state.Pipelines[0].Spec.Jobs[0].Notebook)
}

func TestReadState_APIKeys(t *testing.T) {
	t.Parallel()

	mux := http.NewServeMux()
	mux.HandleFunc("/v1/api-keys", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		resp := map[string]interface{}{
			"data": []map[string]interface{}{
				{"id": "k-1", "name": "key1", "principal_id": "p-1", "expires_at": "2026-12-31T00:00:00Z"},
			},
		}
		_ = json.NewEncoder(w).Encode(resp)
	})
	mux.HandleFunc("/v1/principals", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		resp := map[string]interface{}{
			"data": []map[string]interface{}{
				{"id": "p-1", "name": "alice", "type": "user", "is_admin": false},
			},
		}
		_ = json.NewEncoder(w).Encode(resp)
	})
	mux.HandleFunc("/", emptyListHandler())

	sc := setupReadStateClient(t, mux)
	state, err := sc.ReadState(context.Background())

	require.NoError(t, err)
	require.Len(t, state.APIKeys, 1)
	assert.Equal(t, "key1", state.APIKeys[0].Name)
	assert.Equal(t, "alice", state.APIKeys[0].Principal)
	require.NotNil(t, state.APIKeys[0].ExpiresAt)
	assert.Equal(t, "2026-12-31T00:00:00Z", *state.APIKeys[0].ExpiresAt)
}

func TestExecuteAPIKey_Delete(t *testing.T) {
	t.Parallel()

	var captured []execCapture
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ec := execCapture{Method: r.Method, Path: r.URL.Path, Query: r.URL.Query()}
		captured = append(captured, ec)
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/api-keys":
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"data":[{"id":"key-id-1","name":"local-dev","principal":"alice"}]}`))
		default:
			w.WriteHeader(http.StatusNoContent)
		}
	}))
	t.Cleanup(srv.Close)

	client := gen.NewClient(srv.URL, "", "test-token")
	sc := NewAPIStateClient(client)
	sc.index = newResourceIndex()
	sc = withTestIndex(sc)

	action := declarative.Action{
		Operation:    declarative.OpDelete,
		ResourceKind: declarative.KindAPIKey,
		ResourceName: "local-dev",
		Actual: declarative.APIKeySpec{
			Name:      "local-dev",
			Principal: "alice",
		},
	}

	err := sc.Execute(context.Background(), action)
	require.NoError(t, err)
	require.Len(t, captured, 2)
	assert.Equal(t, http.MethodGet, captured[0].Method)
	assert.Equal(t, "/v1/api-keys", captured[0].Path)
	assert.Equal(t, http.MethodDelete, captured[1].Method)
	assert.Equal(t, "/v1/api-keys/key-id-1", captured[1].Path)
}

func TestExecuteAPIKey_Create(t *testing.T) {
	t.Parallel()

	var captured []execCapture
	sc := withTestIndex(newTestExecuteClient(t, &captured))

	expiry := "2027-01-01T00:00:00Z"
	action := declarative.Action{
		Operation:    declarative.OpCreate,
		ResourceKind: declarative.KindAPIKey,
		ResourceName: "local-dev",
		Desired: declarative.APIKeySpec{
			Name:      "local-dev",
			Principal: "alice",
			ExpiresAt: &expiry,
		},
	}

	err := sc.Execute(context.Background(), action)
	require.NoError(t, err)
	require.Len(t, captured, 1)
	assert.Equal(t, http.MethodPost, captured[0].Method)
	assert.Equal(t, "/v1/api-keys", captured[0].Path)
	assert.Equal(t, "principal-id-alice", bodyStr(captured[0], "principal_id"))
	assert.Equal(t, "local-dev", bodyStr(captured[0], "name"))
	assert.Equal(t, expiry, bodyStr(captured[0], "expires_at"))
}

func TestValidateNoSelfAPIKeyDeletion_BlocksDelete(t *testing.T) {
	t.Parallel()

	mux := http.NewServeMux()
	mux.HandleFunc("/v1/api-keys", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"data":[{"id":"key-id-1","name":"local-dev","principal":"alice","key_prefix":"duckflix"}]}`))
	})

	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	client := gen.NewClient(srv.URL, "duckflix-secret-value", "")
	sc := NewAPIStateClient(client)
	sc.index = newResourceIndex()

	err := sc.ValidateNoSelfAPIKeyDeletion(context.Background(), []declarative.Action{
		{
			Operation:    declarative.OpDelete,
			ResourceKind: declarative.KindAPIKey,
			ResourceName: "local-dev",
			Actual: declarative.APIKeySpec{
				Name:      "local-dev",
				Principal: "alice",
			},
		},
	})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "currently-authenticated API key")
}

func TestValidateNoSelfAPIKeyDeletion_AllowsDeleteWhenUsingToken(t *testing.T) {
	t.Parallel()

	sc := withTestIndex(newTestExecuteClient(t, &[]execCapture{}))

	err := sc.ValidateNoSelfAPIKeyDeletion(context.Background(), []declarative.Action{
		{
			Operation:    declarative.OpDelete,
			ResourceKind: declarative.KindAPIKey,
			ResourceName: "local-dev",
			Actual: declarative.APIKeySpec{
				Name:      "local-dev",
				Principal: "alice",
			},
		},
	})

	require.NoError(t, err)
}

func TestReadState_GrantsResolvedFromIDs(t *testing.T) {
	t.Parallel()

	mux := http.NewServeMux()
	mux.HandleFunc("/v1/principals", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		resp := map[string]interface{}{
			"data": []map[string]interface{}{
				{"id": "p-1", "name": "alice", "type": "user", "is_admin": false},
			},
		}
		_ = json.NewEncoder(w).Encode(resp)
	})
	mux.HandleFunc("/v1/catalogs", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		resp := map[string]interface{}{
			"data": []map[string]interface{}{
				{"id": "cat-1", "name": "demo", "metastore_type": "sqlite", "dsn": ":memory:", "data_path": "/tmp"},
			},
		}
		_ = json.NewEncoder(w).Encode(resp)
	})
	mux.HandleFunc("/v1/catalogs/demo/schemas", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		resp := map[string]interface{}{
			"data": []map[string]interface{}{
				{"id": "sch-1", "name": "analytics"},
			},
		}
		_ = json.NewEncoder(w).Encode(resp)
	})
	mux.HandleFunc("/v1/catalogs/demo/schemas/analytics/tables", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		resp := map[string]interface{}{
			"data": []map[string]interface{}{
				{"id": "tbl-1", "name": "orders", "table_type": "MANAGED"},
			},
		}
		_ = json.NewEncoder(w).Encode(resp)
	})
	mux.HandleFunc("/v1/catalogs/demo/schemas/analytics/views", emptyListHandler())
	mux.HandleFunc("/v1/catalogs/demo/schemas/analytics/volumes", emptyListHandler())
	mux.HandleFunc("/v1/grants", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		resp := map[string]interface{}{
			"data": []map[string]interface{}{
				{
					"principal_id":   "p-1",
					"principal_type": "user",
					"securable_type": "table",
					"securable_id":   "tbl-1",
					"privilege":      "SELECT",
				},
			},
		}
		_ = json.NewEncoder(w).Encode(resp)
	})
	mux.HandleFunc("/", emptyListHandler())

	sc := setupReadStateClient(t, mux)
	state, err := sc.ReadState(context.Background())

	require.NoError(t, err)
	require.Len(t, state.Grants, 1)
	assert.Equal(t, "alice", state.Grants[0].Principal)
	assert.Equal(t, "user", state.Grants[0].PrincipalType)
	assert.Equal(t, "table", state.Grants[0].SecurableType)
	assert.Equal(t, "demo.analytics.orders", state.Grants[0].Securable)
	assert.Equal(t, "SELECT", state.Grants[0].Privilege)
}

func TestReadState_GrantsUnresolvedSecurableIsSkipped(t *testing.T) {
	t.Parallel()

	mux := http.NewServeMux()
	mux.HandleFunc("/v1/principals", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"data": []map[string]interface{}{{"id": "p-1", "name": "alice", "type": "user"}},
		})
	})
	mux.HandleFunc("/v1/grants", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"data": []map[string]interface{}{{
				"principal_id":   "p-1",
				"principal_type": "user",
				"securable_type": "table",
				"securable_id":   "tbl-missing",
				"privilege":      "SELECT",
			}},
		})
	})
	mux.HandleFunc("/", emptyListHandler())

	sc := setupReadStateClient(t, mux)
	state, err := sc.ReadState(context.Background())
	require.NoError(t, err)
	assert.Empty(t, state.Grants)
}

func TestReadState_GrantsUnresolvedPrincipalIsSkipped(t *testing.T) {
	t.Parallel()

	mux := http.NewServeMux()
	mux.HandleFunc("/v1/grants", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"data": []map[string]interface{}{{
				"principal_id":   "p-missing",
				"principal_type": "user",
				"securable_type": "catalog",
				"securable_id":   "cat-1",
				"privilege":      "USE_CATALOG",
			}},
		})
	})
	mux.HandleFunc("/v1/principals/p-missing", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte(`{"code":"NOT_FOUND","message":"not found"}`))
	})
	mux.HandleFunc("/", emptyListHandler())

	sc := setupReadStateClient(t, mux)
	state, err := sc.ReadState(context.Background())
	require.NoError(t, err)
	assert.Empty(t, state.Grants)
}

// === Additional Execute tests ===

func TestExecutePrincipal_Create(t *testing.T) {
	t.Parallel()

	var captured []execCapture
	sc := newTestExecuteClient(t, &captured)

	action := declarative.Action{
		Operation:    declarative.OpCreate,
		ResourceKind: declarative.KindPrincipal,
		ResourceName: "alice",
		Desired: declarative.PrincipalSpec{
			Name:    "alice",
			Type:    "user",
			IsAdmin: true,
		},
	}

	err := sc.Execute(context.Background(), action)
	require.NoError(t, err)
	require.Len(t, captured, 1)

	req := captured[0]
	assert.Equal(t, http.MethodPost, req.Method)
	assert.Contains(t, req.Path, "/principals")
	assert.Equal(t, "alice", bodyStr(req, "name"))
	assert.Equal(t, "user", bodyStr(req, "type"))
	assert.True(t, bodyBool(req, "is_admin"))

	// ID should be stored in index.
	assert.Equal(t, "generated-uuid-123", sc.index.principalIDByName["alice"])
}

func TestExecutePrincipal_Delete(t *testing.T) {
	t.Parallel()

	var captured []execCapture
	sc := withTestIndex(newTestExecuteClient(t, &captured))

	action := declarative.Action{
		Operation:    declarative.OpDelete,
		ResourceKind: declarative.KindPrincipal,
		ResourceName: "alice",
		Actual: declarative.PrincipalSpec{
			Name: "alice",
			Type: "user",
		},
	}

	err := sc.Execute(context.Background(), action)
	require.NoError(t, err)
	require.Len(t, captured, 1)

	req := captured[0]
	assert.Equal(t, http.MethodDelete, req.Method)
	assert.Contains(t, req.Path, "/principals/principal-id-alice")
}

func TestExecutePrincipal_Update(t *testing.T) {
	t.Parallel()

	var captured []execCapture
	sc := withTestIndex(newTestExecuteClient(t, &captured))

	action := declarative.Action{
		Operation:    declarative.OpUpdate,
		ResourceKind: declarative.KindPrincipal,
		ResourceName: "alice",
		Desired: declarative.PrincipalSpec{
			Name:    "alice",
			Type:    "user",
			IsAdmin: true,
		},
	}

	err := sc.Execute(context.Background(), action)
	require.NoError(t, err)
	require.Len(t, captured, 1)

	req := captured[0]
	assert.Equal(t, http.MethodPut, req.Method)
	assert.Contains(t, req.Path, "/principals/principal-id-alice/admin")
	assert.True(t, bodyBool(req, "is_admin"))
}

func TestExecuteSchema_Create(t *testing.T) {
	t.Parallel()

	var captured []execCapture
	sc := newTestExecuteClient(t, &captured)

	action := declarative.Action{
		Operation:    declarative.OpCreate,
		ResourceKind: declarative.KindSchema,
		ResourceName: "demo.analytics",
		Desired: declarative.SchemaResource{
			CatalogName: "demo",
			SchemaName:  "analytics",
			Spec: declarative.SchemaSpec{
				Comment: "Analytics schema",
				Owner:   "alice",
			},
		},
	}

	err := sc.Execute(context.Background(), action)
	require.NoError(t, err)
	require.Len(t, captured, 1)

	req := captured[0]
	assert.Equal(t, http.MethodPost, req.Method)
	assert.Contains(t, req.Path, "/schemas")
}

func TestExecuteSchema_Delete(t *testing.T) {
	t.Parallel()

	var captured []execCapture
	sc := newTestExecuteClient(t, &captured)

	action := declarative.Action{
		Operation:    declarative.OpDelete,
		ResourceKind: declarative.KindSchema,
		ResourceName: "demo.analytics",
	}

	err := sc.Execute(context.Background(), action)
	require.NoError(t, err)
	require.Len(t, captured, 1)

	req := captured[0]
	assert.Equal(t, http.MethodDelete, req.Method)
	assert.Contains(t, req.Path, "/catalogs/demo/schemas/analytics")
}

func TestExecuteCatalog_Delete(t *testing.T) {
	t.Parallel()

	var captured []execCapture
	sc := newTestExecuteClient(t, &captured)

	action := declarative.Action{
		Operation:    declarative.OpDelete,
		ResourceKind: declarative.KindCatalogRegistration,
		ResourceName: "demo",
	}

	err := sc.Execute(context.Background(), action)
	require.NoError(t, err)
	require.Len(t, captured, 1)

	req := captured[0]
	assert.Equal(t, http.MethodDelete, req.Method)
	assert.Contains(t, req.Path, "/catalogs/demo")
}

func TestExecuteGroup_Delete(t *testing.T) {
	t.Parallel()

	var captured []execCapture
	sc := newTestExecuteClient(t, &captured)

	action := declarative.Action{
		Operation:    declarative.OpDelete,
		ResourceKind: declarative.KindGroup,
		ResourceName: "analysts",
	}

	err := sc.Execute(context.Background(), action)
	require.NoError(t, err)
	require.Len(t, captured, 1)

	req := captured[0]
	assert.Equal(t, http.MethodDelete, req.Method)
	assert.Contains(t, req.Path, "/groups/analysts")
}

func TestExecuteTable_Create(t *testing.T) {
	t.Parallel()

	var captured []execCapture
	sc := newTestExecuteClient(t, &captured)

	action := declarative.Action{
		Operation:    declarative.OpCreate,
		ResourceKind: declarative.KindTable,
		ResourceName: "demo.analytics.orders",
		Desired: declarative.TableResource{
			CatalogName: "demo",
			SchemaName:  "analytics",
			TableName:   "orders",
			Spec: declarative.TableSpec{
				TableType: "MANAGED",
				Comment:   "Order data",
			},
		},
	}

	err := sc.Execute(context.Background(), action)
	require.NoError(t, err)
	require.Len(t, captured, 1)

	req := captured[0]
	assert.Equal(t, http.MethodPost, req.Method)
	assert.Contains(t, req.Path, "/tables")
}

func TestExecuteView_Create(t *testing.T) {
	t.Parallel()

	var captured []execCapture
	sc := newTestExecuteClient(t, &captured)

	action := declarative.Action{
		Operation:    declarative.OpCreate,
		ResourceKind: declarative.KindView,
		ResourceName: "demo.analytics.my_view",
		Desired: declarative.ViewResource{
			CatalogName: "demo",
			SchemaName:  "analytics",
			ViewName:    "my_view",
			Spec: declarative.ViewSpec{
				ViewDefinition: "SELECT 1",
				Comment:        "Test view",
			},
		},
	}

	err := sc.Execute(context.Background(), action)
	require.NoError(t, err)
	require.Len(t, captured, 1)

	req := captured[0]
	assert.Equal(t, http.MethodPost, req.Method)
	assert.Contains(t, req.Path, "/views")
}

func TestExecute_APIErrorResponse(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusConflict)
		_, _ = w.Write([]byte(`{"code":"CONFLICT","message":"already exists"}`))
	}))
	t.Cleanup(srv.Close)

	client := gen.NewClient(srv.URL, "", "test-token")
	sc := NewAPIStateClient(client)
	sc.index = newResourceIndex()

	action := declarative.Action{
		Operation:    declarative.OpCreate,
		ResourceKind: declarative.KindPrincipal,
		ResourceName: "alice",
		Desired: declarative.PrincipalSpec{
			Name: "alice",
			Type: "user",
		},
	}

	err := sc.Execute(context.Background(), action)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "already exists")
}

func TestReadState_ViewsAndVolumes(t *testing.T) {
	t.Parallel()

	mux := http.NewServeMux()
	mux.HandleFunc("/v1/catalogs", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		resp := map[string]interface{}{
			"data": []map[string]interface{}{
				{"id": "cat-1", "name": "demo", "metastore_type": "sqlite", "dsn": ":memory:"},
			},
		}
		_ = json.NewEncoder(w).Encode(resp)
	})
	mux.HandleFunc("/v1/catalogs/demo/schemas", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		resp := map[string]interface{}{
			"data": []map[string]interface{}{
				{"id": "sch-1", "name": "public"},
			},
		}
		_ = json.NewEncoder(w).Encode(resp)
	})
	mux.HandleFunc("/v1/catalogs/demo/schemas/public/tables", emptyListHandler())
	mux.HandleFunc("/v1/catalogs/demo/schemas/public/views", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		resp := map[string]interface{}{
			"data": []map[string]interface{}{
				{
					"name":            "my_view",
					"view_definition": "SELECT 1 AS x",
					"comment":         "Simple view",
					"owner":           "bob",
				},
			},
		}
		_ = json.NewEncoder(w).Encode(resp)
	})
	mux.HandleFunc("/v1/catalogs/demo/schemas/public/volumes", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		resp := map[string]interface{}{
			"data": []map[string]interface{}{
				{
					"name":             "data_vol",
					"volume_type":      "EXTERNAL",
					"storage_location": "s3://bucket/data",
					"comment":          "Data volume",
					"owner":            "alice",
				},
			},
		}
		_ = json.NewEncoder(w).Encode(resp)
	})
	mux.HandleFunc("/", emptyListHandler())

	sc := setupReadStateClient(t, mux)
	state, err := sc.ReadState(context.Background())

	require.NoError(t, err)

	// Views
	require.Len(t, state.Views, 1)
	assert.Equal(t, "demo", state.Views[0].CatalogName)
	assert.Equal(t, "public", state.Views[0].SchemaName)
	assert.Equal(t, "my_view", state.Views[0].ViewName)
	assert.Equal(t, "SELECT 1 AS x", state.Views[0].Spec.ViewDefinition)
	assert.Equal(t, "Simple view", state.Views[0].Spec.Comment)

	// Volumes
	require.Len(t, state.Volumes, 1)
	assert.Equal(t, "data_vol", state.Volumes[0].VolumeName)
	assert.Equal(t, "EXTERNAL", state.Volumes[0].Spec.VolumeType)
	assert.Equal(t, "s3://bucket/data", state.Volumes[0].Spec.StorageLocation)
}

// === Issue #141: schema_id / table_id extraction tests ===

func TestExecuteSchema_CreatePopulatesIndexFromSchemaID(t *testing.T) {
	t.Parallel()

	// Server returns schema_id (not id) — the real API behavior.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(`{"schema_id":"schema-uuid-456","name":"analytics"}`))
	}))
	t.Cleanup(srv.Close)

	client := gen.NewClient(srv.URL, "", "test-token")
	sc := NewAPIStateClient(client)
	sc.index = newResourceIndex()

	action := declarative.Action{
		Operation:    declarative.OpCreate,
		ResourceKind: declarative.KindSchema,
		ResourceName: "demo.analytics",
		Desired: declarative.SchemaResource{
			CatalogName: "demo",
			SchemaName:  "analytics",
		},
	}

	err := sc.Execute(context.Background(), action)
	require.NoError(t, err)
	assert.Equal(t, "schema-uuid-456", sc.index.schemaIDByPath["demo.analytics"])
}

func TestExecuteTable_CreatePopulatesIndexFromTableID(t *testing.T) {
	t.Parallel()

	// Server returns table_id (not id) — the real API behavior.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(`{"table_id":"table-uuid-789","name":"orders"}`))
	}))
	t.Cleanup(srv.Close)

	client := gen.NewClient(srv.URL, "", "test-token")
	sc := NewAPIStateClient(client)
	sc.index = newResourceIndex()

	action := declarative.Action{
		Operation:    declarative.OpCreate,
		ResourceKind: declarative.KindTable,
		ResourceName: "demo.analytics.orders",
		Desired: declarative.TableResource{
			CatalogName: "demo",
			SchemaName:  "analytics",
			TableName:   "orders",
			Spec:        declarative.TableSpec{TableType: "MANAGED"},
		},
	}

	err := sc.Execute(context.Background(), action)
	require.NoError(t, err)
	assert.Equal(t, "table-uuid-789", sc.index.tableIDByPath["demo.analytics.orders"])
}

func TestExecuteSchema_CreateConflictHydratesIndex(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/catalogs/demo/schemas"):
			w.WriteHeader(http.StatusConflict)
			_, _ = w.Write([]byte(`{"code":409,"message":"schema exists"}`))
		case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/catalogs/demo/schemas/analytics"):
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"schema_id":"schema-existing-123","name":"analytics"}`))
		default:
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte(`{"code":404,"message":"not found"}`))
		}
	}))
	t.Cleanup(srv.Close)

	client := gen.NewClient(srv.URL, "", "test-token")
	sc := NewAPIStateClient(client)
	sc.index = newResourceIndex()

	action := declarative.Action{
		Operation:    declarative.OpCreate,
		ResourceKind: declarative.KindSchema,
		ResourceName: "demo.analytics",
		Desired:      declarative.SchemaResource{CatalogName: "demo", SchemaName: "analytics"},
	}

	err := sc.Execute(context.Background(), action)
	require.NoError(t, err)
	assert.Equal(t, "schema-existing-123", sc.index.schemaIDByPath["demo.analytics"])
}

func TestExecuteTable_CreateConflictHydratesIndex(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/catalogs/demo/schemas/analytics/tables"):
			w.WriteHeader(http.StatusConflict)
			_, _ = w.Write([]byte(`{"code":409,"message":"table exists"}`))
		case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/catalogs/demo/schemas/analytics/tables/orders"):
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"table_id":"table-existing-456","name":"orders"}`))
		default:
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte(`{"code":404,"message":"not found"}`))
		}
	}))
	t.Cleanup(srv.Close)

	client := gen.NewClient(srv.URL, "", "test-token")
	sc := NewAPIStateClient(client)
	sc.index = newResourceIndex()

	action := declarative.Action{
		Operation:    declarative.OpCreate,
		ResourceKind: declarative.KindTable,
		ResourceName: "demo.analytics.orders",
		Desired:      declarative.TableResource{CatalogName: "demo", SchemaName: "analytics", TableName: "orders"},
	}

	err := sc.Execute(context.Background(), action)
	require.NoError(t, err)
	assert.Equal(t, "table-existing-456", sc.index.tableIDByPath["demo.analytics.orders"])
}

func TestExecuteColumnMask_CreateAlreadyExistsHydratesIndex(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/tables/table-1/column-masks"):
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte(`{"code":500,"message":"resource already exists"}`))
		case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/tables/table-1/column-masks"):
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"data":[{"id":"mask-existing-789","column_name":"name","mask_expression":"'***'","description":"Mask passenger name"}]}`))
		case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/column-masks/mask-existing-789/bindings"):
			w.WriteHeader(http.StatusNoContent)
			_, _ = w.Write([]byte(`{}`))
		default:
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte(`{"code":404,"message":"not found"}`))
		}
	}))
	t.Cleanup(srv.Close)

	client := gen.NewClient(srv.URL, "", "test-token")
	sc := NewAPIStateClient(client)
	sc.index = newResourceIndex()
	sc.index.tableIDByPath["demo.titanic.passengers"] = "table-1"
	sc.index.groupIDByName["viewers"] = "group-viewers"

	createMask := declarative.Action{
		Operation:    declarative.OpCreate,
		ResourceKind: declarative.KindColumnMask,
		ResourceName: "demo.titanic.passengers/mask-name",
		Desired: declarative.ColumnMaskSpec{
			Name:           "mask-name",
			ColumnName:     "name",
			MaskExpression: "'***'",
			Description:    "Mask passenger name",
		},
	}

	err := sc.Execute(context.Background(), createMask)
	require.NoError(t, err)
	assert.Equal(t, "mask-existing-789", sc.index.columnMaskIDByPath["demo.titanic.passengers/mask-name"])

	createBinding := declarative.Action{
		Operation:    declarative.OpCreate,
		ResourceKind: declarative.KindColumnMaskBinding,
		ResourceName: "demo.titanic.passengers/mask-name->group:viewers",
		Desired: declarative.MaskBindingRef{
			Principal:     "viewers",
			PrincipalType: "group",
			SeeOriginal:   false,
		},
	}

	err = sc.Execute(context.Background(), createBinding)
	require.NoError(t, err)
}

func TestExecuteTagAssignment_CreateResolvesTableIDViaLookup(t *testing.T) {
	t.Parallel()

	var assignmentBody map[string]interface{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/catalogs/demo/schemas/bronze/tables/ratings_raw"):
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"table_id":"table-fresh-321","name":"ratings_raw"}`))
		case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/tags/tag-classification-pii/assignments"):
			defer func() { _ = r.Body.Close() }()
			data, _ := io.ReadAll(r.Body)
			_ = json.Unmarshal(data, &assignmentBody)
			w.WriteHeader(http.StatusCreated)
			_, _ = w.Write([]byte(`{"id":"assignment-1"}`))
		default:
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte(`{"code":404,"message":"not found"}`))
		}
	}))
	t.Cleanup(srv.Close)

	client := gen.NewClient(srv.URL, "", "test-token")
	sc := NewAPIStateClient(client)
	sc.index = newResourceIndex()
	sc.index.tagIDByKey["classification:pii"] = "tag-classification-pii"

	action := declarative.Action{
		Operation:    declarative.OpCreate,
		ResourceKind: declarative.KindTagAssignment,
		ResourceName: "classification:pii on table.demo.bronze.ratings_raw",
		Desired: declarative.TagAssignmentSpec{
			Tag:           "classification:pii",
			SecurableType: "table",
			Securable:     "demo.bronze.ratings_raw",
		},
	}

	err := sc.Execute(context.Background(), action)
	require.NoError(t, err)
	assert.Equal(t, "table-fresh-321", sc.index.tableIDByPath["demo.bronze.ratings_raw"])
	require.NotNil(t, assignmentBody)
	assert.Equal(t, "table-fresh-321", assignmentBody["securable_id"])
}

func TestExecuteTagAssignment_DeleteResolvesTableIDViaLookup(t *testing.T) {
	t.Parallel()

	var capturedQuery url.Values
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/catalogs/demo/schemas/bronze/tables/ratings_raw"):
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"table_id":"table-fresh-321","name":"ratings_raw"}`))
		case r.Method == http.MethodDelete && strings.HasSuffix(r.URL.Path, "/tag-assignments"):
			capturedQuery = r.URL.Query()
			w.WriteHeader(http.StatusNoContent)
			_, _ = w.Write([]byte(`{}`))
		default:
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte(`{"code":404,"message":"not found"}`))
		}
	}))
	t.Cleanup(srv.Close)

	client := gen.NewClient(srv.URL, "", "test-token")
	sc := NewAPIStateClient(client)
	sc.index = newResourceIndex()
	sc.index.tagIDByKey["classification:pii"] = "tag-classification-pii"

	action := declarative.Action{
		Operation:    declarative.OpDelete,
		ResourceKind: declarative.KindTagAssignment,
		ResourceName: "classification:pii on table.demo.bronze.ratings_raw",
		Actual: declarative.TagAssignmentSpec{
			Tag:           "classification:pii",
			SecurableType: "table",
			Securable:     "demo.bronze.ratings_raw",
		},
	}

	err := sc.Execute(context.Background(), action)
	require.NoError(t, err)
	assert.Equal(t, "table-fresh-321", sc.index.tableIDByPath["demo.bronze.ratings_raw"])
	require.NotNil(t, capturedQuery)
	assert.Equal(t, "table-fresh-321", capturedQuery.Get("securable_id"))
	assert.Equal(t, "tag-classification-pii", capturedQuery.Get("tag_id"))
}

func TestExecute_CrossLayerResolution(t *testing.T) {
	t.Parallel()

	// Server returns type-specific IDs for creates, 200 for grants.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if strings.Contains(r.URL.Path, "/schemas") && r.Method == http.MethodPost {
			w.WriteHeader(http.StatusCreated)
			_, _ = w.Write([]byte(`{"schema_id":"schema-new-id","name":"analytics"}`))
			return
		}
		if strings.Contains(r.URL.Path, "/grants") {
			w.WriteHeader(http.StatusCreated)
			_, _ = w.Write([]byte(`{"id":"grant-new-id"}`))
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{}`))
	}))
	t.Cleanup(srv.Close)

	client := gen.NewClient(srv.URL, "", "test-token")
	sc := NewAPIStateClient(client)
	sc.index = newResourceIndex()
	// Pre-populate what ReadState would provide for existing resources.
	sc.index.principalIDByName["alice"] = "principal-alice"
	sc.index.catalogIDByName["demo"] = "catalog-id"

	// Step 1: Create schema (layer 3).
	schemaAction := declarative.Action{
		Operation:    declarative.OpCreate,
		ResourceKind: declarative.KindSchema,
		ResourceName: "demo.analytics",
		Desired: declarative.SchemaResource{
			CatalogName: "demo",
			SchemaName:  "analytics",
		},
	}
	err := sc.Execute(context.Background(), schemaAction)
	require.NoError(t, err)

	// Step 2: Create grant referencing the just-created schema (layer 5).
	// This would fail with "schema not found in index" before the fix.
	grantAction := declarative.Action{
		Operation:    declarative.OpCreate,
		ResourceKind: declarative.KindPrivilegeGrant,
		ResourceName: "alice/schema.demo.analytics/USE_SCHEMA",
		Desired: declarative.GrantSpec{
			Principal:     "alice",
			PrincipalType: "user",
			SecurableType: "schema",
			Securable:     "demo.analytics",
			Privilege:     "USE_SCHEMA",
		},
	}
	err = sc.Execute(context.Background(), grantAction)
	require.NoError(t, err, "grant on just-created schema should resolve via index")
}

func TestExecute_CreateWithNoIDInResponse(t *testing.T) {
	t.Parallel()

	// Server returns no ID at all.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(`{"name":"analytics"}`))
	}))
	t.Cleanup(srv.Close)

	client := gen.NewClient(srv.URL, "", "test-token")
	sc := NewAPIStateClient(client)
	sc.index = newResourceIndex()

	action := declarative.Action{
		Operation:    declarative.OpCreate,
		ResourceKind: declarative.KindSchema,
		ResourceName: "demo.analytics",
		Desired: declarative.SchemaResource{
			CatalogName: "demo",
			SchemaName:  "analytics",
		},
	}

	// Should not error, but index won't be populated.
	err := sc.Execute(context.Background(), action)
	require.NoError(t, err)
	assert.Empty(t, sc.index.schemaIDByPath["demo.analytics"])
}

// === Issue #142: group membership delete tests ===

func TestExecuteGroupMembership_DeleteWithMemberID(t *testing.T) {
	t.Parallel()

	var captured []execCapture
	sc := withTestIndex(newTestExecuteClient(t, &captured))

	// Delete a membership where Name is empty but MemberID is set.
	action := declarative.Action{
		Operation:    declarative.OpDelete,
		ResourceKind: declarative.KindGroupMembership,
		ResourceName: "analysts/(user)",
		Actual: declarative.MemberRef{
			Name:     "",
			Type:     "user",
			MemberID: "principal-id-alice",
		},
	}

	err := sc.Execute(context.Background(), action)
	require.NoError(t, err)
	require.Len(t, captured, 1)

	req := captured[0]
	assert.Equal(t, http.MethodDelete, req.Method)
	assert.Equal(t, "principal-id-alice", queryStr(req, "member_id"))
	assert.Equal(t, "user", queryStr(req, "member_type"))
}

func TestExecuteGroupMembership_DeleteNoNameNoID(t *testing.T) {
	t.Parallel()

	var captured []execCapture
	sc := withTestIndex(newTestExecuteClient(t, &captured))

	action := declarative.Action{
		Operation:    declarative.OpDelete,
		ResourceKind: declarative.KindGroupMembership,
		ResourceName: "analysts/(user)",
		Actual: declarative.MemberRef{
			Name:     "",
			Type:     "user",
			MemberID: "",
		},
	}

	err := sc.Execute(context.Background(), action)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "member has neither ID nor name")
}

func TestReadState_GroupMembersReverseLookup(t *testing.T) {
	t.Parallel()

	mux := http.NewServeMux()
	mux.HandleFunc("/v1/principals", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		resp := map[string]interface{}{
			"data": []map[string]interface{}{
				{"id": "p-abc", "name": "alice", "type": "user", "is_admin": false},
			},
		}
		_ = json.NewEncoder(w).Encode(resp)
	})
	mux.HandleFunc("/v1/groups", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/groups" {
			emptyListHandler()(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		resp := map[string]interface{}{
			"data": []map[string]interface{}{
				{"id": "g-1", "name": "admins", "description": ""},
			},
		}
		_ = json.NewEncoder(w).Encode(resp)
	})
	mux.HandleFunc("/v1/groups/g-1/members", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		resp := map[string]interface{}{
			"data": []map[string]interface{}{
				{"member_id": "p-abc", "member_type": "user"},
			},
		}
		_ = json.NewEncoder(w).Encode(resp)
	})
	mux.HandleFunc("/", emptyListHandler())

	sc := setupReadStateClient(t, mux)
	state, err := sc.ReadState(context.Background())

	require.NoError(t, err)
	require.Len(t, state.Groups, 1)
	require.Len(t, state.Groups[0].Members, 1)

	member := state.Groups[0].Members[0]
	assert.Equal(t, "alice", member.Name, "name should be reverse-looked-up from principal index")
	assert.Equal(t, "user", member.Type)
	assert.Equal(t, "p-abc", member.MemberID, "MemberID should be preserved from API response")
}
