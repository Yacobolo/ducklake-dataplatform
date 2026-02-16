package cli

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
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

// === Unimplemented resource kind test ===

func TestExecute_UnimplementedKindReturnsError(t *testing.T) {
	var captured []execCapture
	sc := newTestExecuteClient(t, &captured)

	action := declarative.Action{
		Operation:    declarative.OpCreate,
		ResourceKind: declarative.KindPipelineJob, // not implemented
		ResourceName: "some-job",
	}

	err := sc.Execute(context.Background(), action)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not yet implemented")
}
