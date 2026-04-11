package cuesql

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRenderPredicateSQL_ValueSQL(t *testing.T) {
	sqlPart, err := RenderPredicateSQL(Predicate{
		Column:   "id",
		Op:       "=",
		ValueSQL: "1",
	})
	require.NoError(t, err)
	assert.Equal(t, "id = 1", sqlPart)
}

func TestRenderSQLite_InsertConflict(t *testing.T) {
	rendered, err := RenderSQLite(Query{
		Name: "UpsertAuthProviderConfig",
		Kind: KindExec,
		Params: []Param{
			{Name: "OidcEnabled", Type: "int64"},
		},
		Insert: &Insert{
			Into:    "auth_providers",
			Columns: []string{"id", "oidc_enabled", "updated_at"},
			Values: []ValueExpr{
				{SQL: "1"},
				{Param: "OidcEnabled"},
				{SQL: "CURRENT_TIMESTAMP"},
			},
			Conflict: &Conflict{
				Targets: []string{"id"},
				DoUpdate: []Assignment{
					{Column: "oidc_enabled", Value: ValueExpr{SQL: "excluded.oidc_enabled"}},
					{Column: "updated_at", Value: ValueExpr{SQL: "CURRENT_TIMESTAMP"}},
				},
			},
		},
	}, staticCatalog{
		"auth_providers": {"id", "oidc_enabled", "updated_at"},
	})
	require.NoError(t, err)
	assert.Contains(t, rendered.SQL, "ON CONFLICT(id) DO UPDATE SET")
	assert.Contains(t, rendered.SQL, "oidc_enabled = excluded.oidc_enabled")
	assert.Contains(t, rendered.SQL, "updated_at = CURRENT_TIMESTAMP")
}

func TestRenderSQLite_InsertModifierAndReturningColumns(t *testing.T) {
	rendered, err := RenderSQLite(Query{
		Name: "CreateGroupMember",
		Kind: KindOne,
		Params: []Param{
			{Name: "GroupID", Type: "string"},
			{Name: "MemberID", Type: "string"},
		},
		Insert: &Insert{
			Modifier: "OR IGNORE",
			Into:     "group_members",
			Columns:  []string{"group_id", "member_id"},
			Values: []ValueExpr{
				{Param: "GroupID"},
				{Param: "MemberID"},
			},
			ReturningColumns: []Column{
				{Expr: "group_id"},
				{Expr: "member_id"},
			},
		},
	}, staticCatalog{
		"group_members": {"group_id", "member_id"},
	})
	require.NoError(t, err)
	assert.Contains(t, rendered.SQL, "INSERT OR IGNORE INTO group_members")
	assert.Contains(t, rendered.SQL, "RETURNING group_id, member_id")
}

func TestRenderSQLite_SelectPredicateGroupsAndLiteralLimit(t *testing.T) {
	rendered, err := RenderSQLite(Query{
		Name: "GetAuditSummary",
		Kind: KindMany,
		Result: Result{
			Row: "AuditRow",
			Fields: []Field{
				{Name: "ID", Type: "string"},
			},
		},
		Select: &Select{
			From:    "audit_log",
			Columns: []Column{{Expr: "id"}},
			Where: []Predicate{
				{Column: "action", Op: "=", ValueSQL: "'QUERY'"},
				{
					Any: []Predicate{
						{Column: "status", Op: "=", Param: "Status", Optional: true},
						{Column: "principal_name", Op: "=", Param: "PrincipalName", Optional: true},
					},
				},
			},
			OrderBy:  []OrderBy{{Expr: "created_at DESC"}},
			LimitSQL: "1",
		},
	}, staticCatalog{
		"audit_log": {"id", "action", "status", "principal_name", "created_at"},
	})
	require.NoError(t, err)
	assert.Contains(t, rendered.SQL, "WHERE action = 'QUERY'")
	assert.Contains(t, rendered.SQL, "ORDER BY created_at DESC")
	assert.Contains(t, rendered.SQL, "LIMIT 1")
	assert.True(t, rendered.Dynamic)
}

func TestRenderAssignment_CoalesceWithColumn(t *testing.T) {
	assert.Equal(t, "description = COALESCE(?, description)", renderAssignment(Assignment{
		Column:       "description",
		Value:        ValueExpr{Param: "Description"},
		CoalesceWith: true,
	}))
}

type staticCatalog map[string][]string

func (s staticCatalog) ColumnsForTable(table string) ([]string, error) {
	columns, ok := s[table]
	if !ok {
		return nil, assert.AnError
	}
	return columns, nil
}
