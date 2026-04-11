package cuesqlgen

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	cuesql "duck-demo/pkg/cue-sql"
)

func TestStructurizeQuery_SelectJoinAndStaticPredicate(t *testing.T) {
	query, ok := StructurizeQuery(cuesql.Query{
		Name: "GetAPIKeyByHash",
		Kind: cuesql.KindOne,
		Params: []cuesql.Param{
			{Name: "keyHash", Type: "string"},
		},
		Raw: &cuesql.Raw{
			SQL:  "-- name: GetAPIKeyByHash :one\nSELECT ak.id, p.name AS principal_name FROM api_keys ak JOIN principals p ON ak.principal_id = p.id WHERE ak.key_hash = ? AND (ak.expires_at IS NULL OR ak.expires_at > datetime('now', 'localtime'))",
			Bind: []string{"keyHash"},
		},
	})
	require.True(t, ok)
	require.NotNil(t, query.Select)
	assert.Nil(t, query.Raw)
	assert.Equal(t, "api_keys", query.Select.From)
	assert.Equal(t, "ak", query.Select.Alias)
	require.Len(t, query.Select.Joins, 1)
	assert.Equal(t, "principals", query.Select.Joins[0].Table)
	assert.Equal(t, "keyHash", query.Select.Where[0].Param)
	assert.Equal(t, "(ak.expires_at IS NULL OR ak.expires_at > datetime('now', 'localtime'))", query.Select.Where[1].RawSQL)
}

func TestStructurizeQuery_InsertReturningAndModifier(t *testing.T) {
	query, ok := StructurizeQuery(cuesql.Query{
		Name: "AddGroupMember",
		Kind: cuesql.KindExec,
		Params: []cuesql.Param{
			{Name: "GroupID", Type: "string"},
			{Name: "MemberType", Type: "string"},
			{Name: "MemberID", Type: "string"},
		},
		Raw: &cuesql.Raw{
			SQL:  "-- name: AddGroupMember :exec\nINSERT OR IGNORE INTO group_members (group_id, member_type, member_id)\nVALUES (?, ?, ?)",
			Bind: []string{"GroupID", "MemberType", "MemberID"},
		},
	})
	require.True(t, ok)
	require.NotNil(t, query.Insert)
	assert.Equal(t, "OR IGNORE", query.Insert.Modifier)
	assert.Equal(t, []string{"group_id", "member_type", "member_id"}, query.Insert.Columns)
	assert.Equal(t, "GroupID", query.Insert.Values[0].Param)
}

func TestStructurizeQuery_UpdateReturning(t *testing.T) {
	query, ok := StructurizeQuery(cuesql.Query{
		Name: "UpdateGroup",
		Kind: cuesql.KindOne,
		Params: []cuesql.Param{
			{Name: "Description", Type: "sql.NullString"},
			{Name: "id", Type: "string"},
		},
		Raw: &cuesql.Raw{
			SQL:  "-- name: UpdateGroup :one\nUPDATE groups SET description = ? WHERE id = ? RETURNING id, name, description, created_at",
			Bind: []string{"Description", "id"},
		},
	})
	require.True(t, ok)
	require.NotNil(t, query.Update)
	assert.Equal(t, "Description", query.Update.Set[0].Value.Param)
	assert.Equal(t, "id", query.Update.Where[0].Param)
	assert.Equal(t, "id", query.Update.ReturningColumns[0].Expr)
}
