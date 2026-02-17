package model

import (
	"testing"

	"duck-demo/internal/sqlrewrite"

	"github.com/stretchr/testify/assert"
)

func TestCanDirectExecOnConn(t *testing.T) {
	tests := []struct {
		name     string
		stmtType sqlrewrite.StatementType
		query    string
		want     bool
	}{
		{name: "allow create view", stmtType: sqlrewrite.StmtDDL, query: "CREATE OR REPLACE VIEW main.v AS SELECT 1", want: true},
		{name: "allow create table", stmtType: sqlrewrite.StmtDDL, query: "CREATE OR REPLACE TABLE main.t AS SELECT 1", want: true},
		{name: "allow temp table", stmtType: sqlrewrite.StmtDDL, query: "CREATE TEMP TABLE _tmp AS SELECT 1", want: true},
		{name: "allow drop table", stmtType: sqlrewrite.StmtDDL, query: "DROP TABLE IF EXISTS _tmp", want: true},
		{name: "allow create macro", stmtType: sqlrewrite.StmtDDL, query: "CREATE OR REPLACE MACRO m(x) AS x + 1", want: true},
		{name: "allow set variable", stmtType: sqlrewrite.StmtOther, query: "SET VARIABLE load_window_days='7'", want: true},
		{name: "deny create schema", stmtType: sqlrewrite.StmtDDL, query: "CREATE SCHEMA analytics", want: false},
		{name: "deny drop schema", stmtType: sqlrewrite.StmtDDL, query: "DROP SCHEMA analytics", want: false},
		{name: "deny copy", stmtType: sqlrewrite.StmtOther, query: "COPY t TO 'x.parquet'", want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := canDirectExecOnConn(tt.stmtType, tt.query)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestResolveIncrementalStrategy(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want string
	}{
		{name: "default merge", in: "", want: "merge"},
		{name: "normalized merge", in: " MERGE ", want: "merge"},
		{name: "delete insert alias", in: "delete+insert", want: "delete_insert"},
		{name: "delete insert canonical", in: "delete_insert", want: "delete_insert"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, resolveIncrementalStrategy(tt.in))
		})
	}
}

func TestResolveSchemaChangePolicy(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want string
	}{
		{name: "default ignore", in: "", want: "ignore"},
		{name: "normalize ignore", in: " IGNORE ", want: "ignore"},
		{name: "pass through fail", in: "fail", want: "fail"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, resolveSchemaChangePolicy(tt.in))
		})
	}
}

func TestSameColumns(t *testing.T) {
	assert.True(t, sameColumns([]string{"id", "name"}, []string{"id", "name"}))
	assert.False(t, sameColumns([]string{"id", "name"}, []string{"name", "id"}))
	assert.False(t, sameColumns([]string{"id"}, []string{"id", "name"}))
}
