package api

import (
	"errors"
	"math"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"duck-demo/internal/domain"
)

// === Test helpers (prefixed with "helpers" to avoid collisions) ===

var helpersFixedTime = time.Date(2025, 1, 15, 10, 0, 0, 0, time.UTC)

func helpersStrPtr(s string) *string { return &s }

func helpersIntPtr(v int64) *int64 { return &v }

// === safeIntToInt32 ===

func TestHelpers_safeIntToInt32(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		in   int
		want int32
	}{
		{name: "normal value", in: 100, want: 100},
		{name: "overflow clamps to MaxInt32", in: math.MaxInt32 + 1, want: math.MaxInt32},
		{name: "underflow clamps to MinInt32", in: -(math.MaxInt32 + 2), want: math.MinInt32},
		{name: "zero", in: 0, want: 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := safeIntToInt32(tt.in)
			assert.Equal(t, tt.want, got)
		})
	}
}

// === pageFromParams ===

func TestHelpers_pageFromParams(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		maxResults *MaxResults
		pageToken  *PageToken
		wantMR     int
		wantPT     string
	}{
		{name: "nil nil returns defaults", maxResults: nil, pageToken: nil, wantMR: 0, wantPT: ""},
		{name: "both set", maxResults: ptrInt32(10), pageToken: ptrString("abc"), wantMR: 10, wantPT: "abc"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := pageFromParams(tt.maxResults, tt.pageToken)
			assert.Equal(t, tt.wantMR, got.MaxResults)
			assert.Equal(t, tt.wantPT, got.PageToken)
		})
	}
}

func ptrInt32(v int32) *int32    { return &v }
func ptrString(s string) *string { return &s }

// === httpStatusFromError ===

func TestHelpers_httpStatusFromError(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		err  error
		want int
	}{
		{name: "NotFoundError -> 404", err: domain.ErrNotFound("gone"), want: http.StatusNotFound},
		{name: "AccessDeniedError -> 403", err: domain.ErrAccessDenied("nope"), want: http.StatusForbidden},
		{name: "ValidationError -> 400", err: domain.ErrValidation("bad"), want: http.StatusBadRequest},
		{name: "ConflictError -> 409", err: domain.ErrConflict("dup"), want: http.StatusConflict},
		{name: "generic error -> 500", err: errors.New("boom"), want: http.StatusInternalServerError},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := httpStatusFromError(tt.err)
			assert.Equal(t, tt.want, got)
		})
	}
}

// === errorCodeFromError ===

func TestHelpers_errorCodeFromError(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		err  error
		want int32
	}{
		{name: "NotFoundError -> 404", err: domain.ErrNotFound("gone"), want: 404},
		{name: "AccessDeniedError -> 403", err: domain.ErrAccessDenied("nope"), want: 403},
		{name: "ValidationError -> 400", err: domain.ErrValidation("bad"), want: 400},
		{name: "ConflictError -> 409", err: domain.ErrConflict("dup"), want: 409},
		{name: "generic error -> 500", err: errors.New("boom"), want: 500},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := errorCodeFromError(tt.err)
			assert.Equal(t, tt.want, got)
		})
	}
}

// === Mapping functions ===

func TestHelpers_principalToAPI(t *testing.T) {
	t.Parallel()
	p := domain.Principal{ID: "p-1", Name: "alice", Type: "user", IsAdmin: true, CreatedAt: helpersFixedTime}
	result := principalToAPI(p)

	require.NotNil(t, result.Id)
	assert.Equal(t, "p-1", *result.Id)
	require.NotNil(t, result.Name)
	assert.Equal(t, "alice", *result.Name)
	require.NotNil(t, result.Type)
	assert.Equal(t, PrincipalType("user"), *result.Type)
	require.NotNil(t, result.IsAdmin)
	assert.True(t, *result.IsAdmin)
	require.NotNil(t, result.CreatedAt)
	assert.Equal(t, helpersFixedTime, *result.CreatedAt)
}

func TestHelpers_groupToAPI(t *testing.T) {
	t.Parallel()
	g := domain.Group{ID: "g-1", Name: "analysts", Description: "Data analysts", CreatedAt: helpersFixedTime}
	result := groupToAPI(g)

	require.NotNil(t, result.Id)
	assert.Equal(t, "g-1", *result.Id)
	require.NotNil(t, result.Name)
	assert.Equal(t, "analysts", *result.Name)
	require.NotNil(t, result.Description)
	assert.Equal(t, "Data analysts", *result.Description)
	require.NotNil(t, result.CreatedAt)
	assert.Equal(t, helpersFixedTime, *result.CreatedAt)
}

func TestHelpers_grantToAPI(t *testing.T) {
	t.Parallel()
	g := domain.PrivilegeGrant{
		ID: "gr-1", PrincipalID: "p-1", PrincipalType: "user",
		SecurableType: "table", SecurableID: "t-1", Privilege: "SELECT",
		GrantedAt: helpersFixedTime,
	}
	result := grantToAPI(g)

	require.NotNil(t, result.Id)
	assert.Equal(t, "gr-1", *result.Id)
	require.NotNil(t, result.PrincipalId)
	assert.Equal(t, "p-1", *result.PrincipalId)
	require.NotNil(t, result.PrincipalType)
	assert.Equal(t, PrivilegeGrantPrincipalType("user"), *result.PrincipalType)
	require.NotNil(t, result.SecurableType)
	assert.Equal(t, "table", *result.SecurableType)
	require.NotNil(t, result.SecurableId)
	assert.Equal(t, "t-1", *result.SecurableId)
	require.NotNil(t, result.Privilege)
	assert.Equal(t, "SELECT", *result.Privilege)
	require.NotNil(t, result.GrantedAt)
	assert.Equal(t, helpersFixedTime, *result.GrantedAt)
}

func TestHelpers_rowFilterToAPI(t *testing.T) {
	t.Parallel()
	rf := domain.RowFilter{
		ID: "rf-1", TableID: "t-1", FilterSQL: "age > 18",
		Description: "Adults only", CreatedAt: helpersFixedTime,
	}
	result := rowFilterToAPI(rf)

	require.NotNil(t, result.Id)
	assert.Equal(t, "rf-1", *result.Id)
	require.NotNil(t, result.TableId)
	assert.Equal(t, "t-1", *result.TableId)
	require.NotNil(t, result.FilterSql)
	assert.Equal(t, "age > 18", *result.FilterSql)
	require.NotNil(t, result.Description)
	assert.Equal(t, "Adults only", *result.Description)
	require.NotNil(t, result.CreatedAt)
	assert.Equal(t, helpersFixedTime, *result.CreatedAt)
}

func TestHelpers_columnMaskToAPI(t *testing.T) {
	t.Parallel()
	cm := domain.ColumnMask{
		ID: "cm-1", TableID: "t-1", ColumnName: "ssn",
		MaskExpression: "'***'", Description: "Mask SSN",
		CreatedAt: helpersFixedTime,
	}
	result := columnMaskToAPI(cm)

	require.NotNil(t, result.Id)
	assert.Equal(t, "cm-1", *result.Id)
	require.NotNil(t, result.TableId)
	assert.Equal(t, "t-1", *result.TableId)
	require.NotNil(t, result.ColumnName)
	assert.Equal(t, "ssn", *result.ColumnName)
	require.NotNil(t, result.MaskExpression)
	assert.Equal(t, "'***'", *result.MaskExpression)
	require.NotNil(t, result.Description)
	assert.Equal(t, "Mask SSN", *result.Description)
	require.NotNil(t, result.CreatedAt)
	assert.Equal(t, helpersFixedTime, *result.CreatedAt)
}

func TestHelpers_schemaDetailToAPI(t *testing.T) {
	t.Parallel()
	s := domain.SchemaDetail{
		SchemaID: "s-1", Name: "main", CatalogName: "default",
		Comment: "default schema", Owner: "admin",
		Properties: map[string]string{"key": "val"},
		CreatedAt:  helpersFixedTime, UpdatedAt: helpersFixedTime,
	}
	result := schemaDetailToAPI(s)

	require.NotNil(t, result.SchemaId)
	assert.Equal(t, "s-1", *result.SchemaId)
	require.NotNil(t, result.Name)
	assert.Equal(t, "main", *result.Name)
	require.NotNil(t, result.CatalogName)
	assert.Equal(t, "default", *result.CatalogName)
	require.NotNil(t, result.Comment)
	assert.Equal(t, "default schema", *result.Comment)
	require.NotNil(t, result.Owner)
	assert.Equal(t, "admin", *result.Owner)
	require.NotNil(t, result.Properties)
	assert.Equal(t, map[string]string{"key": "val"}, *result.Properties)
	require.NotNil(t, result.CreatedAt)
	assert.Equal(t, helpersFixedTime, *result.CreatedAt)
	require.NotNil(t, result.UpdatedAt)
	assert.Equal(t, helpersFixedTime, *result.UpdatedAt)
}

func TestHelpers_tableDetailToAPI(t *testing.T) {
	t.Parallel()
	td := domain.TableDetail{
		TableID: "t-1", Name: "users", SchemaName: "main",
		CatalogName: "default", TableType: "MANAGED",
		Comment: "User data", Owner: "admin",
		Properties: map[string]string{},
		Columns: []domain.ColumnDetail{
			{Name: "id", Type: "INTEGER", Position: 0, Nullable: false},
		},
		CreatedAt:   helpersFixedTime,
		UpdatedAt:   helpersFixedTime,
		StoragePath: "/data/users",
		Statistics:  &domain.TableStatistics{RowCount: helpersIntPtr(100)},
	}
	result := tableDetailToAPI(td)

	require.NotNil(t, result.TableId)
	assert.Equal(t, "t-1", *result.TableId)
	require.NotNil(t, result.Name)
	assert.Equal(t, "users", *result.Name)
	require.NotNil(t, result.SchemaName)
	assert.Equal(t, "main", *result.SchemaName)
	require.NotNil(t, result.CatalogName)
	assert.Equal(t, "default", *result.CatalogName)
	require.NotNil(t, result.TableType)
	assert.Equal(t, "MANAGED", *result.TableType)
	require.NotNil(t, result.Columns)
	require.Len(t, *result.Columns, 1)
	assert.Equal(t, "id", *(*result.Columns)[0].Name)
	require.NotNil(t, result.StoragePath, "StoragePath should be set")
	assert.Equal(t, "/data/users", *result.StoragePath)
	require.NotNil(t, result.Statistics, "Statistics should be set")
	require.NotNil(t, result.Statistics.RowCount)
	assert.Equal(t, int64(100), *result.Statistics.RowCount)
}

func TestHelpers_columnDetailToAPI(t *testing.T) {
	t.Parallel()
	c := domain.ColumnDetail{
		Name: "id", Type: "INTEGER", Position: 0,
		Nullable: false, Comment: "Primary key",
		Properties: map[string]string{},
	}
	result := columnDetailToAPI(c)

	require.NotNil(t, result.Name)
	assert.Equal(t, "id", *result.Name)
	require.NotNil(t, result.Type)
	assert.Equal(t, "INTEGER", *result.Type)
	require.NotNil(t, result.Position)
	assert.Equal(t, int32(0), *result.Position)
	require.NotNil(t, result.Nullable)
	assert.False(t, *result.Nullable)
	require.NotNil(t, result.Comment)
	assert.Equal(t, "Primary key", *result.Comment)
	require.NotNil(t, result.Properties)
	assert.Empty(t, *result.Properties)
}

func TestHelpers_searchResultToAPI(t *testing.T) {
	t.Parallel()
	sr := domain.SearchResult{
		Type: "table", Name: "users",
		SchemaName: helpersStrPtr("main"), MatchField: "name",
	}
	result := searchResultToAPI(sr)

	require.NotNil(t, result.Type)
	assert.Equal(t, "table", *result.Type)
	require.NotNil(t, result.Name)
	assert.Equal(t, "users", *result.Name)
	require.NotNil(t, result.SchemaName)
	assert.Equal(t, "main", *result.SchemaName)
	require.NotNil(t, result.MatchField)
	assert.Equal(t, "name", *result.MatchField)
}

func TestHelpers_lineageEdgeToAPI(t *testing.T) {
	t.Parallel()
	le := domain.LineageEdge{
		ID: "e-1", SourceTable: "src", TargetTable: helpersStrPtr("tgt"),
		SourceSchema: "s1", TargetSchema: "s2",
		EdgeType: "READ", PrincipalName: "alice",
		CreatedAt: helpersFixedTime,
	}
	result := lineageEdgeToAPI(le)

	require.NotNil(t, result.Id)
	assert.Equal(t, "e-1", *result.Id)
	require.NotNil(t, result.SourceTable)
	assert.Equal(t, "src", *result.SourceTable)
	require.NotNil(t, result.TargetTable)
	assert.Equal(t, "tgt", *result.TargetTable)
	require.NotNil(t, result.SourceSchema)
	assert.Equal(t, "s1", *result.SourceSchema)
	require.NotNil(t, result.TargetSchema)
	assert.Equal(t, "s2", *result.TargetSchema)
	require.NotNil(t, result.EdgeType)
	assert.Equal(t, LineageEdgeEdgeType("READ"), *result.EdgeType)
	require.NotNil(t, result.PrincipalName)
	assert.Equal(t, "alice", *result.PrincipalName)
	require.NotNil(t, result.CreatedAt)
	assert.Equal(t, helpersFixedTime, *result.CreatedAt)
}

func TestHelpers_tagToAPI(t *testing.T) {
	t.Parallel()
	tg := domain.Tag{
		ID: "t-1", Key: "classification", Value: helpersStrPtr("pii"),
		CreatedBy: "admin", CreatedAt: helpersFixedTime,
	}
	result := tagToAPI(tg)

	require.NotNil(t, result.Id)
	assert.Equal(t, "t-1", *result.Id)
	require.NotNil(t, result.Key)
	assert.Equal(t, "classification", *result.Key)
	require.NotNil(t, result.Value)
	assert.Equal(t, "pii", *result.Value)
	require.NotNil(t, result.CreatedBy)
	assert.Equal(t, "admin", *result.CreatedBy)
	require.NotNil(t, result.CreatedAt)
	assert.Equal(t, helpersFixedTime, *result.CreatedAt)
}

func TestHelpers_viewDetailToAPI(t *testing.T) {
	t.Parallel()
	v := domain.ViewDetail{
		ID: "v-1", SchemaID: "s-1", SchemaName: "main",
		CatalogName: "default", Name: "active_users",
		ViewDefinition: "SELECT * FROM users WHERE active",
		Owner:          "admin",
		Properties:     map[string]string{},
		SourceTables:   []string{"users"},
		CreatedAt:      helpersFixedTime,
		UpdatedAt:      helpersFixedTime,
	}
	result := viewDetailToAPI(v)

	require.NotNil(t, result.Id)
	assert.Equal(t, "v-1", *result.Id)
	require.NotNil(t, result.SchemaId)
	assert.Equal(t, "s-1", *result.SchemaId)
	require.NotNil(t, result.SchemaName)
	assert.Equal(t, "main", *result.SchemaName)
	require.NotNil(t, result.CatalogName)
	assert.Equal(t, "default", *result.CatalogName)
	require.NotNil(t, result.Name)
	assert.Equal(t, "active_users", *result.Name)
	require.NotNil(t, result.ViewDefinition)
	assert.Equal(t, "SELECT * FROM users WHERE active", *result.ViewDefinition)
	require.NotNil(t, result.Owner)
	assert.Equal(t, "admin", *result.Owner)
	require.NotNil(t, result.Properties)
	assert.Empty(t, *result.Properties)
	require.NotNil(t, result.SourceTables)
	assert.Equal(t, []string{"users"}, *result.SourceTables)
	require.NotNil(t, result.CreatedAt)
	assert.Equal(t, helpersFixedTime, *result.CreatedAt)
	require.NotNil(t, result.UpdatedAt)
	assert.Equal(t, helpersFixedTime, *result.UpdatedAt)
}

func TestHelpers_catalogInfoToAPI(t *testing.T) {
	t.Parallel()
	ci := domain.CatalogInfo{
		Name: "default", Comment: "Default catalog",
		CreatedAt: helpersFixedTime, UpdatedAt: helpersFixedTime,
	}
	result := catalogInfoToAPI(ci)

	require.NotNil(t, result.Name)
	assert.Equal(t, "default", *result.Name)
	require.NotNil(t, result.Comment)
	assert.Equal(t, "Default catalog", *result.Comment)
	require.NotNil(t, result.CreatedAt)
	assert.Equal(t, helpersFixedTime, *result.CreatedAt)
	require.NotNil(t, result.UpdatedAt)
	assert.Equal(t, helpersFixedTime, *result.UpdatedAt)
}

// === strPtrIfNonEmpty ===

func TestHelpers_strPtrIfNonEmpty(t *testing.T) {
	t.Parallel()

	t.Run("empty returns nil", func(t *testing.T) {
		t.Parallel()
		assert.Nil(t, strPtrIfNonEmpty(""))
	})

	t.Run("non-empty returns pointer", func(t *testing.T) {
		t.Parallel()
		result := strPtrIfNonEmpty("hello")
		require.NotNil(t, result)
		assert.Equal(t, "hello", *result)
	})
}

// === optStr ===

func TestHelpers_optStr(t *testing.T) {
	t.Parallel()

	t.Run("empty returns nil", func(t *testing.T) {
		t.Parallel()
		assert.Nil(t, optStr(""))
	})

	t.Run("non-empty returns pointer", func(t *testing.T) {
		t.Parallel()
		result := optStr("hello")
		require.NotNil(t, result)
		assert.Equal(t, "hello", *result)
	})
}

// === tableStatisticsToAPI ===

func TestHelpers_tableStatisticsToAPI(t *testing.T) {
	t.Parallel()

	t.Run("nil returns empty", func(t *testing.T) {
		t.Parallel()
		result := tableStatisticsToAPI(nil)
		assert.Nil(t, result.RowCount)
		assert.Nil(t, result.SizeBytes)
		assert.Nil(t, result.ColumnCount)
	})

	t.Run("non-nil populates fields", func(t *testing.T) {
		t.Parallel()
		stats := &domain.TableStatistics{RowCount: helpersIntPtr(100)}
		result := tableStatisticsToAPI(stats)
		require.NotNil(t, result.RowCount)
		assert.Equal(t, int64(100), *result.RowCount)
	})
}
