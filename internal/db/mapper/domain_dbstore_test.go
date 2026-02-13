package mapper

import (
	"database/sql"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	dbstore "duck-demo/internal/db/dbstore"
	"duck-demo/internal/domain"
)

// === Utility function tests ===

func TestParseTime_Valid(t *testing.T) {
	got := parseTime("2024-06-15 10:30:45")
	assert.Equal(t, 2024, got.Year())
	assert.Equal(t, 6, int(got.Month()))
	assert.Equal(t, 15, got.Day())
	assert.Equal(t, 10, got.Hour())
	assert.Equal(t, 30, got.Minute())
	assert.Equal(t, 45, got.Second())
}

func TestParseTime_Invalid(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{"empty string", ""},
		{"garbage", "not-a-date"},
		{"wrong format", "2024/06/15"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := parseTime(tc.input)
			assert.True(t, got.IsZero())
		})
	}
}

func TestNullStr(t *testing.T) {
	t.Run("non-nil", func(t *testing.T) {
		s := "hello"
		got := nullStr(&s)
		assert.True(t, got.Valid)
		assert.Equal(t, "hello", got.String)
	})
	t.Run("nil", func(t *testing.T) {
		got := nullStr(nil)
		assert.False(t, got.Valid)
	})
}

func TestNullStrVal(t *testing.T) {
	t.Run("non-empty", func(t *testing.T) {
		got := nullStrVal("hello")
		assert.True(t, got.Valid)
		assert.Equal(t, "hello", got.String)
	})
	t.Run("empty", func(t *testing.T) {
		got := nullStrVal("")
		assert.False(t, got.Valid)
	})
}

func TestNullInt(t *testing.T) {
	t.Run("non-nil", func(t *testing.T) {
		i := int64(42)
		got := nullInt(&i)
		assert.True(t, got.Valid)
		assert.Equal(t, int64(42), got.Int64)
	})
	t.Run("nil", func(t *testing.T) {
		got := nullInt(nil)
		assert.False(t, got.Valid)
	})
}

func TestPtrStr(t *testing.T) {
	t.Run("valid", func(t *testing.T) {
		got := ptrStr(sql.NullString{String: "hello", Valid: true})
		require.NotNil(t, got)
		assert.Equal(t, "hello", *got)
	})
	t.Run("invalid", func(t *testing.T) {
		got := ptrStr(sql.NullString{})
		assert.Nil(t, got)
	})
}

func TestPtrInt(t *testing.T) {
	t.Run("valid", func(t *testing.T) {
		got := ptrInt(sql.NullInt64{Int64: 42, Valid: true})
		require.NotNil(t, got)
		assert.Equal(t, int64(42), *got)
	})
	t.Run("invalid", func(t *testing.T) {
		got := ptrInt(sql.NullInt64{})
		assert.Nil(t, got)
	})
}

func TestNullStrFromPtr(t *testing.T) {
	s := "test"
	got := NullStrFromPtr(&s)
	assert.True(t, got.Valid)
	assert.Equal(t, "test", got.String)

	got = NullStrFromPtr(nil)
	assert.False(t, got.Valid)
}

func TestNullStrFromStr(t *testing.T) {
	got := NullStrFromStr("test")
	assert.True(t, got.Valid)
	assert.Equal(t, "test", got.String)

	got = NullStrFromStr("")
	assert.False(t, got.Valid)
}

func TestInterfaceFromPtr(t *testing.T) {
	s := "hello"
	got := InterfaceFromPtr(&s)
	assert.Equal(t, "hello", got)

	got = InterfaceFromPtr(nil)
	assert.Nil(t, got)
}

func TestStringFromPtr(t *testing.T) {
	s := "test"
	got := StringFromPtr(&s)
	assert.True(t, got.Valid)
	assert.Equal(t, "test", got.String)

	got = StringFromPtr(nil)
	assert.False(t, got.Valid)
}

// === Principal mapper tests ===

func TestPrincipalFromDB(t *testing.T) {
	t.Run("full fields", func(t *testing.T) {
		db := dbstore.Principal{
			ID:             "p-1",
			Name:           "alice",
			Type:           "user",
			IsAdmin:        1,
			CreatedAt:      "2024-06-15 10:30:45",
			ExternalID:     sql.NullString{String: "ext-123", Valid: true},
			ExternalIssuer: sql.NullString{String: "https://issuer.example.com", Valid: true},
		}
		got := PrincipalFromDB(db)
		assert.Equal(t, "p-1", got.ID)
		assert.Equal(t, "alice", got.Name)
		assert.Equal(t, "user", got.Type)
		assert.True(t, got.IsAdmin)
		require.NotNil(t, got.ExternalID)
		assert.Equal(t, "ext-123", *got.ExternalID)
		require.NotNil(t, got.ExternalIssuer)
		assert.Equal(t, "https://issuer.example.com", *got.ExternalIssuer)
		assert.False(t, got.CreatedAt.IsZero())
	})

	t.Run("null fields", func(t *testing.T) {
		db := dbstore.Principal{
			ID:        "p-2",
			Name:      "bob",
			Type:      "service_principal",
			IsAdmin:   0,
			CreatedAt: "2024-01-01 00:00:00",
		}
		got := PrincipalFromDB(db)
		assert.False(t, got.IsAdmin)
		assert.Nil(t, got.ExternalID)
		assert.Nil(t, got.ExternalIssuer)
	})
}

func TestPrincipalsFromDB(t *testing.T) {
	t.Run("multiple", func(t *testing.T) {
		dbs := []dbstore.Principal{
			{ID: "p-1", Name: "alice", Type: "user", CreatedAt: "2024-01-01 00:00:00"},
			{ID: "p-2", Name: "bob", Type: "user", CreatedAt: "2024-01-02 00:00:00"},
		}
		got := PrincipalsFromDB(dbs)
		assert.Len(t, got, 2)
		assert.Equal(t, "alice", got[0].Name)
		assert.Equal(t, "bob", got[1].Name)
	})

	t.Run("empty slice", func(t *testing.T) {
		got := PrincipalsFromDB([]dbstore.Principal{})
		assert.Empty(t, got)
	})
}

// === Group mapper tests ===

func TestGroupFromDB(t *testing.T) {
	t.Run("with description", func(t *testing.T) {
		db := dbstore.Group{
			ID:          "g-1",
			Name:        "analysts",
			Description: sql.NullString{String: "Data analysts", Valid: true},
			CreatedAt:   "2024-03-15 12:00:00",
		}
		got := GroupFromDB(db)
		assert.Equal(t, "g-1", got.ID)
		assert.Equal(t, "analysts", got.Name)
		assert.Equal(t, "Data analysts", got.Description)
	})

	t.Run("empty description", func(t *testing.T) {
		db := dbstore.Group{
			ID:        "g-2",
			Name:      "admins",
			CreatedAt: "2024-01-01 00:00:00",
		}
		got := GroupFromDB(db)
		assert.Empty(t, got.Description)
	})
}

func TestGroupMemberFromDB(t *testing.T) {
	db := dbstore.GroupMember{
		GroupID:    "g-1",
		MemberType: "user",
		MemberID:   "p-1",
	}
	got := GroupMemberFromDB(db)
	assert.Equal(t, "g-1", got.GroupID)
	assert.Equal(t, "user", got.MemberType)
	assert.Equal(t, "p-1", got.MemberID)
}

// === Grant mapper tests ===

func TestGrantFromDB(t *testing.T) {
	t.Run("with granted_by", func(t *testing.T) {
		db := dbstore.PrivilegeGrant{
			ID:            "grant-1",
			PrincipalID:   "p-1",
			PrincipalType: "user",
			SecurableType: "table",
			SecurableID:   "t-1",
			Privilege:     "SELECT",
			GrantedBy:     sql.NullString{String: "admin", Valid: true},
			GrantedAt:     "2024-05-01 10:00:00",
		}
		got := GrantFromDB(db)
		assert.Equal(t, "grant-1", got.ID)
		assert.Equal(t, "p-1", got.PrincipalID)
		assert.Equal(t, "SELECT", got.Privilege)
		require.NotNil(t, got.GrantedBy)
		assert.Equal(t, "admin", *got.GrantedBy)
	})

	t.Run("without granted_by", func(t *testing.T) {
		db := dbstore.PrivilegeGrant{
			ID:        "grant-2",
			GrantedAt: "2024-01-01 00:00:00",
		}
		got := GrantFromDB(db)
		assert.Nil(t, got.GrantedBy)
	})
}

// === RowFilter mapper tests ===

func TestRowFilterFromDB(t *testing.T) {
	db := dbstore.RowFilter{
		ID:          "rf-1",
		TableID:     "t-1",
		FilterSql:   `"Pclass" = 1`,
		Description: sql.NullString{String: "First class only", Valid: true},
		CreatedAt:   "2024-06-01 09:00:00",
	}
	got := RowFilterFromDB(db)
	assert.Equal(t, "rf-1", got.ID)
	assert.Equal(t, "t-1", got.TableID)
	assert.Equal(t, `"Pclass" = 1`, got.FilterSQL)
	assert.Equal(t, "First class only", got.Description)
}

func TestRowFilterBindingFromDB(t *testing.T) {
	db := dbstore.RowFilterBinding{
		ID:            "rfb-1",
		RowFilterID:   "rf-1",
		PrincipalID:   "p-1",
		PrincipalType: "user",
	}
	got := RowFilterBindingFromDB(db)
	assert.Equal(t, "rfb-1", got.ID)
	assert.Equal(t, "rf-1", got.RowFilterID)
	assert.Equal(t, "p-1", got.PrincipalID)
	assert.Equal(t, "user", got.PrincipalType)
}

// === ColumnMask mapper tests ===

func TestColumnMaskFromDB(t *testing.T) {
	db := dbstore.ColumnMask{
		ID:             "cm-1",
		TableID:        "t-1",
		ColumnName:     "Name",
		MaskExpression: "'***'",
		Description:    sql.NullString{String: "Mask PII", Valid: true},
		CreatedAt:      "2024-06-01 09:00:00",
	}
	got := ColumnMaskFromDB(db)
	assert.Equal(t, "cm-1", got.ID)
	assert.Equal(t, "Name", got.ColumnName)
	assert.Equal(t, "'***'", got.MaskExpression)
	assert.Equal(t, "Mask PII", got.Description)
}

func TestColumnMaskBindingFromDB(t *testing.T) {
	t.Run("see_original true", func(t *testing.T) {
		db := dbstore.ColumnMaskBinding{
			ID:            "cmb-1",
			ColumnMaskID:  "cm-1",
			PrincipalID:   "p-1",
			PrincipalType: "user",
			SeeOriginal:   1,
		}
		got := ColumnMaskBindingFromDB(db)
		assert.True(t, got.SeeOriginal)
	})

	t.Run("see_original false", func(t *testing.T) {
		db := dbstore.ColumnMaskBinding{
			ID:            "cmb-2",
			ColumnMaskID:  "cm-1",
			PrincipalID:   "p-2",
			PrincipalType: "group",
			SeeOriginal:   0,
		}
		got := ColumnMaskBindingFromDB(db)
		assert.False(t, got.SeeOriginal)
	})
}

// === AuditEntry mapper tests ===

func TestAuditEntryFromDB(t *testing.T) {
	t.Run("full entry", func(t *testing.T) {
		stmtType := "SELECT"
		origSQL := "SELECT * FROM titanic"
		rewrittenSQL := `SELECT * FROM titanic WHERE "Pclass" = 1`
		errMsg := ""
		durMs := int64(42)
		rowsRet := int64(100)

		db := dbstore.AuditLog{
			ID:             "audit-1",
			PrincipalName:  "alice",
			Action:         "QUERY",
			StatementType:  sql.NullString{String: stmtType, Valid: true},
			OriginalSql:    sql.NullString{String: origSQL, Valid: true},
			RewrittenSql:   sql.NullString{String: rewrittenSQL, Valid: true},
			TablesAccessed: sql.NullString{String: `["titanic"]`, Valid: true},
			Status:         "ALLOWED",
			ErrorMessage:   sql.NullString{String: errMsg, Valid: false},
			DurationMs:     sql.NullInt64{Int64: durMs, Valid: true},
			RowsReturned:   sql.NullInt64{Int64: rowsRet, Valid: true},
			CreatedAt:      "2024-06-15 10:30:45",
		}
		got := AuditEntryFromDB(db)
		assert.Equal(t, "audit-1", got.ID)
		assert.Equal(t, "alice", got.PrincipalName)
		assert.Equal(t, "QUERY", got.Action)
		require.NotNil(t, got.StatementType)
		assert.Equal(t, "SELECT", *got.StatementType)
		assert.Equal(t, []string{"titanic"}, got.TablesAccessed)
		assert.Equal(t, "ALLOWED", got.Status)
		assert.Nil(t, got.ErrorMessage)
		require.NotNil(t, got.DurationMs)
		assert.Equal(t, int64(42), *got.DurationMs)
		require.NotNil(t, got.RowsReturned)
		assert.Equal(t, int64(100), *got.RowsReturned)
	})

	t.Run("empty tables JSON", func(t *testing.T) {
		db := dbstore.AuditLog{
			ID:             "audit-2",
			PrincipalName:  "bob",
			Action:         "QUERY",
			TablesAccessed: sql.NullString{String: "", Valid: false},
			Status:         "DENIED",
			CreatedAt:      "2024-01-01 00:00:00",
		}
		got := AuditEntryFromDB(db)
		assert.Empty(t, got.TablesAccessed)
	})

	t.Run("invalid tables JSON", func(t *testing.T) {
		db := dbstore.AuditLog{
			ID:             "audit-3",
			PrincipalName:  "carol",
			Action:         "QUERY",
			TablesAccessed: sql.NullString{String: "not-json", Valid: true},
			Status:         "ALLOWED",
			CreatedAt:      "2024-01-01 00:00:00",
		}
		got := AuditEntryFromDB(db)
		assert.Empty(t, got.TablesAccessed) // silently fails
	})
}

func TestAuditEntriesToDBParams(t *testing.T) {
	t.Run("with tables", func(t *testing.T) {
		stmtType := "SELECT"
		origSQL := "SELECT * FROM titanic"
		durMs := int64(42)
		rowsRet := int64(100)

		entry := &domain.AuditEntry{
			PrincipalName:  "alice",
			Action:         "QUERY",
			StatementType:  &stmtType,
			OriginalSQL:    &origSQL,
			TablesAccessed: []string{"titanic", "passengers"},
			Status:         "ALLOWED",
			DurationMs:     &durMs,
			RowsReturned:   &rowsRet,
		}
		got := AuditEntriesToDBParams(entry)
		assert.Equal(t, "alice", got.PrincipalName)
		assert.Equal(t, "QUERY", got.Action)
		assert.True(t, got.StatementType.Valid)
		assert.True(t, got.TablesAccessed.Valid)
		assert.Contains(t, got.TablesAccessed.String, "titanic")
		assert.Contains(t, got.TablesAccessed.String, "passengers")
		assert.True(t, got.DurationMs.Valid)
		assert.Equal(t, int64(42), got.DurationMs.Int64)
		assert.True(t, got.RowsReturned.Valid)
		assert.Equal(t, int64(100), got.RowsReturned.Int64)
		assert.NotEmpty(t, got.ID) // UUID generated
	})

	t.Run("empty tables", func(t *testing.T) {
		entry := &domain.AuditEntry{
			PrincipalName:  "bob",
			Action:         "QUERY",
			TablesAccessed: nil,
			Status:         "DENIED",
		}
		got := AuditEntriesToDBParams(entry)
		assert.False(t, got.TablesAccessed.Valid)
	})
}

// === QueryHistory mapper tests ===

func TestQueryHistoryEntryFromDB(t *testing.T) {
	db := dbstore.AuditLog{
		ID:             "qh-1",
		PrincipalName:  "alice",
		Action:         "QUERY",
		StatementType:  sql.NullString{String: "SELECT", Valid: true},
		OriginalSql:    sql.NullString{String: "SELECT 1", Valid: true},
		RewrittenSql:   sql.NullString{String: "SELECT 1", Valid: true},
		TablesAccessed: sql.NullString{String: `["t1"]`, Valid: true},
		Status:         "ALLOWED",
		DurationMs:     sql.NullInt64{Int64: 10, Valid: true},
		RowsReturned:   sql.NullInt64{Int64: 1, Valid: true},
		CreatedAt:      "2024-06-15 10:30:45",
	}
	got := QueryHistoryEntryFromDB(db)
	assert.Equal(t, "qh-1", got.ID)
	assert.Equal(t, "alice", got.PrincipalName)
	assert.Equal(t, "ALLOWED", got.Status)
	assert.Equal(t, []string{"t1"}, got.TablesAccessed)
	require.NotNil(t, got.DurationMs)
	assert.Equal(t, int64(10), *got.DurationMs)
}

// === Lineage mapper tests ===

func TestLineageEdgeFromDB(t *testing.T) {
	db := dbstore.GetUpstreamLineageRow{
		SourceTable:   "source_table",
		TargetTable:   sql.NullString{String: "target_table", Valid: true},
		SourceSchema:  sql.NullString{String: "main", Valid: true},
		TargetSchema:  sql.NullString{String: "analytics", Valid: true},
		EdgeType:      "READ",
		PrincipalName: "alice",
		CreatedAt:     "2024-06-01 09:00:00",
	}
	got := LineageEdgeFromDB(db)
	assert.Equal(t, "source_table", got.SourceTable)
	require.NotNil(t, got.TargetTable)
	assert.Equal(t, "target_table", *got.TargetTable)
	assert.Equal(t, "main", got.SourceSchema)
	assert.Equal(t, "analytics", got.TargetSchema)
	assert.Equal(t, "READ", got.EdgeType)
}

func TestLineageEdgeFromDownstreamDB(t *testing.T) {
	db := dbstore.GetDownstreamLineageRow{
		SourceTable:   "source_table",
		TargetTable:   sql.NullString{String: "downstream_table", Valid: true},
		SourceSchema:  sql.NullString{String: "main", Valid: true},
		TargetSchema:  sql.NullString{String: "main", Valid: true},
		EdgeType:      "WRITE",
		PrincipalName: "bob",
		CreatedAt:     "2024-06-01 09:00:00",
	}
	got := LineageEdgeFromDownstreamDB(db)
	assert.Equal(t, "source_table", got.SourceTable)
	require.NotNil(t, got.TargetTable)
	assert.Equal(t, "downstream_table", *got.TargetTable)
	assert.Equal(t, "WRITE", got.EdgeType)
}

// === Tag mapper tests ===

func TestTagFromDB(t *testing.T) {
	t.Run("with value", func(t *testing.T) {
		db := dbstore.Tag{
			ID:        "tag-1",
			Key:       "pii",
			Value:     sql.NullString{String: "email", Valid: true},
			CreatedBy: "admin",
			CreatedAt: "2024-06-01 09:00:00",
		}
		got := TagFromDB(db)
		assert.Equal(t, "tag-1", got.ID)
		assert.Equal(t, "pii", got.Key)
		require.NotNil(t, got.Value)
		assert.Equal(t, "email", *got.Value)
		assert.Equal(t, "admin", got.CreatedBy)
	})

	t.Run("without value", func(t *testing.T) {
		db := dbstore.Tag{
			ID:        "tag-2",
			Key:       "deprecated",
			CreatedBy: "admin",
			CreatedAt: "2024-01-01 00:00:00",
		}
		got := TagFromDB(db)
		assert.Nil(t, got.Value)
	})
}

func TestTagAssignmentFromDB(t *testing.T) {
	t.Run("with column", func(t *testing.T) {
		db := dbstore.TagAssignment{
			ID:            "ta-1",
			TagID:         "tag-1",
			SecurableType: "column",
			SecurableID:   "t-1",
			ColumnName:    sql.NullString{String: "email", Valid: true},
			AssignedBy:    "admin",
			AssignedAt:    "2024-06-01 09:00:00",
		}
		got := TagAssignmentFromDB(db)
		assert.Equal(t, "ta-1", got.ID)
		require.NotNil(t, got.ColumnName)
		assert.Equal(t, "email", *got.ColumnName)
	})

	t.Run("without column", func(t *testing.T) {
		db := dbstore.TagAssignment{
			ID:            "ta-2",
			TagID:         "tag-1",
			SecurableType: "table",
			SecurableID:   "t-1",
			AssignedBy:    "admin",
			AssignedAt:    "2024-01-01 00:00:00",
		}
		got := TagAssignmentFromDB(db)
		assert.Nil(t, got.ColumnName)
	})
}

// === CatalogRegistration mapper tests ===

func TestCatalogRegistrationFromDB(t *testing.T) {
	db := dbstore.Catalog{
		ID:            "cat-1",
		Name:          "production",
		MetastoreType: "sqlite",
		Dsn:           "/path/to/meta.db",
		DataPath:      "s3://bucket/data",
		Status:        "active",
		StatusMessage: sql.NullString{String: "ready", Valid: true},
		IsDefault:     1,
		Comment:       sql.NullString{String: "prod catalog", Valid: true},
		CreatedAt:     "2024-06-15 10:30:45",
		UpdatedAt:     "2024-06-15 10:30:45",
	}
	got := CatalogRegistrationFromDB(db)
	assert.Equal(t, "cat-1", got.ID)
	assert.Equal(t, "production", got.Name)
	assert.Equal(t, "sqlite", string(got.MetastoreType))
	assert.Equal(t, "active", string(got.Status))
	assert.Equal(t, "ready", got.StatusMessage)
	assert.True(t, got.IsDefault)
	assert.Equal(t, "prod catalog", got.Comment)
}

// === View mapper tests ===

func TestViewFromDB(t *testing.T) {
	t.Run("full view", func(t *testing.T) {
		db := dbstore.View{
			ID:             "v-1",
			SchemaID:       "s-1",
			Name:           "my_view",
			ViewDefinition: "SELECT * FROM titanic WHERE survived = 1",
			Comment:        sql.NullString{String: "survivors only", Valid: true},
			Properties:     sql.NullString{String: `{"env":"prod"}`, Valid: true},
			Owner:          "admin",
			SourceTables:   sql.NullString{String: `["titanic"]`, Valid: true},
			CreatedAt:      "2024-06-15 10:30:45",
			UpdatedAt:      "2024-06-15 10:30:45",
			DeletedAt:      sql.NullString{},
		}
		got := ViewFromDB(db)
		assert.Equal(t, "v-1", got.ID)
		assert.Equal(t, "s-1", got.SchemaID)
		assert.Equal(t, "my_view", got.Name)
		require.NotNil(t, got.Comment)
		assert.Equal(t, "survivors only", *got.Comment)
		assert.Equal(t, map[string]string{"env": "prod"}, got.Properties)
		assert.Equal(t, []string{"titanic"}, got.SourceTables)
		assert.Nil(t, got.DeletedAt)
	})

	t.Run("empty JSON fields", func(t *testing.T) {
		db := dbstore.View{
			ID:             "v-2",
			SchemaID:       "s-1",
			Name:           "empty_view",
			ViewDefinition: "SELECT 1",
			Properties:     sql.NullString{},
			SourceTables:   sql.NullString{},
			Owner:          "admin",
			CreatedAt:      "2024-01-01 00:00:00",
			UpdatedAt:      "2024-01-01 00:00:00",
		}
		got := ViewFromDB(db)
		assert.NotNil(t, got.Properties) // always initialized
		assert.Empty(t, got.Properties)
		assert.Empty(t, got.SourceTables)
	})

	t.Run("with deleted_at", func(t *testing.T) {
		db := dbstore.View{
			ID:             "v-3",
			SchemaID:       "s-1",
			Name:           "deleted_view",
			ViewDefinition: "SELECT 1",
			Owner:          "admin",
			CreatedAt:      "2024-01-01 00:00:00",
			UpdatedAt:      "2024-01-01 00:00:00",
			DeletedAt:      sql.NullString{String: "2024-06-15 10:30:45", Valid: true},
		}
		got := ViewFromDB(db)
		require.NotNil(t, got.DeletedAt)
		assert.Equal(t, 2024, got.DeletedAt.Year())
	})
}
