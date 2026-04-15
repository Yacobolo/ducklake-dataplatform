package declarative

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDuckDB15_ValidateRowFilters_RejectsInvalidSQL(t *testing.T) {
	state := &DesiredState{
		Principals: []PrincipalSpec{{Name: "analyst", Type: "user"}},
		Catalogs:   []CatalogResource{{CatalogName: "main", Spec: CatalogSpec{MetastoreType: "sqlite", DSN: "/db", DataPath: "/data"}}},
		Schemas:    []SchemaResource{{CatalogName: "main", SchemaName: "analytics"}},
		Tables: []TableResource{{
			CatalogName: "main",
			SchemaName:  "analytics",
			TableName:   "events",
			Spec: TableSpec{
				TableType: "MANAGED",
				Columns: []ColumnDef{
					{Name: "payload", Type: "VARIANT"},
				},
			},
		}},
		RowFilters: []RowFilterResource{{
			CatalogName: "main",
			SchemaName:  "analytics",
			TableName:   "events",
			Filters: []RowFilterSpec{{
				Name:      "bad_filter",
				FilterSQL: "INVALID SQL (((",
				Bindings:  []FilterBindingRef{{Principal: "analyst", PrincipalType: "user"}},
			}},
		}},
	}

	errs := Validate(state)
	require.NotEmpty(t, errs)

	found := false
	for _, err := range errs {
		if containsStr(err.Error(), "filter_sql must be valid SQL expression") {
			found = true
			break
		}
	}
	assert.True(t, found, "expected filter_sql validation error, got %v", errs)
}

func TestDuckDB15_ValidateColumnMasks_AllowsPythonLambdaExpressions(t *testing.T) {
	state := &DesiredState{
		Principals: []PrincipalSpec{{Name: "analyst", Type: "user"}},
		Catalogs:   []CatalogResource{{CatalogName: "main", Spec: CatalogSpec{MetastoreType: "sqlite", DSN: "/db", DataPath: "/data"}}},
		Schemas:    []SchemaResource{{CatalogName: "main", SchemaName: "analytics"}},
		Tables: []TableResource{{
			CatalogName: "main",
			SchemaName:  "analytics",
			TableName:   "events",
			Spec: TableSpec{
				TableType: "MANAGED",
				Columns: []ColumnDef{
					{Name: "tags", Type: "VARCHAR[]"},
				},
			},
		}},
		ColumnMasks: []ColumnMaskResource{{
			CatalogName: "main",
			SchemaName:  "analytics",
			TableName:   "events",
			Masks: []ColumnMaskSpec{{
				Name:           "tags_mask",
				ColumnName:     "tags",
				MaskExpression: "list_transform(tags, lambda x: '***')",
				Bindings:       []MaskBindingRef{{Principal: "analyst", PrincipalType: "user"}},
			}},
		}},
	}

	errs := Validate(state)
	assert.Empty(t, errs)
}
