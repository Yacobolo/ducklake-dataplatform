//go:build integration

package integration

import (
	"context"
	"errors"
	"testing"

	"duck-demo/internal/db/repository"
	"duck-demo/internal/domain"
)

// ---------------------------------------------------------------------------
// TestCatalog_SchemaCRUD — full schema lifecycle (create, get, list, update, delete)
// ---------------------------------------------------------------------------

func TestCatalog_SchemaCRUD(t *testing.T) {
	env := requireCatalogEnv(t)
	repo := repository.NewCatalogRepo(env.MetaDB, env.DuckDB)
	ctx := context.Background()

	type step struct {
		name string
		fn   func(t *testing.T)
	}

	steps := []step{
		{
			name: "CreateSchema",
			fn: func(t *testing.T) {
				s, err := repo.CreateSchema(ctx, "crud_analytics", "test schema", "admin")
				if err != nil {
					t.Fatalf("CreateSchema: %v", err)
				}
				if s.Name != "crud_analytics" {
					t.Errorf("Name = %q, want %q", s.Name, "crud_analytics")
				}
				if s.CatalogName != "lake" {
					t.Errorf("CatalogName = %q, want %q", s.CatalogName, "lake")
				}
				if s.SchemaID <= 0 {
					t.Errorf("SchemaID = %d, want > 0", s.SchemaID)
				}
				if s.Comment != "test schema" {
					t.Errorf("Comment = %q, want %q", s.Comment, "test schema")
				}
				if s.Owner != "admin" {
					t.Errorf("Owner = %q, want %q", s.Owner, "admin")
				}
			},
		},
		{
			name: "GetSchema",
			fn: func(t *testing.T) {
				s, err := repo.GetSchema(ctx, "crud_analytics")
				if err != nil {
					t.Fatalf("GetSchema: %v", err)
				}
				if s.Name != "crud_analytics" {
					t.Errorf("Name = %q, want %q", s.Name, "crud_analytics")
				}
				if s.CatalogName != "lake" {
					t.Errorf("CatalogName = %q, want %q", s.CatalogName, "lake")
				}
			},
		},
		{
			name: "ListSchemas_ContainsCreatedAndMain",
			fn: func(t *testing.T) {
				schemas, total, err := repo.ListSchemas(ctx, domain.PageRequest{})
				if err != nil {
					t.Fatalf("ListSchemas: %v", err)
				}
				if total < 2 {
					t.Errorf("total = %d, want >= 2 (main + crud_analytics)", total)
				}
				names := make(map[string]bool)
				for _, s := range schemas {
					names[s.Name] = true
				}
				for _, want := range []string{"main", "crud_analytics"} {
					if !names[want] {
						t.Errorf("schema %q not found in list: %v", want, names)
					}
				}
			},
		},
		{
			name: "UpdateSchema",
			fn: func(t *testing.T) {
				newComment := "updated comment"
				props := map[string]string{"team": "data", "env": "test"}
				s, err := repo.UpdateSchema(ctx, "crud_analytics", &newComment, props)
				if err != nil {
					t.Fatalf("UpdateSchema: %v", err)
				}
				if s.Comment != "updated comment" {
					t.Errorf("Comment = %q, want %q", s.Comment, "updated comment")
				}
				if s.Properties["team"] != "data" {
					t.Errorf("Properties[team] = %q, want %q", s.Properties["team"], "data")
				}
				if s.Properties["env"] != "test" {
					t.Errorf("Properties[env] = %q, want %q", s.Properties["env"], "test")
				}
			},
		},
		{
			name: "DeleteSchema",
			fn: func(t *testing.T) {
				if err := repo.DeleteSchema(ctx, "crud_analytics", false); err != nil {
					t.Fatalf("DeleteSchema: %v", err)
				}
			},
		},
		{
			name: "GetSchema_AfterDelete_NotFound",
			fn: func(t *testing.T) {
				_, err := repo.GetSchema(ctx, "crud_analytics")
				if err == nil {
					t.Fatal("expected NotFoundError, got nil")
				}
				var nfe *domain.NotFoundError
				if !errors.As(err, &nfe) {
					t.Errorf("expected NotFoundError, got %T: %v", err, err)
				}
			},
		},
	}

	for _, s := range steps {
		t.Run(s.name, s.fn)
	}
}

// ---------------------------------------------------------------------------
// TestCatalog_TableCRUD — full table lifecycle (create, get, list columns, delete)
// ---------------------------------------------------------------------------

func TestCatalog_TableCRUD(t *testing.T) {
	env := requireCatalogEnv(t)
	repo := repository.NewCatalogRepo(env.MetaDB, env.DuckDB)
	ctx := context.Background()

	type step struct {
		name string
		fn   func(t *testing.T)
	}

	steps := []step{
		{
			name: "CreateSchema_ForTable",
			fn: func(t *testing.T) {
				_, err := repo.CreateSchema(ctx, "tbl_test_schema", "for table tests", "admin")
				if err != nil {
					t.Fatalf("CreateSchema: %v", err)
				}
			},
		},
		{
			name: "CreateTable",
			fn: func(t *testing.T) {
				req := domain.CreateTableRequest{
					Name: "users",
					Columns: []domain.CreateColumnDef{
						{Name: "id", Type: "INTEGER"},
						{Name: "name", Type: "VARCHAR"},
					},
					Comment: "test users table",
				}
				td, err := repo.CreateTable(ctx, "tbl_test_schema", req, "admin")
				if err != nil {
					t.Fatalf("CreateTable: %v", err)
				}
				if td.Name != "users" {
					t.Errorf("Name = %q, want %q", td.Name, "users")
				}
				if td.SchemaName != "tbl_test_schema" {
					t.Errorf("SchemaName = %q, want %q", td.SchemaName, "tbl_test_schema")
				}
				if td.CatalogName != "lake" {
					t.Errorf("CatalogName = %q, want %q", td.CatalogName, "lake")
				}
				if td.TableType != "MANAGED" {
					t.Errorf("TableType = %q, want %q", td.TableType, "MANAGED")
				}
				if len(td.Columns) != 2 {
					t.Fatalf("len(Columns) = %d, want 2", len(td.Columns))
				}
			},
		},
		{
			name: "GetTable",
			fn: func(t *testing.T) {
				td, err := repo.GetTable(ctx, "tbl_test_schema", "users")
				if err != nil {
					t.Fatalf("GetTable: %v", err)
				}
				if len(td.Columns) != 2 {
					t.Fatalf("len(Columns) = %d, want 2", len(td.Columns))
				}
				// Verify column names (order may vary by column_id)
				colNames := make(map[string]bool)
				for _, c := range td.Columns {
					colNames[c.Name] = true
				}
				for _, want := range []string{"id", "name"} {
					if !colNames[want] {
						t.Errorf("column %q not found in %v", want, colNames)
					}
				}
			},
		},
		{
			name: "ListTables",
			fn: func(t *testing.T) {
				tables, total, err := repo.ListTables(ctx, "tbl_test_schema", domain.PageRequest{})
				if err != nil {
					t.Fatalf("ListTables: %v", err)
				}
				if total != 1 {
					t.Errorf("total = %d, want 1", total)
				}
				if len(tables) != 1 {
					t.Fatalf("len(tables) = %d, want 1", len(tables))
				}
				if tables[0].Name != "users" {
					t.Errorf("tables[0].Name = %q, want %q", tables[0].Name, "users")
				}
			},
		},
		{
			name: "ListColumns",
			fn: func(t *testing.T) {
				cols, total, err := repo.ListColumns(ctx, "tbl_test_schema", "users", domain.PageRequest{})
				if err != nil {
					t.Fatalf("ListColumns: %v", err)
				}
				if total != 2 {
					t.Errorf("total = %d, want 2", total)
				}
				if len(cols) != 2 {
					t.Fatalf("len(cols) = %d, want 2", len(cols))
				}
				// Verify names and types
				colMap := make(map[string]string) // name -> type
				for _, c := range cols {
					colMap[c.Name] = c.Type
				}
				// DuckLake may store INTEGER as int32 and VARCHAR as varchar;
				// accept any of the known aliases for each type.
				for _, want := range []struct {
					name    string
					aliases []string
				}{
					{"id", []string{"INTEGER", "int32", "INT"}},
					{"name", []string{"VARCHAR", "varchar", "TEXT"}},
				} {
					got, ok := colMap[want.name]
					if !ok {
						t.Errorf("column %q not found", want.name)
						continue
					}
					if !containsAny(got, want.aliases...) {
						t.Errorf("column %q type = %q, want one of %v", want.name, got, want.aliases)
					}
				}
			},
		},
		{
			name: "DeleteTable",
			fn: func(t *testing.T) {
				if err := repo.DeleteTable(ctx, "tbl_test_schema", "users"); err != nil {
					t.Fatalf("DeleteTable: %v", err)
				}
			},
		},
		{
			name: "GetTable_AfterDelete_NotFound",
			fn: func(t *testing.T) {
				_, err := repo.GetTable(ctx, "tbl_test_schema", "users")
				if err == nil {
					t.Fatal("expected NotFoundError, got nil")
				}
				var nfe *domain.NotFoundError
				if !errors.As(err, &nfe) {
					t.Errorf("expected NotFoundError, got %T: %v", err, err)
				}
			},
		},
	}

	for _, s := range steps {
		t.Run(s.name, s.fn)
	}
}

// ---------------------------------------------------------------------------
// TestCatalog_SchemaConflict — table-driven error cases
// ---------------------------------------------------------------------------

func TestCatalog_SchemaConflict(t *testing.T) {
	env := requireCatalogEnv(t)
	repo := repository.NewCatalogRepo(env.MetaDB, env.DuckDB)
	ctx := context.Background()

	// Pre-create a schema for duplicate tests (unique prefix to avoid collisions)
	if _, err := repo.CreateSchema(ctx, "conflict_existing", "", ""); err != nil {
		t.Fatalf("setup CreateSchema: %v", err)
	}

	cases := []struct {
		name    string
		fn      func() error
		wantErr interface{} // expected error type
	}{
		{
			name: "DuplicateSchema_Conflict",
			fn: func() error {
				_, err := repo.CreateSchema(ctx, "conflict_existing", "", "")
				return err
			},
			wantErr: &domain.ConflictError{},
		},
		{
			name: "CreateTable_NonexistentSchema_NotFound",
			fn: func() error {
				_, err := repo.CreateTable(ctx, "conflict_nonexistent", domain.CreateTableRequest{
					Name:    "t",
					Columns: []domain.CreateColumnDef{{Name: "id", Type: "INTEGER"}},
				}, "admin")
				return err
			},
			wantErr: &domain.NotFoundError{},
		},
		{
			name: "DeleteNonexistentSchema_NotFound",
			fn: func() error {
				return repo.DeleteSchema(ctx, "conflict_no_such", false)
			},
			wantErr: &domain.NotFoundError{},
		},
		{
			name: "CreateSchema_InvalidName_Validation",
			fn: func() error {
				_, err := repo.CreateSchema(ctx, "invalid-name!", "", "")
				return err
			},
			wantErr: &domain.ValidationError{},
		},
		{
			name: "CreateTable_InvalidName_Validation",
			fn: func() error {
				_, err := repo.CreateTable(ctx, "conflict_existing", domain.CreateTableRequest{
					Name:    "bad name!",
					Columns: []domain.CreateColumnDef{{Name: "id", Type: "INTEGER"}},
				}, "admin")
				return err
			},
			wantErr: &domain.ValidationError{},
		},
		{
			name: "CreateTable_NoColumns_Validation",
			fn: func() error {
				_, err := repo.CreateTable(ctx, "conflict_existing", domain.CreateTableRequest{
					Name:    "empty_table",
					Columns: nil,
				}, "admin")
				return err
			},
			wantErr: &domain.ValidationError{},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.fn()
			if err == nil {
				t.Fatal("expected error, got nil")
			}

			switch tc.wantErr.(type) {
			case *domain.ConflictError:
				var ce *domain.ConflictError
				if !errors.As(err, &ce) {
					t.Errorf("expected ConflictError, got %T: %v", err, err)
				}
			case *domain.NotFoundError:
				var nfe *domain.NotFoundError
				if !errors.As(err, &nfe) {
					t.Errorf("expected NotFoundError, got %T: %v", err, err)
				}
			case *domain.ValidationError:
				var ve *domain.ValidationError
				if !errors.As(err, &ve) {
					t.Errorf("expected ValidationError, got %T: %v", err, err)
				}
			}
		})
	}
}

// ---------------------------------------------------------------------------
// TestCatalog_CascadeDelete — non-empty schema requires force=true
// ---------------------------------------------------------------------------

func TestCatalog_CascadeDelete(t *testing.T) {
	env := requireCatalogEnv(t)
	repo := repository.NewCatalogRepo(env.MetaDB, env.DuckDB)
	ctx := context.Background()

	type step struct {
		name string
		fn   func(t *testing.T)
	}

	steps := []step{
		{
			name: "Setup_CreateSchemaAndTable",
			fn: func(t *testing.T) {
				if _, err := repo.CreateSchema(ctx, "cascade_schema", "", ""); err != nil {
					t.Fatalf("CreateSchema: %v", err)
				}
				req := domain.CreateTableRequest{
					Name:    "cascade_table",
					Columns: []domain.CreateColumnDef{{Name: "id", Type: "INTEGER"}},
				}
				if _, err := repo.CreateTable(ctx, "cascade_schema", req, "admin"); err != nil {
					t.Fatalf("CreateTable: %v", err)
				}
			},
		},
		{
			name: "DeleteSchema_NoForce_Conflict",
			fn: func(t *testing.T) {
				err := repo.DeleteSchema(ctx, "cascade_schema", false)
				if err == nil {
					t.Fatal("expected ConflictError for non-empty schema, got nil")
				}
				var ce *domain.ConflictError
				if !errors.As(err, &ce) {
					t.Errorf("expected ConflictError, got %T: %v", err, err)
				}
			},
		},
		{
			name: "DeleteSchema_WithForce_Success",
			fn: func(t *testing.T) {
				if err := repo.DeleteSchema(ctx, "cascade_schema", true); err != nil {
					t.Fatalf("DeleteSchema(force=true): %v", err)
				}
			},
		},
		{
			name: "GetTable_AfterCascade_NotFound",
			fn: func(t *testing.T) {
				_, err := repo.GetTable(ctx, "cascade_schema", "cascade_table")
				if err == nil {
					t.Fatal("expected NotFoundError after cascade delete, got nil")
				}
				var nfe *domain.NotFoundError
				if !errors.As(err, &nfe) {
					t.Errorf("expected NotFoundError, got %T: %v", err, err)
				}
			},
		},
	}

	for _, s := range steps {
		t.Run(s.name, s.fn)
	}
}

// ---------------------------------------------------------------------------
// TestCatalog_MetastoreSummary — verify counts change after mutations
// ---------------------------------------------------------------------------

func TestCatalog_MetastoreSummary(t *testing.T) {
	env := requireCatalogEnv(t)
	repo := repository.NewCatalogRepo(env.MetaDB, env.DuckDB)
	ctx := context.Background()

	type step struct {
		name string
		fn   func(t *testing.T)
	}

	steps := []step{
		{
			name: "InitialSummary",
			fn: func(t *testing.T) {
				summary, err := repo.GetMetastoreSummary(ctx)
				if err != nil {
					t.Fatalf("GetMetastoreSummary: %v", err)
				}
				if summary.CatalogName != "lake" {
					t.Errorf("CatalogName = %q, want %q", summary.CatalogName, "lake")
				}
				// DuckLake auto-creates "main" schema
				if summary.SchemaCount < 1 {
					t.Errorf("SchemaCount = %d, want >= 1", summary.SchemaCount)
				}
			},
		},
		{
			name: "AfterCreateSchemaAndTable",
			fn: func(t *testing.T) {
				// Get baseline
				before, err := repo.GetMetastoreSummary(ctx)
				if err != nil {
					t.Fatalf("GetMetastoreSummary(before): %v", err)
				}

				// Create a schema + table (unique names to avoid collisions)
				if _, err := repo.CreateSchema(ctx, "summary_schema", "", ""); err != nil {
					t.Fatalf("CreateSchema: %v", err)
				}
				req := domain.CreateTableRequest{
					Name:    "summary_table",
					Columns: []domain.CreateColumnDef{{Name: "val", Type: "INTEGER"}},
				}
				if _, err := repo.CreateTable(ctx, "summary_schema", req, "admin"); err != nil {
					t.Fatalf("CreateTable: %v", err)
				}

				after, err := repo.GetMetastoreSummary(ctx)
				if err != nil {
					t.Fatalf("GetMetastoreSummary(after): %v", err)
				}

				// Use >= to tolerate concurrent tests creating schemas/tables
				if after.SchemaCount < before.SchemaCount+1 {
					t.Errorf("SchemaCount = %d, want >= %d", after.SchemaCount, before.SchemaCount+1)
				}
				if after.TableCount < before.TableCount+1 {
					t.Errorf("TableCount = %d, want >= %d", after.TableCount, before.TableCount+1)
				}
			},
		},
	}

	for _, s := range steps {
		t.Run(s.name, s.fn)
	}
}

// ---------------------------------------------------------------------------
// TestCatalog_Pagination — create multiple schemas, paginate through them
// ---------------------------------------------------------------------------

func TestCatalog_Pagination(t *testing.T) {
	env := requireCatalogEnv(t)
	repo := repository.NewCatalogRepo(env.MetaDB, env.DuckDB)
	ctx := context.Background()

	// Create 5 schemas with unique prefix (DuckLake auto-creates "main")
	schemaNames := []string{"page_alpha", "page_bravo", "page_charlie", "page_delta", "page_echo"}
	for _, name := range schemaNames {
		if _, err := repo.CreateSchema(ctx, name, "", ""); err != nil {
			t.Fatalf("CreateSchema(%s): %v", name, err)
		}
	}

	cases := []struct {
		name       string
		maxResults int
	}{
		{
			name:       "PageSize2",
			maxResults: 2,
		},
		{
			name:       "PageSize3",
			maxResults: 3,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var allNames []string
			pageToken := ""
			pages := 0

			for {
				page := domain.PageRequest{
					MaxResults: tc.maxResults,
					PageToken:  pageToken,
				}
				schemas, total, err := repo.ListSchemas(ctx, page)
				if err != nil {
					t.Fatalf("ListSchemas page %d: %v", pages, err)
				}
				pages++

				if len(schemas) > tc.maxResults {
					t.Errorf("page %d: got %d results, want <= %d", pages, len(schemas), tc.maxResults)
				}

				for _, s := range schemas {
					allNames = append(allNames, s.Name)
				}

				// Calculate next page token
				offset := page.Offset() + len(schemas)
				if int64(offset) >= total {
					break
				}
				pageToken = domain.EncodePageToken(offset)
			}

			if pages < 2 {
				t.Errorf("pages = %d, want >= 2 (should need multiple pages)", pages)
			}

			// Verify all created schemas appear in the collected results
			collected := make(map[string]bool)
			for _, n := range allNames {
				collected[n] = true
			}
			for _, want := range schemaNames {
				if !collected[want] {
					t.Errorf("schema %q not found in paginated results: %v", want, allNames)
				}
			}
			// Also verify "main" was included
			if !collected["main"] {
				t.Errorf("schema %q not found in paginated results", "main")
			}
		})
	}
}
