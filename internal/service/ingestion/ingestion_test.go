package ingestion

import (
	"context"
	"testing"

	"github.com/google/uuid"
	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	internaldb "duck-demo/internal/db"
	dbstore "duck-demo/internal/db/dbstore"
	"duck-demo/internal/db/repository"
	"duck-demo/internal/domain"
	"duck-demo/internal/service/security"
)

// setupTestService creates an AuthorizationService with a temporary SQLite DB,
// runs migrations, and creates mock DuckLake metadata tables.
func setupTestService(t *testing.T) (*security.AuthorizationService, *dbstore.Queries, context.Context) {
	t.Helper()

	db, _ := internaldb.OpenTestSQLite(t)

	// Create mock DuckLake catalog tables
	ctx := context.Background()
	_, err := db.ExecContext(ctx, `
		CREATE TABLE ducklake_schema (
			schema_id INTEGER PRIMARY KEY,
			schema_uuid TEXT,
			begin_snapshot INTEGER,
			end_snapshot INTEGER,
			schema_name TEXT,
			path TEXT,
			path_is_relative INTEGER
		);
		CREATE TABLE ducklake_table (
			table_id INTEGER,
			table_uuid TEXT,
			begin_snapshot INTEGER,
			end_snapshot INTEGER,
			schema_id INTEGER,
			table_name TEXT,
			path TEXT,
			path_is_relative INTEGER
		);
		INSERT INTO ducklake_schema (schema_id, schema_name, begin_snapshot)
		VALUES (0, 'main', 0);
		INSERT INTO ducklake_table (table_id, table_name, schema_id, begin_snapshot)
		VALUES (1, 'titanic', 0, 1);
		INSERT INTO ducklake_table (table_id, table_name, schema_id, begin_snapshot)
		VALUES (2, 'orders', 0, 1);
	`)
	if err != nil {
		t.Fatalf("create mock tables: %v", err)
	}

	principalRepo := repository.NewPrincipalRepo(db)
	groupRepo := repository.NewGroupRepo(db)
	grantRepo := repository.NewGrantRepo(db)
	rowFilterRepo := repository.NewRowFilterRepo(db)
	columnMaskRepo := repository.NewColumnMaskRepo(db)
	introspectionRepo := repository.NewIntrospectionRepo(db)

	svc := security.NewAuthorizationService(
		principalRepo, groupRepo, grantRepo,
		rowFilterRepo, columnMaskRepo, introspectionRepo,
		nil,
	)

	q := dbstore.New(db)
	return svc, q, ctx
}

// setupIngestionTest creates an IngestionService backed by a real SQLite DB
// (for authorization) but with nil presigner and executor (since we can't run
// ducklake_add_data_files in unit tests). Returns the service and test helpers.
func setupIngestionTest(t *testing.T) (*IngestionService, *security.AuthorizationService, *dbstore.Queries, context.Context) {
	t.Helper()
	authSvc, q, ctx := setupTestService(t)

	svc := &IngestionService{
		executor:         nil, // CALL statements can't be tested in unit tests
		metastoreFactory: nil, // metastore only needed for readDataPath
		authSvc:          authSvc,
		presigner:        nil, // presigner requires real S3 credentials
		auditRepo:        &noopAuditRepo{},
		bucket:           "test-bucket",
	}
	return svc, authSvc, q, ctx
}

// noopAuditRepo is a best-effort audit logger that discards all entries.
type noopAuditRepo struct{}

func (r *noopAuditRepo) Insert(_ context.Context, _ *domain.AuditEntry) error { return nil }
func (r *noopAuditRepo) List(_ context.Context, _ domain.AuditFilter) ([]domain.AuditEntry, int64, error) {
	return nil, 0, nil
}

func TestIngestion_SanitizeFilename(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "simple parquet name",
			input:    "orders.parquet",
			expected: "orders.parquet",
		},
		{
			name:     "adds parquet extension",
			input:    "orders",
			expected: "orders.parquet",
		},
		{
			name:     "strips path separators",
			input:    "../../etc/passwd",
			expected: "____etc_passwd.parquet",
		},
		{
			name:     "strips backslashes",
			input:    "data\\file.parquet",
			expected: "data_file.parquet",
		},
		{
			name:     "handles complex name",
			input:    "my data file (2024).parquet",
			expected: "my data file (2024).parquet",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := sanitizeFilename(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestIngestion_ClassifyDuckDBError(t *testing.T) {
	tests := []struct {
		name       string
		errMsg     string
		expectType string
	}{
		{
			name:       "table does not exist",
			errMsg:     "Table 'orders' does not exist",
			expectType: "NotFoundError",
		},
		{
			name:       "column type mismatch",
			errMsg:     "Column 'id' type mismatch: expected INTEGER, got VARCHAR",
			expectType: "ValidationError",
		},
		{
			name:       "column not found in table",
			errMsg:     "Column 'extra_col' not found in table",
			expectType: "ValidationError",
		},
		{
			name:       "no files found",
			errMsg:     "No files found at path s3://bucket/data/*.parquet",
			expectType: "ValidationError",
		},
		{
			name:       "could not read file",
			errMsg:     "Could not read file s3://bucket/data/bad.parquet",
			expectType: "ValidationError",
		},
		{
			name:       "unknown error",
			errMsg:     "some unexpected error",
			expectType: "ValidationError",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := classifyDuckDBError(&testError{msg: tt.errMsg})
			require.Error(t, err)

			switch tt.expectType {
			case "NotFoundError":
				var notFound *domain.NotFoundError
				require.ErrorAs(t, err, &notFound)
			case "ValidationError":
				var validationErr *domain.ValidationError
				require.ErrorAs(t, err, &validationErr)
			}
		})
	}
}

type testError struct {
	msg string
}

func (e *testError) Error() string { return e.msg }

func TestIngestion_CommitEmptyKeys(t *testing.T) {
	svc, _, q, ctx := setupIngestionTest(t)

	_, err := q.CreatePrincipal(ctx, dbstore.CreatePrincipalParams{ID: uuid.New().String(),
		Name: "writer", Type: "user", IsAdmin: 1,
	})
	require.NoError(t, err)

	ctx = domain.WithPrincipal(ctx, domain.ContextPrincipal{Name: "writer", Type: "user"})
	_, err = svc.CommitIngestion(ctx, "writer", "lake", "main", "titanic", []string{}, domain.IngestionOptions{})
	require.Error(t, err)

	var validationErr *domain.ValidationError
	require.ErrorAs(t, err, &validationErr)
	assert.Contains(t, err.Error(), "s3_keys must not be empty")
}

func TestIngestion_LoadEmptyPaths(t *testing.T) {
	svc, _, q, ctx := setupIngestionTest(t)

	_, err := q.CreatePrincipal(ctx, dbstore.CreatePrincipalParams{ID: uuid.New().String(),
		Name: "writer", Type: "user", IsAdmin: 1,
	})
	require.NoError(t, err)

	ctx = domain.WithPrincipal(ctx, domain.ContextPrincipal{Name: "writer", Type: "user"})
	_, err = svc.LoadExternalFiles(ctx, "writer", "lake", "main", "titanic", []string{}, domain.IngestionOptions{})
	require.Error(t, err)

	var validationErr *domain.ValidationError
	require.ErrorAs(t, err, &validationErr)
	assert.Contains(t, err.Error(), "paths must not be empty")
}

func TestIngestion_AccessDenied(t *testing.T) {
	svc, _, q, ctx := setupIngestionTest(t)

	// Create a user with NO grants
	_, err := q.CreatePrincipal(ctx, dbstore.CreatePrincipalParams{ID: uuid.New().String(),
		Name: "no_access", Type: "user", IsAdmin: 0,
	})
	require.NoError(t, err)

	ctx = domain.WithPrincipal(ctx, domain.ContextPrincipal{Name: "no_access", Type: "user"})

	t.Run("upload-url denied", func(t *testing.T) {
		_, err := svc.RequestUploadURL(ctx, "no_access", "lake", "main", "titanic", nil)
		require.Error(t, err)

		var accessDenied *domain.AccessDeniedError
		require.ErrorAs(t, err, &accessDenied)
	})

	t.Run("commit denied", func(t *testing.T) {
		_, err := svc.CommitIngestion(ctx, "no_access", "lake", "main", "titanic",
			[]string{"lake_data/main/titanic/uploads/test.parquet"},
			domain.IngestionOptions{})
		require.Error(t, err)

		var accessDenied *domain.AccessDeniedError
		require.ErrorAs(t, err, &accessDenied)
	})

	t.Run("load denied", func(t *testing.T) {
		_, err := svc.LoadExternalFiles(ctx, "no_access", "lake", "main", "titanic",
			[]string{"s3://bucket/data.parquet"},
			domain.IngestionOptions{})
		require.Error(t, err)

		var accessDenied *domain.AccessDeniedError
		require.ErrorAs(t, err, &accessDenied)
	})
}

func TestIngestion_TableNotFound(t *testing.T) {
	svc, _, q, ctx := setupIngestionTest(t)

	_, err := q.CreatePrincipal(ctx, dbstore.CreatePrincipalParams{ID: uuid.New().String(),
		Name: "writer", Type: "user", IsAdmin: 1,
	})
	require.NoError(t, err)

	ctx = domain.WithPrincipal(ctx, domain.ContextPrincipal{Name: "writer", Type: "user"})

	_, err = svc.RequestUploadURL(ctx, "writer", "lake", "main", "nonexistent_table", nil)
	require.Error(t, err)

	var notFoundErr *domain.NotFoundError
	require.ErrorAs(t, err, &notFoundErr)
}

func TestIngestion_AdminPassesAuthCheck(t *testing.T) {
	svc, _, q, ctx := setupIngestionTest(t)

	_, err := q.CreatePrincipal(ctx, dbstore.CreatePrincipalParams{ID: uuid.New().String(),
		Name: "admin", Type: "user", IsAdmin: 1,
	})
	require.NoError(t, err)

	ctx = domain.WithPrincipal(ctx, domain.ContextPrincipal{Name: "admin", Type: "user"})

	// Admin should pass the auth check for commit, but fail on validation (empty keys).
	// This verifies auth is not the blocker.
	_, err = svc.CommitIngestion(ctx, "admin", "lake", "main", "titanic", []string{}, domain.IngestionOptions{})
	require.Error(t, err)
	// Should be validation error, not access denied
	var validationErr *domain.ValidationError
	require.ErrorAs(t, err, &validationErr)
}

func TestIngestion_UserWithInsertGrant(t *testing.T) {
	svc, _, q, ctx := setupIngestionTest(t)

	user, err := q.CreatePrincipal(ctx, dbstore.CreatePrincipalParams{ID: uuid.New().String(),
		Name: "inserter", Type: "user", IsAdmin: 0,
	})
	require.NoError(t, err)

	// Grant USAGE on schema + INSERT on table
	_, err = q.GrantPrivilege(ctx, dbstore.GrantPrivilegeParams{
		ID: uuid.New().String(), PrincipalID: user.ID, PrincipalType: "user",
		SecurableType: domain.SecurableSchema, SecurableID: "0",
		Privilege: domain.PrivUsage,
	})
	require.NoError(t, err)
	_, err = q.GrantPrivilege(ctx, dbstore.GrantPrivilegeParams{
		ID: uuid.New().String(), PrincipalID: user.ID, PrincipalType: "user",
		SecurableType: domain.SecurableTable, SecurableID: "1", // titanic
		Privilege: domain.PrivInsert,
	})
	require.NoError(t, err)

	ctx = domain.WithPrincipal(ctx, domain.ContextPrincipal{Name: "inserter", Type: "user"})

	// Should pass auth, but fail on validation (empty keys) — NOT AccessDenied.
	_, err = svc.CommitIngestion(ctx, "inserter", "lake", "main", "titanic", []string{}, domain.IngestionOptions{})
	require.Error(t, err)
	var validationErr *domain.ValidationError
	require.ErrorAs(t, err, &validationErr, "user with INSERT should pass auth, fail on validation")
}
