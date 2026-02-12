package service

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"duck-demo/internal/db/repository"
	"duck-demo/internal/domain"
	"duck-demo/internal/middleware"

	dbstore "duck-demo/internal/db/dbstore"
)

// setupIngestionTest creates an IngestionService backed by a real SQLite DB
// (for authorization) but with nil presigner and duckDB (since we can't run
// ducklake_add_data_files in unit tests). Returns the service and test helpers.
func setupIngestionTest(t *testing.T) (*IngestionService, *AuthorizationService, *dbstore.Queries, context.Context) {
	t.Helper()
	authSvc, q, ctx := setupTestService(t)
	auditRepo := repository.NewAuditRepo(nil) // audit is best-effort, nil DB will silently fail

	svc := &IngestionService{
		duckDB:      nil, // CALL statements can't be tested in unit tests
		metaDB:      nil, // metaDB only needed for readDataPath
		authSvc:     authSvc,
		presigner:   nil, // presigner requires real S3 credentials
		auditRepo:   auditRepo,
		catalogName: "lake",
		bucket:      "test-bucket",
	}
	return svc, authSvc, q, ctx
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

	_, err := q.CreatePrincipal(ctx, dbstore.CreatePrincipalParams{
		Name: "writer", Type: "user", IsAdmin: 1,
	})
	require.NoError(t, err)

	ctx = middleware.WithPrincipal(ctx, "writer")
	_, err = svc.CommitIngestion(ctx, "writer", "main", "titanic", []string{}, domain.IngestionOptions{})
	require.Error(t, err)

	var validationErr *domain.ValidationError
	require.ErrorAs(t, err, &validationErr)
	assert.Contains(t, err.Error(), "s3_keys must not be empty")
}

func TestIngestion_LoadEmptyPaths(t *testing.T) {
	svc, _, q, ctx := setupIngestionTest(t)

	_, err := q.CreatePrincipal(ctx, dbstore.CreatePrincipalParams{
		Name: "writer", Type: "user", IsAdmin: 1,
	})
	require.NoError(t, err)

	ctx = middleware.WithPrincipal(ctx, "writer")
	_, err = svc.LoadExternalFiles(ctx, "writer", "main", "titanic", []string{}, domain.IngestionOptions{})
	require.Error(t, err)

	var validationErr *domain.ValidationError
	require.ErrorAs(t, err, &validationErr)
	assert.Contains(t, err.Error(), "paths must not be empty")
}

func TestIngestion_AccessDenied(t *testing.T) {
	svc, _, q, ctx := setupIngestionTest(t)

	// Create a user with NO grants
	_, err := q.CreatePrincipal(ctx, dbstore.CreatePrincipalParams{
		Name: "no_access", Type: "user", IsAdmin: 0,
	})
	require.NoError(t, err)

	ctx = middleware.WithPrincipal(ctx, "no_access")

	t.Run("upload-url denied", func(t *testing.T) {
		_, err := svc.RequestUploadURL(ctx, "no_access", "main", "titanic", nil)
		require.Error(t, err)

		var accessDenied *domain.AccessDeniedError
		require.ErrorAs(t, err, &accessDenied)
	})

	t.Run("commit denied", func(t *testing.T) {
		_, err := svc.CommitIngestion(ctx, "no_access", "main", "titanic",
			[]string{"lake_data/main/titanic/uploads/test.parquet"},
			domain.IngestionOptions{})
		require.Error(t, err)

		var accessDenied *domain.AccessDeniedError
		require.ErrorAs(t, err, &accessDenied)
	})

	t.Run("load denied", func(t *testing.T) {
		_, err := svc.LoadExternalFiles(ctx, "no_access", "main", "titanic",
			[]string{"s3://bucket/data.parquet"},
			domain.IngestionOptions{})
		require.Error(t, err)

		var accessDenied *domain.AccessDeniedError
		require.ErrorAs(t, err, &accessDenied)
	})
}

func TestIngestion_TableNotFound(t *testing.T) {
	svc, _, q, ctx := setupIngestionTest(t)

	_, err := q.CreatePrincipal(ctx, dbstore.CreatePrincipalParams{
		Name: "writer", Type: "user", IsAdmin: 1,
	})
	require.NoError(t, err)

	ctx = middleware.WithPrincipal(ctx, "writer")

	_, err = svc.RequestUploadURL(ctx, "writer", "main", "nonexistent_table", nil)
	require.Error(t, err)

	var notFoundErr *domain.NotFoundError
	require.ErrorAs(t, err, &notFoundErr)
}

func TestIngestion_AdminPassesAuthCheck(t *testing.T) {
	svc, _, q, ctx := setupIngestionTest(t)

	_, err := q.CreatePrincipal(ctx, dbstore.CreatePrincipalParams{
		Name: "admin", Type: "user", IsAdmin: 1,
	})
	require.NoError(t, err)

	ctx = middleware.WithPrincipal(ctx, "admin")

	// Admin should pass the auth check for commit, but fail on validation (empty keys).
	// This verifies auth is not the blocker.
	_, err = svc.CommitIngestion(ctx, "admin", "main", "titanic", []string{}, domain.IngestionOptions{})
	require.Error(t, err)
	// Should be validation error, not access denied
	var validationErr *domain.ValidationError
	require.ErrorAs(t, err, &validationErr)
}

func TestIngestion_UserWithInsertGrant(t *testing.T) {
	svc, _, q, ctx := setupIngestionTest(t)

	user, err := q.CreatePrincipal(ctx, dbstore.CreatePrincipalParams{
		Name: "inserter", Type: "user", IsAdmin: 0,
	})
	require.NoError(t, err)

	// Grant USAGE on schema + INSERT on table
	_, err = q.GrantPrivilege(ctx, dbstore.GrantPrivilegeParams{
		PrincipalID: user.ID, PrincipalType: "user",
		SecurableType: SecurableSchema, SecurableID: 0,
		Privilege: PrivUsage,
	})
	require.NoError(t, err)
	_, err = q.GrantPrivilege(ctx, dbstore.GrantPrivilegeParams{
		PrincipalID: user.ID, PrincipalType: "user",
		SecurableType: SecurableTable, SecurableID: 1, // titanic
		Privilege: PrivInsert,
	})
	require.NoError(t, err)

	ctx = middleware.WithPrincipal(ctx, "inserter")

	// Should pass auth, but fail on validation (empty keys) — NOT AccessDenied.
	_, err = svc.CommitIngestion(ctx, "inserter", "main", "titanic", []string{}, domain.IngestionOptions{})
	require.Error(t, err)
	var validationErr *domain.ValidationError
	require.ErrorAs(t, err, &validationErr, "user with INSERT should pass auth, fail on validation")
}
