package catalog

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"duck-demo/internal/domain"
	"duck-demo/internal/testutil"
)

// mockStatsRepo implements domain.TableStatisticsRepository for testing.
type mockStatsRepo struct {
	UpsertFn func(ctx context.Context, securableName string, stats *domain.TableStatistics) error
	GetFn    func(ctx context.Context, securableName string) (*domain.TableStatistics, error)
	DeleteFn func(ctx context.Context, securableName string) error
}

func (m *mockStatsRepo) Upsert(ctx context.Context, securableName string, stats *domain.TableStatistics) error {
	if m.UpsertFn != nil {
		return m.UpsertFn(ctx, securableName, stats)
	}
	return nil
}

func (m *mockStatsRepo) Get(ctx context.Context, securableName string) (*domain.TableStatistics, error) {
	if m.GetFn != nil {
		return m.GetFn(ctx, securableName)
	}
	return nil, nil
}

func (m *mockStatsRepo) Delete(ctx context.Context, securableName string) error {
	if m.DeleteFn != nil {
		return m.DeleteFn(ctx, securableName)
	}
	return nil
}

var _ domain.TableStatisticsRepository = (*mockStatsRepo)(nil)

// mockTagRepo aliases testutil.MockTagRepo for convenience in this file.
type mockTagRepo = testutil.MockTagRepo

// mockExternalLocationRepo aliases testutil.MockExternalLocationRepo for convenience.
type mockExternalLocationRepo = testutil.MockExternalLocationRepo

// newTestCatalogService is a helper to construct CatalogService with test mocks.
func newTestCatalogService(
	repo *mockCatalogRepo,
	auth *mockAuthService,
	audit *mockAuditRepo,
	tags *mockTagRepo,
	stats *mockStatsRepo,
	locations domain.ExternalLocationRepository,
) *CatalogService {
	return NewCatalogService(
		&mockCatalogRepoFactory{repo: repo},
		auth,
		audit,
		tags,
		stats,
		locations,
	)
}

// === CreateSchema ===

func TestCatalogService_CreateSchema(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		principal string
		req       domain.CreateSchemaRequest
		setupAuth func(*mockAuthService)
		setupRepo func(*mockCatalogRepo)
		setupLoc  func() domain.ExternalLocationRepository
		wantErr   bool
		errCheck  func(t *testing.T, err error)
		resCheck  func(t *testing.T, res *domain.SchemaDetail)
		auditChk  func(t *testing.T, audit *mockAuditRepo)
	}{
		{
			name:      "happy path creates schema and logs audit",
			principal: "alice",
			req:       domain.CreateSchemaRequest{Name: "analytics", Comment: "analytics schema"},
			setupAuth: func(auth *mockAuthService) {
				auth.CheckPrivilegeFn = func(_ context.Context, _, _ string, _ string, _ string) (bool, error) {
					return true, nil
				}
			},
			setupRepo: func(repo *mockCatalogRepo) {
				repo.CreateSchemaFn = func(_ context.Context, name, comment, owner string) (*domain.SchemaDetail, error) {
					return &domain.SchemaDetail{SchemaID: "1", Name: name, Comment: comment, Owner: owner}, nil
				}
			},
			wantErr: false,
			resCheck: func(t *testing.T, res *domain.SchemaDetail) {
				t.Helper()
				assert.Equal(t, "analytics", res.Name)
				assert.Equal(t, "alice", res.Owner)
			},
			auditChk: func(t *testing.T, audit *mockAuditRepo) {
				t.Helper()
				require.True(t, audit.HasAction("CREATE_SCHEMA"))
			},
		},
		{
			name:      "access denied returns 403",
			principal: "bob",
			req:       domain.CreateSchemaRequest{Name: "secret"},
			setupAuth: func(auth *mockAuthService) {
				auth.CheckPrivilegeFn = func(_ context.Context, _, _ string, _ string, _ string) (bool, error) {
					return false, nil
				}
			},
			wantErr: true,
			errCheck: func(t *testing.T, err error) {
				t.Helper()
				var ade *domain.AccessDeniedError
				require.ErrorAs(t, err, &ade)
			},
		},
		{
			name:      "privilege check error returns wrapped error",
			principal: "alice",
			req:       domain.CreateSchemaRequest{Name: "test"},
			setupAuth: func(auth *mockAuthService) {
				auth.CheckPrivilegeFn = func(_ context.Context, _, _ string, _ string, _ string) (bool, error) {
					return false, fmt.Errorf("db timeout")
				}
			},
			wantErr: true,
			errCheck: func(t *testing.T, err error) {
				t.Helper()
				assert.Contains(t, err.Error(), "check privilege")
				assert.Contains(t, err.Error(), "db timeout")
			},
		},
		{
			name:      "with location name validates location and sets storage path",
			principal: "alice",
			req:       domain.CreateSchemaRequest{Name: "ext_schema", LocationName: "my-s3-loc"},
			setupAuth: func(auth *mockAuthService) {
				auth.CheckPrivilegeFn = func(_ context.Context, _, _ string, _ string, _ string) (bool, error) {
					return true, nil
				}
			},
			setupRepo: func(repo *mockCatalogRepo) {
				repo.CreateSchemaFn = func(_ context.Context, name, _, _ string) (*domain.SchemaDetail, error) {
					return &domain.SchemaDetail{SchemaID: "99", Name: name}, nil
				}
				repo.SetSchemaStoragePathFn = func(_ context.Context, schemaID, path string) error {
					return nil
				}
			},
			setupLoc: func() domain.ExternalLocationRepository {
				return &mockExternalLocationRepo{
					GetByNameFn: func(_ context.Context, name string) (*domain.ExternalLocation, error) {
						return &domain.ExternalLocation{Name: name, URL: "s3://bucket/prefix/"}, nil
					},
				}
			},
			wantErr: false,
			resCheck: func(t *testing.T, res *domain.SchemaDetail) {
				t.Helper()
				assert.Equal(t, "ext_schema", res.Name)
			},
		},
		{
			name:      "location not found returns error",
			principal: "alice",
			req:       domain.CreateSchemaRequest{Name: "test", LocationName: "missing"},
			setupAuth: func(auth *mockAuthService) {
				auth.CheckPrivilegeFn = func(_ context.Context, _, _ string, _ string, _ string) (bool, error) {
					return true, nil
				}
			},
			setupLoc: func() domain.ExternalLocationRepository {
				return &mockExternalLocationRepo{
					GetByNameFn: func(_ context.Context, _ string) (*domain.ExternalLocation, error) {
						return nil, domain.ErrNotFound("location not found")
					},
				}
			},
			wantErr: true,
			errCheck: func(t *testing.T, err error) {
				t.Helper()
				assert.Contains(t, err.Error(), "lookup location")
			},
		},
		{
			name:      "repo error propagates",
			principal: "alice",
			req:       domain.CreateSchemaRequest{Name: "failing"},
			setupAuth: func(auth *mockAuthService) {
				auth.CheckPrivilegeFn = func(_ context.Context, _, _ string, _ string, _ string) (bool, error) {
					return true, nil
				}
			},
			setupRepo: func(repo *mockCatalogRepo) {
				repo.CreateSchemaFn = func(_ context.Context, _, _, _ string) (*domain.SchemaDetail, error) {
					return nil, fmt.Errorf("unique constraint violation")
				}
			},
			wantErr: true,
			errCheck: func(t *testing.T, err error) {
				t.Helper()
				assert.Contains(t, err.Error(), "unique constraint violation")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			auth := &mockAuthService{}
			if tt.setupAuth != nil {
				tt.setupAuth(auth)
			}

			repo := &mockCatalogRepo{}
			if tt.setupRepo != nil {
				tt.setupRepo(repo)
			}

			audit := &mockAuditRepo{}
			tags := &mockTagRepo{}

			var locations domain.ExternalLocationRepository
			if tt.setupLoc != nil {
				locations = tt.setupLoc()
			}

			svc := newTestCatalogService(repo, auth, audit, tags, &mockStatsRepo{}, locations)

			result, err := svc.CreateSchema(context.Background(), "lake", tt.principal, tt.req)

			if tt.wantErr {
				require.Error(t, err)
				if tt.errCheck != nil {
					tt.errCheck(t, err)
				}
				assert.Nil(t, result)
			} else {
				require.NoError(t, err)
				require.NotNil(t, result)
				if tt.resCheck != nil {
					tt.resCheck(t, result)
				}
			}
			if tt.auditChk != nil {
				tt.auditChk(t, audit)
			}
		})
	}
}

// === DeleteSchema ===

func TestCatalogService_DeleteSchema(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		principal string
		schema    string
		setupAuth func(*mockAuthService)
		setupRepo func(*mockCatalogRepo)
		wantErr   bool
		errCheck  func(t *testing.T, err error)
	}{
		{
			name:      "happy path deletes schema",
			principal: "alice",
			schema:    "old_schema",
			setupAuth: func(auth *mockAuthService) {
				auth.CheckPrivilegeFn = func(_ context.Context, _, _ string, _ string, _ string) (bool, error) {
					return true, nil
				}
			},
			setupRepo: func(repo *mockCatalogRepo) {
				repo.DeleteSchemaFn = func(_ context.Context, _ string, _ bool) error {
					return nil
				}
			},
			wantErr: false,
		},
		{
			name:      "access denied returns 403",
			principal: "bob",
			schema:    "protected",
			setupAuth: func(auth *mockAuthService) {
				auth.CheckPrivilegeFn = func(_ context.Context, _, _ string, _ string, _ string) (bool, error) {
					return false, nil
				}
			},
			wantErr: true,
			errCheck: func(t *testing.T, err error) {
				t.Helper()
				var ade *domain.AccessDeniedError
				require.ErrorAs(t, err, &ade)
			},
		},
		{
			name:      "not found from repo propagates",
			principal: "alice",
			schema:    "ghost",
			setupAuth: func(auth *mockAuthService) {
				auth.CheckPrivilegeFn = func(_ context.Context, _, _ string, _ string, _ string) (bool, error) {
					return true, nil
				}
			},
			setupRepo: func(repo *mockCatalogRepo) {
				repo.DeleteSchemaFn = func(_ context.Context, _ string, _ bool) error {
					return domain.ErrNotFound("schema not found")
				}
			},
			wantErr: true,
			errCheck: func(t *testing.T, err error) {
				t.Helper()
				var nfe *domain.NotFoundError
				require.ErrorAs(t, err, &nfe)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			auth := &mockAuthService{}
			if tt.setupAuth != nil {
				tt.setupAuth(auth)
			}
			repo := &mockCatalogRepo{}
			if tt.setupRepo != nil {
				tt.setupRepo(repo)
			}
			audit := &mockAuditRepo{}
			svc := newTestCatalogService(repo, auth, audit, &mockTagRepo{}, &mockStatsRepo{}, nil)

			err := svc.DeleteSchema(context.Background(), "lake", tt.principal, tt.schema, false)

			if tt.wantErr {
				require.Error(t, err)
				if tt.errCheck != nil {
					tt.errCheck(t, err)
				}
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// === CreateTable ===

func TestCatalogService_CreateTable(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		principal string
		schema    string
		req       domain.CreateTableRequest
		setupAuth func(*mockAuthService)
		setupRepo func(*mockCatalogRepo)
		setupLoc  func() domain.ExternalLocationRepository
		wantErr   bool
		errCheck  func(t *testing.T, err error)
		resCheck  func(t *testing.T, res *domain.TableDetail)
	}{
		{
			name:      "happy path managed table creates table",
			principal: "alice",
			schema:    "main",
			req:       domain.CreateTableRequest{Name: "events", TableType: domain.TableTypeManaged},
			setupAuth: func(auth *mockAuthService) {
				auth.CheckPrivilegeFn = func(_ context.Context, _, _ string, _ string, _ string) (bool, error) {
					return true, nil
				}
			},
			setupRepo: func(repo *mockCatalogRepo) {
				repo.CreateTableFn = func(_ context.Context, schemaName string, req domain.CreateTableRequest, owner string) (*domain.TableDetail, error) {
					return &domain.TableDetail{TableID: "1", Name: req.Name, SchemaName: schemaName, Owner: owner}, nil
				}
			},
			wantErr: false,
			resCheck: func(t *testing.T, res *domain.TableDetail) {
				t.Helper()
				assert.Equal(t, "events", res.Name)
				assert.Equal(t, "alice", res.Owner)
			},
		},
		{
			name:      "access denied returns 403",
			principal: "bob",
			schema:    "main",
			req:       domain.CreateTableRequest{Name: "events"},
			setupAuth: func(auth *mockAuthService) {
				auth.CheckPrivilegeFn = func(_ context.Context, _, _ string, _ string, _ string) (bool, error) {
					return false, nil
				}
			},
			wantErr: true,
			errCheck: func(t *testing.T, err error) {
				t.Helper()
				var ade *domain.AccessDeniedError
				require.ErrorAs(t, err, &ade)
			},
		},
		{
			name:      "external table validates location prefix",
			principal: "alice",
			schema:    "main",
			req: domain.CreateTableRequest{
				Name:         "ext_events",
				TableType:    domain.TableTypeExternal,
				SourcePath:   "s3://bucket/prefix/data/events.parquet",
				LocationName: "my-loc",
			},
			setupAuth: func(auth *mockAuthService) {
				auth.CheckPrivilegeFn = func(_ context.Context, _, _ string, _ string, _ string) (bool, error) {
					return true, nil
				}
			},
			setupRepo: func(repo *mockCatalogRepo) {
				repo.CreateExternalTableFn = func(_ context.Context, _ string, req domain.CreateTableRequest, _ string) (*domain.TableDetail, error) {
					return &domain.TableDetail{TableID: "2", Name: req.Name, TableType: domain.TableTypeExternal}, nil
				}
			},
			setupLoc: func() domain.ExternalLocationRepository {
				return &mockExternalLocationRepo{
					GetByNameFn: func(_ context.Context, _ string) (*domain.ExternalLocation, error) {
						return &domain.ExternalLocation{Name: "my-loc", URL: "s3://bucket/prefix/"}, nil
					},
				}
			},
			wantErr: false,
			resCheck: func(t *testing.T, res *domain.TableDetail) {
				t.Helper()
				assert.Equal(t, "ext_events", res.Name)
			},
		},
		{
			name:      "external table with invalid prefix returns validation error",
			principal: "alice",
			schema:    "main",
			req: domain.CreateTableRequest{
				Name:         "ext_events",
				TableType:    domain.TableTypeExternal,
				SourcePath:   "s3://other-bucket/data.parquet",
				LocationName: "my-loc",
			},
			setupAuth: func(auth *mockAuthService) {
				auth.CheckPrivilegeFn = func(_ context.Context, _, _ string, _ string, _ string) (bool, error) {
					return true, nil
				}
			},
			setupLoc: func() domain.ExternalLocationRepository {
				return &mockExternalLocationRepo{
					GetByNameFn: func(_ context.Context, _ string) (*domain.ExternalLocation, error) {
						return &domain.ExternalLocation{Name: "my-loc", URL: "s3://bucket/prefix/"}, nil
					},
				}
			},
			wantErr: true,
			errCheck: func(t *testing.T, err error) {
				t.Helper()
				var ve *domain.ValidationError
				require.ErrorAs(t, err, &ve)
				assert.Contains(t, ve.Message, "not under location")
			},
		},
		{
			name:      "unsupported table type returns validation error",
			principal: "alice",
			schema:    "main",
			req:       domain.CreateTableRequest{Name: "events", TableType: "MAGIC"},
			setupAuth: func(auth *mockAuthService) {
				auth.CheckPrivilegeFn = func(_ context.Context, _, _ string, _ string, _ string) (bool, error) {
					return true, nil
				}
			},
			wantErr: true,
			errCheck: func(t *testing.T, err error) {
				t.Helper()
				var ve *domain.ValidationError
				require.ErrorAs(t, err, &ve)
				assert.Contains(t, ve.Message, "unsupported table_type")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			auth := &mockAuthService{}
			if tt.setupAuth != nil {
				tt.setupAuth(auth)
			}
			repo := &mockCatalogRepo{}
			if tt.setupRepo != nil {
				tt.setupRepo(repo)
			}
			audit := &mockAuditRepo{}

			var locations domain.ExternalLocationRepository
			if tt.setupLoc != nil {
				locations = tt.setupLoc()
			}

			svc := newTestCatalogService(repo, auth, audit, &mockTagRepo{}, &mockStatsRepo{}, locations)

			result, err := svc.CreateTable(context.Background(), "lake", tt.principal, tt.schema, tt.req)

			if tt.wantErr {
				require.Error(t, err)
				if tt.errCheck != nil {
					tt.errCheck(t, err)
				}
			} else {
				require.NoError(t, err)
				require.NotNil(t, result)
				if tt.resCheck != nil {
					tt.resCheck(t, result)
				}
			}
		})
	}
}

// === UpdateTable ===

func TestCatalogService_UpdateTable(t *testing.T) {
	t.Parallel()

	comment := "updated"

	tests := []struct {
		name      string
		principal string
		setupAuth func(*mockAuthService)
		setupRepo func(*mockCatalogRepo)
		wantErr   bool
		errCheck  func(t *testing.T, err error)
		resCheck  func(t *testing.T, res *domain.TableDetail)
	}{
		{
			name:      "happy path updates table and enriches tags/stats",
			principal: "alice",
			setupAuth: func(auth *mockAuthService) {
				auth.CheckPrivilegeFn = func(_ context.Context, _, _ string, _ string, _ string) (bool, error) {
					return true, nil
				}
			},
			setupRepo: func(repo *mockCatalogRepo) {
				repo.UpdateTableFn = func(_ context.Context, schema, table string, _ *string, _ map[string]string, _ *string) (*domain.TableDetail, error) {
					return &domain.TableDetail{
						TableID:    "1",
						Name:       table,
						SchemaName: schema,
						Comment:    "updated",
					}, nil
				}
			},
			wantErr: false,
			resCheck: func(t *testing.T, res *domain.TableDetail) {
				t.Helper()
				assert.Equal(t, "updated", res.Comment)
			},
		},
		{
			name:      "access denied returns 403",
			principal: "bob",
			setupAuth: func(auth *mockAuthService) {
				auth.CheckPrivilegeFn = func(_ context.Context, _, _ string, _ string, _ string) (bool, error) {
					return false, nil
				}
			},
			wantErr: true,
			errCheck: func(t *testing.T, err error) {
				t.Helper()
				var ade *domain.AccessDeniedError
				require.ErrorAs(t, err, &ade)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			auth := &mockAuthService{}
			if tt.setupAuth != nil {
				tt.setupAuth(auth)
			}
			repo := &mockCatalogRepo{}
			if tt.setupRepo != nil {
				tt.setupRepo(repo)
			}
			audit := &mockAuditRepo{}
			tags := &mockTagRepo{
				ListTagsForSecurableFn: func(_ context.Context, _ string, _ string, _ *string) ([]domain.Tag, error) {
					return nil, nil
				},
			}
			svc := newTestCatalogService(repo, auth, audit, tags, &mockStatsRepo{}, nil)

			result, err := svc.UpdateTable(context.Background(), "lake", tt.principal, "main", "events", domain.UpdateTableRequest{Comment: &comment})

			if tt.wantErr {
				require.Error(t, err)
				if tt.errCheck != nil {
					tt.errCheck(t, err)
				}
			} else {
				require.NoError(t, err)
				require.NotNil(t, result)
				if tt.resCheck != nil {
					tt.resCheck(t, result)
				}
			}
		})
	}
}

// === DeleteTable ===

func TestCatalogService_DeleteTable(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		principal string
		setupAuth func(*mockAuthService)
		setupRepo func(*mockCatalogRepo)
		wantErr   bool
		errCheck  func(t *testing.T, err error)
	}{
		{
			name:      "happy path deletes table",
			principal: "alice",
			setupAuth: func(auth *mockAuthService) {
				auth.CheckPrivilegeFn = func(_ context.Context, _, _ string, _ string, _ string) (bool, error) {
					return true, nil
				}
			},
			setupRepo: func(repo *mockCatalogRepo) {
				repo.DeleteTableFn = func(_ context.Context, _, _ string) error {
					return nil
				}
			},
			wantErr: false,
		},
		{
			name:      "access denied returns 403",
			principal: "bob",
			setupAuth: func(auth *mockAuthService) {
				auth.CheckPrivilegeFn = func(_ context.Context, _, _ string, _ string, _ string) (bool, error) {
					return false, nil
				}
			},
			wantErr: true,
			errCheck: func(t *testing.T, err error) {
				t.Helper()
				var ade *domain.AccessDeniedError
				require.ErrorAs(t, err, &ade)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			auth := &mockAuthService{}
			if tt.setupAuth != nil {
				tt.setupAuth(auth)
			}
			repo := &mockCatalogRepo{}
			if tt.setupRepo != nil {
				tt.setupRepo(repo)
			}
			audit := &mockAuditRepo{}
			svc := newTestCatalogService(repo, auth, audit, &mockTagRepo{}, &mockStatsRepo{}, nil)

			err := svc.DeleteTable(context.Background(), "lake", tt.principal, "main", "events")

			if tt.wantErr {
				require.Error(t, err)
				if tt.errCheck != nil {
					tt.errCheck(t, err)
				}
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// === ProfileTable ===

func TestCatalogService_ProfileTable(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		principal string
		setupAuth func(*mockAuthService)
		setupRepo func(*mockCatalogRepo)
		stats     *mockStatsRepo
		wantErr   bool
		errCheck  func(t *testing.T, err error)
		resCheck  func(t *testing.T, res *domain.TableStatistics)
	}{
		{
			name:      "happy path profiles table",
			principal: "alice",
			setupAuth: func(auth *mockAuthService) {
				auth.CheckPrivilegeFn = func(_ context.Context, _, _ string, _ string, _ string) (bool, error) {
					return true, nil
				}
			},
			setupRepo: func(repo *mockCatalogRepo) {
				repo.GetTableFn = func(_ context.Context, _, _ string) (*domain.TableDetail, error) {
					return &domain.TableDetail{
						TableID:    "1",
						Name:       "events",
						SchemaName: "main",
						Columns:    []domain.ColumnDetail{{Name: "id"}, {Name: "name"}},
					}, nil
				}
			},
			stats: &mockStatsRepo{
				UpsertFn: func(_ context.Context, _ string, _ *domain.TableStatistics) error {
					return nil
				},
				GetFn: func(_ context.Context, _ string) (*domain.TableStatistics, error) {
					colCount := int64(2)
					return &domain.TableStatistics{ColumnCount: &colCount, ProfiledBy: "alice"}, nil
				},
			},
			wantErr: false,
			resCheck: func(t *testing.T, res *domain.TableStatistics) {
				t.Helper()
				require.NotNil(t, res.ColumnCount)
				assert.Equal(t, int64(2), *res.ColumnCount)
				assert.Equal(t, "alice", res.ProfiledBy)
			},
		},
		{
			name:      "access denied returns 403",
			principal: "bob",
			setupAuth: func(auth *mockAuthService) {
				auth.CheckPrivilegeFn = func(_ context.Context, _, _ string, _ string, _ string) (bool, error) {
					return false, nil
				}
			},
			setupRepo: func(repo *mockCatalogRepo) {
				repo.GetTableFn = func(_ context.Context, _, _ string) (*domain.TableDetail, error) {
					return &domain.TableDetail{TableID: "1", Name: "events", SchemaName: "main"}, nil
				}
			},
			stats:   &mockStatsRepo{},
			wantErr: true,
			errCheck: func(t *testing.T, err error) {
				t.Helper()
				var ade *domain.AccessDeniedError
				require.ErrorAs(t, err, &ade)
			},
		},
		{
			name:      "table not found from repo propagates",
			principal: "alice",
			setupRepo: func(repo *mockCatalogRepo) {
				repo.GetTableFn = func(_ context.Context, _, _ string) (*domain.TableDetail, error) {
					return nil, domain.ErrNotFound("table not found")
				}
			},
			stats:   &mockStatsRepo{},
			wantErr: true,
			errCheck: func(t *testing.T, err error) {
				t.Helper()
				var nfe *domain.NotFoundError
				require.ErrorAs(t, err, &nfe)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			auth := &mockAuthService{}
			if tt.setupAuth != nil {
				tt.setupAuth(auth)
			}
			repo := &mockCatalogRepo{}
			if tt.setupRepo != nil {
				tt.setupRepo(repo)
			}
			audit := &mockAuditRepo{}
			svc := newTestCatalogService(repo, auth, audit, &mockTagRepo{}, tt.stats, nil)

			result, err := svc.ProfileTable(context.Background(), "lake", tt.principal, "main", "events")

			if tt.wantErr {
				require.Error(t, err)
				if tt.errCheck != nil {
					tt.errCheck(t, err)
				}
			} else {
				require.NoError(t, err)
				require.NotNil(t, result)
				if tt.resCheck != nil {
					tt.resCheck(t, result)
				}
			}
		})
	}
}

// === ListSchemas ===

func TestCatalogService_ListSchemas(t *testing.T) {
	t.Parallel()

	t.Run("delegates to repo and enriches with tags", func(t *testing.T) {
		t.Parallel()

		repo := &mockCatalogRepo{
			ListSchemasFn: func(_ context.Context, _ domain.PageRequest) ([]domain.SchemaDetail, int64, error) {
				return []domain.SchemaDetail{
					{SchemaID: "1", Name: "main"},
					{SchemaID: "2", Name: "analytics"},
				}, 2, nil
			},
		}
		tags := &mockTagRepo{
			ListTagsForSecurableFn: func(_ context.Context, _ string, _ string, _ *string) ([]domain.Tag, error) {
				return []domain.Tag{{Key: "pii"}}, nil
			},
		}
		svc := newTestCatalogService(repo, &mockAuthService{}, &mockAuditRepo{}, tags, &mockStatsRepo{}, nil)

		schemas, total, err := svc.ListSchemas(context.Background(), "lake", domain.PageRequest{MaxResults: 100})

		require.NoError(t, err)
		assert.Equal(t, int64(2), total)
		require.Len(t, schemas, 2)
		// Tags were enriched
		assert.Len(t, schemas[0].Tags, 1)
		assert.Equal(t, "pii", schemas[0].Tags[0].Key)
	})
}

// === GetSchema ===

func TestCatalogService_GetSchema(t *testing.T) {
	t.Parallel()

	t.Run("delegates to repo and enriches with tags", func(t *testing.T) {
		t.Parallel()

		repo := &mockCatalogRepo{
			GetSchemaFn: func(_ context.Context, name string) (*domain.SchemaDetail, error) {
				return &domain.SchemaDetail{SchemaID: "1", Name: name}, nil
			},
		}
		tags := &mockTagRepo{
			ListTagsForSecurableFn: func(_ context.Context, _ string, _ string, _ *string) ([]domain.Tag, error) {
				return []domain.Tag{{Key: "team:data"}}, nil
			},
		}
		svc := newTestCatalogService(repo, &mockAuthService{}, &mockAuditRepo{}, tags, &mockStatsRepo{}, nil)

		result, err := svc.GetSchema(context.Background(), "lake", "main")

		require.NoError(t, err)
		assert.Equal(t, "main", result.Name)
		require.Len(t, result.Tags, 1)
	})
}

// === ListTables ===

func TestCatalogService_ListTables(t *testing.T) {
	t.Parallel()

	t.Run("delegates to repo and enriches with tags and stats", func(t *testing.T) {
		t.Parallel()

		repo := &mockCatalogRepo{
			ListTablesFn: func(_ context.Context, _ string, _ domain.PageRequest) ([]domain.TableDetail, int64, error) {
				return []domain.TableDetail{
					{TableID: "1", Name: "events", SchemaName: "main"},
				}, 1, nil
			},
		}
		tags := &mockTagRepo{
			ListTagsForSecurableFn: func(_ context.Context, _ string, _ string, _ *string) ([]domain.Tag, error) {
				return []domain.Tag{{Key: "pii"}}, nil
			},
		}
		rowCount := int64(1000)
		stats := &mockStatsRepo{
			GetFn: func(_ context.Context, _ string) (*domain.TableStatistics, error) {
				return &domain.TableStatistics{RowCount: &rowCount}, nil
			},
		}
		svc := newTestCatalogService(repo, &mockAuthService{}, &mockAuditRepo{}, tags, stats, nil)

		tables, total, err := svc.ListTables(context.Background(), "lake", "main", domain.PageRequest{MaxResults: 100})

		require.NoError(t, err)
		assert.Equal(t, int64(1), total)
		require.Len(t, tables, 1)
		assert.Len(t, tables[0].Tags, 1)
		require.NotNil(t, tables[0].Statistics)
		assert.Equal(t, int64(1000), *tables[0].Statistics.RowCount)
	})
}

// === GetTable ===

func TestCatalogService_GetTable(t *testing.T) {
	t.Parallel()

	t.Run("delegates to repo and enriches with tags and stats", func(t *testing.T) {
		t.Parallel()

		repo := &mockCatalogRepo{
			GetTableFn: func(_ context.Context, _, tableName string) (*domain.TableDetail, error) {
				return &domain.TableDetail{TableID: "1", Name: tableName, SchemaName: "main"}, nil
			},
		}
		tags := &mockTagRepo{
			ListTagsForSecurableFn: func(_ context.Context, _ string, _ string, _ *string) ([]domain.Tag, error) {
				return []domain.Tag{{Key: "confidential"}}, nil
			},
		}
		svc := newTestCatalogService(repo, &mockAuthService{}, &mockAuditRepo{}, tags, &mockStatsRepo{}, nil)

		result, err := svc.GetTable(context.Background(), "lake", "main", "events")

		require.NoError(t, err)
		assert.Equal(t, "events", result.Name)
		require.Len(t, result.Tags, 1)
		assert.Equal(t, "confidential", result.Tags[0].Key)
	})
}

// === ListColumns ===

func TestCatalogService_ListColumns(t *testing.T) {
	t.Parallel()

	t.Run("delegates to repo and returns result", func(t *testing.T) {
		t.Parallel()

		repo := &mockCatalogRepo{
			ListColumnsFn: func(_ context.Context, _, _ string, _ domain.PageRequest) ([]domain.ColumnDetail, int64, error) {
				return []domain.ColumnDetail{
					{Name: "id", Type: "INTEGER"},
					{Name: "name", Type: "VARCHAR"},
				}, 2, nil
			},
		}
		svc := newTestCatalogService(repo, &mockAuthService{}, &mockAuditRepo{}, &mockTagRepo{}, &mockStatsRepo{}, nil)

		cols, total, err := svc.ListColumns(context.Background(), "lake", "main", "events", domain.PageRequest{MaxResults: 100})

		require.NoError(t, err)
		assert.Equal(t, int64(2), total)
		require.Len(t, cols, 2)
		assert.Equal(t, "id", cols[0].Name)
	})
}
