package query

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"duck-demo/internal/domain"
	"duck-demo/internal/testutil"
)

// === Local mock types for manifest tests ===

type mockMetastoreQuerierFactory struct {
	ForCatalogFn func(ctx context.Context, catalogName string) (domain.MetastoreQuerier, error)
}

func (m *mockMetastoreQuerierFactory) ForCatalog(ctx context.Context, catalogName string) (domain.MetastoreQuerier, error) {
	if m.ForCatalogFn != nil {
		return m.ForCatalogFn(ctx, catalogName)
	}
	panic("unexpected call to mockMetastoreQuerierFactory.ForCatalog")
}

func (m *mockMetastoreQuerierFactory) Close(_ string) error { return nil }

type mockMetastoreQuerier struct {
	ReadDataPathFn   func(ctx context.Context) (string, error)
	ReadSchemaPathFn func(ctx context.Context, schemaName string) (string, error)
	ListDataFilesFn  func(ctx context.Context, tableID string) ([]string, []bool, error)
}

func (m *mockMetastoreQuerier) ReadDataPath(ctx context.Context) (string, error) {
	if m.ReadDataPathFn != nil {
		return m.ReadDataPathFn(ctx)
	}
	panic("unexpected call to mockMetastoreQuerier.ReadDataPath")
}

func (m *mockMetastoreQuerier) ReadSchemaPath(ctx context.Context, schemaName string) (string, error) {
	if m.ReadSchemaPathFn != nil {
		return m.ReadSchemaPathFn(ctx, schemaName)
	}
	return "", nil
}

func (m *mockMetastoreQuerier) ListDataFiles(ctx context.Context, tableID string) ([]string, []bool, error) {
	if m.ListDataFilesFn != nil {
		return m.ListDataFilesFn(ctx, tableID)
	}
	panic("unexpected call to mockMetastoreQuerier.ListDataFiles")
}

type mockPresigner struct {
	PresignGetObjectFn func(ctx context.Context, path string, expiry time.Duration) (string, error)
}

func (m *mockPresigner) PresignGetObject(ctx context.Context, path string, expiry time.Duration) (string, error) {
	if m.PresignGetObjectFn != nil {
		return m.PresignGetObjectFn(ctx, path, expiry)
	}
	panic("unexpected call to mockPresigner.PresignGetObject")
}

var _ FilePresigner = (*mockPresigner)(nil)

// newManifestService is a test helper that builds a ManifestService with the given mocks.
func newManifestService(
	msFactory domain.MetastoreQuerierFactory,
	auth *testutil.MockAuthService,
	presigner FilePresigner,
	intro *testutil.MockIntrospectionRepo,
	audit *testutil.MockAuditRepo,
	cred *testutil.MockStorageCredentialRepo,
	loc *testutil.MockExternalLocationRepo,
) *ManifestService {
	return NewManifestService(msFactory, auth, presigner, intro, audit, cred, loc)
}

// === GetManifest ===

func TestManifestService_GetManifest(t *testing.T) {
	t.Parallel()

	const (
		principal = "alice"
		catalog   = "lake"
		schema    = "main"
		table     = "events"
		tableID   = "42"
		schemaID  = "10"
	)

	tests := []struct {
		name       string
		setupAuth  func(*testutil.MockAuthService)
		setupIntro func(*testutil.MockIntrospectionRepo)
		setupMS    func(*mockMetastoreQuerierFactory)
		setupSign  func(*mockPresigner)
		wantErr    bool
		errCheck   func(t *testing.T, err error)
		resCheck   func(t *testing.T, res *ManifestResult)
	}{
		{
			name: "table not found returns not found error",
			setupAuth: func(auth *testutil.MockAuthService) {
				auth.LookupTableIDFn = func(_ context.Context, _ string) (string, string, bool, error) {
					return "", "", false, domain.ErrNotFound("table not found")
				}
			},
			wantErr: true,
			errCheck: func(t *testing.T, err error) {
				t.Helper()
				var nfe *domain.NotFoundError
				require.ErrorAs(t, err, &nfe)
			},
		},
		{
			name: "access denied returns access denied error",
			setupAuth: func(auth *testutil.MockAuthService) {
				auth.LookupTableIDFn = func(_ context.Context, _ string) (string, string, bool, error) {
					return tableID, schemaID, false, nil
				}
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
			name: "privilege check error returns wrapped error",
			setupAuth: func(auth *testutil.MockAuthService) {
				auth.LookupTableIDFn = func(_ context.Context, _ string) (string, string, bool, error) {
					return tableID, schemaID, false, nil
				}
				auth.CheckPrivilegeFn = func(_ context.Context, _, _ string, _ string, _ string) (bool, error) {
					return false, fmt.Errorf("db connection lost")
				}
			},
			wantErr: true,
			errCheck: func(t *testing.T, err error) {
				t.Helper()
				assert.Contains(t, err.Error(), "privilege check")
				assert.Contains(t, err.Error(), "db connection lost")
			},
		},
		{
			name: "successful manifest with no filters or masks",
			setupAuth: func(auth *testutil.MockAuthService) {
				auth.LookupTableIDFn = func(_ context.Context, _ string) (string, string, bool, error) {
					return tableID, schemaID, false, nil
				}
				auth.CheckPrivilegeFn = func(_ context.Context, _, _ string, _ string, _ string) (bool, error) {
					return true, nil
				}
				auth.GetEffectiveRowFiltersFn = func(_ context.Context, _ string, _ string) ([]string, error) {
					return nil, nil
				}
				auth.GetEffectiveColumnMasksFn = func(_ context.Context, _ string, _ string) (map[string]string, error) {
					return nil, nil
				}
			},
			setupIntro: func(intro *testutil.MockIntrospectionRepo) {
				intro.ListColumnsFn = func(_ context.Context, _ string, _ domain.PageRequest) ([]domain.Column, int64, error) {
					return []domain.Column{
						{Name: "id", Type: "INTEGER"},
						{Name: "name", Type: "VARCHAR"},
					}, 2, nil
				}
			},
			setupMS: func(msf *mockMetastoreQuerierFactory) {
				msf.ForCatalogFn = func(_ context.Context, _ string) (domain.MetastoreQuerier, error) {
					return &mockMetastoreQuerier{
						ReadDataPathFn: func(_ context.Context) (string, error) {
							return "s3://bucket/data/", nil
						},
						ListDataFilesFn: func(_ context.Context, _ string) ([]string, []bool, error) {
							return []string{"events/part-0001.parquet"}, []bool{true}, nil
						},
					}, nil
				}
			},
			setupSign: func(p *mockPresigner) {
				p.PresignGetObjectFn = func(_ context.Context, path string, _ time.Duration) (string, error) {
					return "https://signed.example.com/" + path, nil
				}
			},
			wantErr: false,
			resCheck: func(t *testing.T, res *ManifestResult) {
				t.Helper()
				assert.Equal(t, table, res.Table)
				assert.Equal(t, schema, res.Schema)
				require.Len(t, res.Columns, 2)
				assert.Equal(t, "id", res.Columns[0].Name)
				assert.Equal(t, "INTEGER", res.Columns[0].Type)
				require.Len(t, res.Files, 1)
				assert.Contains(t, res.Files[0], "signed.example.com")
				assert.Empty(t, res.RowFilters)
				assert.Empty(t, res.ColumnMasks)
				assert.False(t, res.ExpiresAt.IsZero())
			},
		},
		{
			name: "successful manifest with row filters OR-combined",
			setupAuth: func(auth *testutil.MockAuthService) {
				auth.LookupTableIDFn = func(_ context.Context, _ string) (string, string, bool, error) {
					return tableID, schemaID, false, nil
				}
				auth.CheckPrivilegeFn = func(_ context.Context, _, _ string, _ string, _ string) (bool, error) {
					return true, nil
				}
				auth.GetEffectiveRowFiltersFn = func(_ context.Context, _ string, _ string) ([]string, error) {
					return []string{"region = 'US'", "region = 'EU'"}, nil
				}
				auth.GetEffectiveColumnMasksFn = func(_ context.Context, _ string, _ string) (map[string]string, error) {
					return nil, nil
				}
			},
			setupIntro: func(intro *testutil.MockIntrospectionRepo) {
				intro.ListColumnsFn = func(_ context.Context, _ string, _ domain.PageRequest) ([]domain.Column, int64, error) {
					return []domain.Column{{Name: "id", Type: "INTEGER"}}, 1, nil
				}
			},
			setupMS: func(msf *mockMetastoreQuerierFactory) {
				msf.ForCatalogFn = func(_ context.Context, _ string) (domain.MetastoreQuerier, error) {
					return &mockMetastoreQuerier{
						ReadDataPathFn: func(_ context.Context) (string, error) {
							return "s3://bucket/data/", nil
						},
						ListDataFilesFn: func(_ context.Context, _ string) ([]string, []bool, error) {
							return []string{"file.parquet"}, []bool{true}, nil
						},
					}, nil
				}
			},
			setupSign: func(p *mockPresigner) {
				p.PresignGetObjectFn = func(_ context.Context, _ string, _ time.Duration) (string, error) {
					return "https://signed.example.com/file", nil
				}
			},
			wantErr: false,
			resCheck: func(t *testing.T, res *ManifestResult) {
				t.Helper()
				require.Len(t, res.RowFilters, 1)
				assert.Contains(t, res.RowFilters[0], "OR")
				assert.Contains(t, res.RowFilters[0], "region = 'US'")
				assert.Contains(t, res.RowFilters[0], "region = 'EU'")
			},
		},
		{
			name: "successful manifest with column masks",
			setupAuth: func(auth *testutil.MockAuthService) {
				auth.LookupTableIDFn = func(_ context.Context, _ string) (string, string, bool, error) {
					return tableID, schemaID, false, nil
				}
				auth.CheckPrivilegeFn = func(_ context.Context, _, _ string, _ string, _ string) (bool, error) {
					return true, nil
				}
				auth.GetEffectiveRowFiltersFn = func(_ context.Context, _ string, _ string) ([]string, error) {
					return nil, nil
				}
				auth.GetEffectiveColumnMasksFn = func(_ context.Context, _ string, _ string) (map[string]string, error) {
					return map[string]string{
						"email": "'***'",
						"phone": "NULL",
					}, nil
				}
			},
			setupIntro: func(intro *testutil.MockIntrospectionRepo) {
				intro.ListColumnsFn = func(_ context.Context, _ string, _ domain.PageRequest) ([]domain.Column, int64, error) {
					return []domain.Column{{Name: "id", Type: "INTEGER"}}, 1, nil
				}
			},
			setupMS: func(msf *mockMetastoreQuerierFactory) {
				msf.ForCatalogFn = func(_ context.Context, _ string) (domain.MetastoreQuerier, error) {
					return &mockMetastoreQuerier{
						ReadDataPathFn: func(_ context.Context) (string, error) {
							return "s3://bucket/data/", nil
						},
						ListDataFilesFn: func(_ context.Context, _ string) ([]string, []bool, error) {
							return []string{"f.parquet"}, []bool{true}, nil
						},
					}, nil
				}
			},
			setupSign: func(p *mockPresigner) {
				p.PresignGetObjectFn = func(_ context.Context, _ string, _ time.Duration) (string, error) {
					return "https://signed.example.com/f", nil
				}
			},
			wantErr: false,
			resCheck: func(t *testing.T, res *ManifestResult) {
				t.Helper()
				require.Len(t, res.ColumnMasks, 2)
				assert.Equal(t, "'***'", res.ColumnMasks["email"])
				assert.Equal(t, "NULL", res.ColumnMasks["phone"])
			},
		},
		{
			name: "metastore error propagates",
			setupAuth: func(auth *testutil.MockAuthService) {
				auth.LookupTableIDFn = func(_ context.Context, _ string) (string, string, bool, error) {
					return tableID, schemaID, false, nil
				}
				auth.CheckPrivilegeFn = func(_ context.Context, _, _ string, _ string, _ string) (bool, error) {
					return true, nil
				}
				auth.GetEffectiveRowFiltersFn = func(_ context.Context, _ string, _ string) ([]string, error) {
					return nil, nil
				}
				auth.GetEffectiveColumnMasksFn = func(_ context.Context, _ string, _ string) (map[string]string, error) {
					return nil, nil
				}
			},
			setupIntro: func(intro *testutil.MockIntrospectionRepo) {
				intro.ListColumnsFn = func(_ context.Context, _ string, _ domain.PageRequest) ([]domain.Column, int64, error) {
					return []domain.Column{{Name: "id", Type: "INTEGER"}}, 1, nil
				}
			},
			setupMS: func(msf *mockMetastoreQuerierFactory) {
				msf.ForCatalogFn = func(_ context.Context, _ string) (domain.MetastoreQuerier, error) {
					return nil, fmt.Errorf("metastore unavailable")
				}
			},
			wantErr: true,
			errCheck: func(t *testing.T, err error) {
				t.Helper()
				assert.Contains(t, err.Error(), "resolve files")
				assert.Contains(t, err.Error(), "metastore unavailable")
			},
		},
		{
			name: "presign error propagates",
			setupAuth: func(auth *testutil.MockAuthService) {
				auth.LookupTableIDFn = func(_ context.Context, _ string) (string, string, bool, error) {
					return tableID, schemaID, false, nil
				}
				auth.CheckPrivilegeFn = func(_ context.Context, _, _ string, _ string, _ string) (bool, error) {
					return true, nil
				}
				auth.GetEffectiveRowFiltersFn = func(_ context.Context, _ string, _ string) ([]string, error) {
					return nil, nil
				}
				auth.GetEffectiveColumnMasksFn = func(_ context.Context, _ string, _ string) (map[string]string, error) {
					return nil, nil
				}
			},
			setupIntro: func(intro *testutil.MockIntrospectionRepo) {
				intro.ListColumnsFn = func(_ context.Context, _ string, _ domain.PageRequest) ([]domain.Column, int64, error) {
					return []domain.Column{{Name: "id", Type: "INTEGER"}}, 1, nil
				}
			},
			setupMS: func(msf *mockMetastoreQuerierFactory) {
				msf.ForCatalogFn = func(_ context.Context, _ string) (domain.MetastoreQuerier, error) {
					return &mockMetastoreQuerier{
						ReadDataPathFn: func(_ context.Context) (string, error) {
							return "s3://bucket/data/", nil
						},
						ListDataFilesFn: func(_ context.Context, _ string) ([]string, []bool, error) {
							return []string{"f.parquet"}, []bool{true}, nil
						},
					}, nil
				}
			},
			setupSign: func(p *mockPresigner) {
				p.PresignGetObjectFn = func(_ context.Context, _ string, _ time.Duration) (string, error) {
					return "", fmt.Errorf("signing credentials expired")
				}
			},
			wantErr: true,
			errCheck: func(t *testing.T, err error) {
				t.Helper()
				assert.Contains(t, err.Error(), "presign")
				assert.Contains(t, err.Error(), "signing credentials expired")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			auth := &testutil.MockAuthService{}
			if tt.setupAuth != nil {
				tt.setupAuth(auth)
			}

			intro := &testutil.MockIntrospectionRepo{}
			if tt.setupIntro != nil {
				tt.setupIntro(intro)
			}

			msf := &mockMetastoreQuerierFactory{}
			if tt.setupMS != nil {
				tt.setupMS(msf)
			}

			ps := &mockPresigner{}
			if tt.setupSign != nil {
				tt.setupSign(ps)
			}

			audit := &testutil.MockAuditRepo{}

			svc := newManifestService(msf, auth, ps, intro, audit, nil, nil)

			result, err := svc.GetManifest(context.Background(), principal, catalog, schema, table)

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
		})
	}
}

func TestManifestService_GetManifest_UsesQualifiedLookupName(t *testing.T) {
	t.Parallel()

	auth := &testutil.MockAuthService{}
	auth.LookupTableIDFn = func(_ context.Context, tableName string) (string, string, bool, error) {
		require.Equal(t, "demo.titanic.passengers", tableName)
		return "42", "10", false, nil
	}
	auth.CheckPrivilegeFn = func(_ context.Context, _, _ string, _ string, _ string) (bool, error) {
		return true, nil
	}
	auth.GetEffectiveRowFiltersFn = func(_ context.Context, _ string, _ string) ([]string, error) {
		return nil, nil
	}
	auth.GetEffectiveColumnMasksFn = func(_ context.Context, _ string, _ string) (map[string]string, error) {
		return nil, nil
	}

	intro := &testutil.MockIntrospectionRepo{}
	intro.ListColumnsFn = func(_ context.Context, _ string, _ domain.PageRequest) ([]domain.Column, int64, error) {
		return []domain.Column{{Name: "id", Type: "INTEGER"}}, 1, nil
	}

	msf := &mockMetastoreQuerierFactory{
		ForCatalogFn: func(_ context.Context, catalogName string) (domain.MetastoreQuerier, error) {
			require.Equal(t, "demo", catalogName)
			return &mockMetastoreQuerier{
				ReadDataPathFn: func(_ context.Context) (string, error) {
					return "s3://bucket/data/", nil
				},
				ReadSchemaPathFn: func(_ context.Context, schemaName string) (string, error) {
					require.Equal(t, "titanic", schemaName)
					return "", nil
				},
				ListDataFilesFn: func(_ context.Context, _ string) ([]string, []bool, error) {
					return []string{"f.parquet"}, []bool{true}, nil
				},
			}, nil
		},
	}

	ps := &mockPresigner{PresignGetObjectFn: func(_ context.Context, path string, _ time.Duration) (string, error) {
		return "https://signed.example.com/" + path, nil
	}}

	audit := &testutil.MockAuditRepo{}
	svc := newManifestService(msf, auth, ps, intro, audit, nil, nil)

	result, err := svc.GetManifest(context.Background(), "alice", "demo", "titanic", "passengers")
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Equal(t, "passengers", result.Table)
}
