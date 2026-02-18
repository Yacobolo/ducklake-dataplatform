//go:build integration

package security

import (
	"context"
	"database/sql"
	"testing"

	"github.com/google/uuid"
	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	internaldb "duck-demo/internal/db"
	dbstore "duck-demo/internal/db/dbstore"
	"duck-demo/internal/db/repository"
	"duck-demo/internal/domain"
)

// ctx is a package-level background context used by setup helpers.
var ctx = context.Background()

const (
	PrivSelect        = domain.PrivSelect
	PrivInsert        = domain.PrivInsert
	PrivUsage         = domain.PrivUsage
	PrivAllPrivileges = domain.PrivAllPrivileges

	PrivCreateExternalLocation  = domain.PrivCreateExternalLocation
	PrivCreateStorageCredential = domain.PrivCreateStorageCredential
	PrivCreateVolume            = domain.PrivCreateVolume
	PrivReadVolume              = domain.PrivReadVolume
	PrivWriteVolume             = domain.PrivWriteVolume

	SecurableCatalog           = domain.SecurableCatalog
	SecurableSchema            = domain.SecurableSchema
	SecurableTable             = domain.SecurableTable
	SecurableExternalLocation  = domain.SecurableExternalLocation
	SecurableStorageCredential = domain.SecurableStorageCredential
	SecurableVolume            = domain.SecurableVolume

	CatalogID = domain.CatalogID
)

// setupTestService creates an AuthorizationService with a temporary SQLite DB,
// runs migrations, and creates mock DuckLake metadata tables.
// Returns the service, a dbstore.Queries (for test data seeding), and a context.
func setupTestService(t *testing.T) (*AuthorizationService, *dbstore.Queries, context.Context) {
	t.Helper()

	db, _ := internaldb.OpenTestSQLite(t)

	// Create mock DuckLake catalog tables
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

	// Create repositories
	principalRepo := repository.NewPrincipalRepo(db)
	groupRepo := repository.NewGroupRepo(db)
	grantRepo := repository.NewGrantRepo(db)
	rowFilterRepo := repository.NewRowFilterRepo(db)
	columnMaskRepo := repository.NewColumnMaskRepo(db)
	introspectionRepo := repository.NewIntrospectionRepo(db)
	viewRepo := repository.NewViewRepo(db)

	svc := NewAuthorizationService(
		principalRepo, groupRepo, grantRepo,
		rowFilterRepo, columnMaskRepo, introspectionRepo,
		nil,
	)
	svc.SetViewRepository(viewRepo)

	// Return dbstore.Queries for test data seeding
	q := dbstore.New(db)

	return svc, q, context.Background()
}

func TestAdminBypassesAllChecks(t *testing.T) {
	cat, q, ctx := setupTestService(t)

	_, err := q.CreatePrincipal(ctx, dbstore.CreatePrincipalParams{ID: uuid.New().String(),
		Name: "admin", Type: "user", IsAdmin: 1,
	})
	if err != nil {
		t.Fatalf("create admin: %v", err)
	}

	// Admin should have access to anything without explicit grants
	ok, err := cat.CheckPrivilege(ctx, "admin", SecurableTable, "1", PrivSelect)
	if err != nil {
		t.Fatalf("check privilege: %v", err)
	}
	if !ok {
		t.Error("admin should bypass all privilege checks")
	}
}

func TestUserWithNoGrantsDenied(t *testing.T) {
	cat, q, ctx := setupTestService(t)

	_, err := q.CreatePrincipal(ctx, dbstore.CreatePrincipalParams{ID: uuid.New().String(),
		Name: "nobody", Type: "user", IsAdmin: 0,
	})
	if err != nil {
		t.Fatalf("create user: %v", err)
	}

	ok, err := cat.CheckPrivilege(ctx, "nobody", SecurableTable, "1", PrivSelect)
	if err != nil {
		t.Fatalf("check privilege: %v", err)
	}
	if ok {
		t.Error("user with no grants should be denied")
	}
}

func TestDirectTableGrant(t *testing.T) {
	cat, q, ctx := setupTestService(t)

	user, err := q.CreatePrincipal(ctx, dbstore.CreatePrincipalParams{ID: uuid.New().String(),
		Name: "analyst", Type: "user", IsAdmin: 0,
	})
	require.NoError(t, err)

	// Grant USAGE on schema (required gate)
	_, err = q.GrantPrivilege(ctx, dbstore.GrantPrivilegeParams{
		ID: uuid.New().String(), PrincipalID: user.ID, PrincipalType: "user",
		SecurableType: SecurableSchema, SecurableID: "0",
		Privilege: PrivUsage,
	})
	require.NoError(t, err)

	// Grant SELECT on table
	_, err = q.GrantPrivilege(ctx, dbstore.GrantPrivilegeParams{
		ID: uuid.New().String(), PrincipalID: user.ID, PrincipalType: "user",
		SecurableType: SecurableTable, SecurableID: "1",
		Privilege: PrivSelect,
	})
	require.NoError(t, err)

	ok, err := cat.CheckPrivilege(ctx, "analyst", SecurableTable, "1", PrivSelect)
	if err != nil {
		t.Fatalf("check: %v", err)
	}
	if !ok {
		t.Error("user with direct SELECT grant should have access")
	}
}

func TestUsageGateRequired(t *testing.T) {
	cat, q, ctx := setupTestService(t)

	user, err := q.CreatePrincipal(ctx, dbstore.CreatePrincipalParams{ID: uuid.New().String(),
		Name: "analyst", Type: "user", IsAdmin: 0,
	})
	require.NoError(t, err)

	// Grant SELECT on table but NOT USAGE on schema
	_, err = q.GrantPrivilege(ctx, dbstore.GrantPrivilegeParams{
		ID: uuid.New().String(), PrincipalID: user.ID, PrincipalType: "user",
		SecurableType: SecurableTable, SecurableID: "1",
		Privilege: PrivSelect,
	})
	require.NoError(t, err)

	ok, err := cat.CheckPrivilege(ctx, "analyst", SecurableTable, "1", PrivSelect)
	if err != nil {
		t.Fatalf("check: %v", err)
	}
	if ok {
		t.Error("user without USAGE on schema should be denied even with SELECT on table")
	}
}

func TestSchemaLevelInheritance(t *testing.T) {
	cat, q, ctx := setupTestService(t)

	user, err := q.CreatePrincipal(ctx, dbstore.CreatePrincipalParams{ID: uuid.New().String(),
		Name: "analyst", Type: "user", IsAdmin: 0,
	})
	require.NoError(t, err)

	// Grant USAGE + SELECT on schema (should cascade to tables)
	_, err = q.GrantPrivilege(ctx, dbstore.GrantPrivilegeParams{
		ID: uuid.New().String(), PrincipalID: user.ID, PrincipalType: "user",
		SecurableType: SecurableSchema, SecurableID: "0",
		Privilege: PrivUsage,
	})
	require.NoError(t, err)
	_, err = q.GrantPrivilege(ctx, dbstore.GrantPrivilegeParams{
		ID: uuid.New().String(), PrincipalID: user.ID, PrincipalType: "user",
		SecurableType: SecurableSchema, SecurableID: "0",
		Privilege: PrivSelect,
	})
	require.NoError(t, err)

	// Should inherit SELECT on any table within the schema
	ok, err := cat.CheckPrivilege(ctx, "analyst", SecurableTable, "1", PrivSelect)
	if err != nil {
		t.Fatalf("check: %v", err)
	}
	if !ok {
		t.Error("SELECT on schema should cascade to tables within it")
	}

	// Also check table 2 (orders)
	ok, err = cat.CheckPrivilege(ctx, "analyst", SecurableTable, "2", PrivSelect)
	if err != nil {
		t.Fatalf("check: %v", err)
	}
	if !ok {
		t.Error("SELECT on schema should cascade to all tables within it")
	}
}

func TestCatalogLevelInheritance(t *testing.T) {
	cat, q, ctx := setupTestService(t)

	user, err := q.CreatePrincipal(ctx, dbstore.CreatePrincipalParams{ID: uuid.New().String(),
		Name: "superuser", Type: "user", IsAdmin: 0,
	})
	require.NoError(t, err)

	// Grant ALL_PRIVILEGES at catalog level
	_, err = q.GrantPrivilege(ctx, dbstore.GrantPrivilegeParams{
		ID: uuid.New().String(), PrincipalID: user.ID, PrincipalType: "user",
		SecurableType: SecurableCatalog, SecurableID: CatalogID,
		Privilege: PrivAllPrivileges,
	})
	require.NoError(t, err)

	// Should inherit to schema USAGE and table SELECT
	ok, err := cat.CheckPrivilege(ctx, "superuser", SecurableTable, "1", PrivSelect)
	if err != nil {
		t.Fatalf("check: %v", err)
	}
	if !ok {
		t.Error("ALL_PRIVILEGES at catalog level should cascade to table SELECT")
	}
}

func TestGroupMembership(t *testing.T) {
	cat, q, ctx := setupTestService(t)

	user, err := q.CreatePrincipal(ctx, dbstore.CreatePrincipalParams{ID: uuid.New().String(),
		Name: "analyst", Type: "user", IsAdmin: 0,
	})
	require.NoError(t, err)

	group, err := q.CreateGroup(ctx, dbstore.CreateGroupParams{ID: uuid.New().String(), Name: "analysts"})
	require.NoError(t, err)
	err = q.AddGroupMember(ctx, dbstore.AddGroupMemberParams{
		GroupID: group.ID, MemberType: "user", MemberID: user.ID,
	})
	require.NoError(t, err)

	// Grant to group
	_, err = q.GrantPrivilege(ctx, dbstore.GrantPrivilegeParams{
		ID: uuid.New().String(), PrincipalID: group.ID, PrincipalType: "group",
		SecurableType: SecurableSchema, SecurableID: "0",
		Privilege: PrivUsage,
	})
	require.NoError(t, err)
	_, err = q.GrantPrivilege(ctx, dbstore.GrantPrivilegeParams{
		ID: uuid.New().String(), PrincipalID: group.ID, PrincipalType: "group",
		SecurableType: SecurableTable, SecurableID: "1",
		Privilege: PrivSelect,
	})
	require.NoError(t, err)

	ok, err := cat.CheckPrivilege(ctx, "analyst", SecurableTable, "1", PrivSelect)
	if err != nil {
		t.Fatalf("check: %v", err)
	}
	if !ok {
		t.Error("user in group with SELECT should have access")
	}
}

func TestNestedGroupMembership(t *testing.T) {
	cat, q, ctx := setupTestService(t)

	user, err := q.CreatePrincipal(ctx, dbstore.CreatePrincipalParams{ID: uuid.New().String(),
		Name: "analyst", Type: "user", IsAdmin: 0,
	})
	require.NoError(t, err)

	innerGroup, err := q.CreateGroup(ctx, dbstore.CreateGroupParams{ID: uuid.New().String(), Name: "analysts"})
	require.NoError(t, err)
	outerGroup, err := q.CreateGroup(ctx, dbstore.CreateGroupParams{ID: uuid.New().String(), Name: "data_team"})
	require.NoError(t, err)

	// user -> analysts -> data_team
	err = q.AddGroupMember(ctx, dbstore.AddGroupMemberParams{
		GroupID: innerGroup.ID, MemberType: "user", MemberID: user.ID,
	})
	require.NoError(t, err)
	err = q.AddGroupMember(ctx, dbstore.AddGroupMemberParams{
		GroupID: outerGroup.ID, MemberType: "group", MemberID: innerGroup.ID,
	})
	require.NoError(t, err)

	// Grant to outer group
	_, err = q.GrantPrivilege(ctx, dbstore.GrantPrivilegeParams{
		ID: uuid.New().String(), PrincipalID: outerGroup.ID, PrincipalType: "group",
		SecurableType: SecurableCatalog, SecurableID: CatalogID,
		Privilege: PrivAllPrivileges,
	})
	require.NoError(t, err)

	ok, err := cat.CheckPrivilege(ctx, "analyst", SecurableTable, "1", PrivSelect)
	if err != nil {
		t.Fatalf("check: %v", err)
	}
	if !ok {
		t.Error("user in nested group should inherit privileges from outer group")
	}
}

func TestRowFilterForPrincipal(t *testing.T) {
	cat, q, ctx := setupTestService(t)

	user, err := q.CreatePrincipal(ctx, dbstore.CreatePrincipalParams{ID: uuid.New().String(),
		Name: "analyst", Type: "user", IsAdmin: 0,
	})
	require.NoError(t, err)

	// Create and bind row filter
	filter, err := q.CreateRowFilter(ctx, dbstore.CreateRowFilterParams{
		ID: uuid.New().String(), TableID: "1",
		FilterSql: `"Pclass" = 1`,
	})
	require.NoError(t, err)
	err = q.BindRowFilter(ctx, dbstore.BindRowFilterParams{
		ID: uuid.New().String(), RowFilterID: filter.ID, PrincipalID: user.ID, PrincipalType: "user",
	})
	require.NoError(t, err)

	results, err := cat.GetEffectiveRowFilters(ctx, "analyst", "1")
	if err != nil {
		t.Fatalf("get row filters: %v", err)
	}
	if len(results) == 0 {
		t.Fatal("expected row filters, got none")
	}
	if results[0] != `"Pclass" = 1` {
		t.Errorf("expected filter \"Pclass\" = 1, got %q", results[0])
	}
}

func TestRowFilterForGroupMember(t *testing.T) {
	cat, q, ctx := setupTestService(t)

	user, err := q.CreatePrincipal(ctx, dbstore.CreatePrincipalParams{ID: uuid.New().String(),
		Name: "analyst", Type: "user", IsAdmin: 0,
	})
	require.NoError(t, err)
	group, err := q.CreateGroup(ctx, dbstore.CreateGroupParams{ID: uuid.New().String(), Name: "analysts"})
	require.NoError(t, err)
	err = q.AddGroupMember(ctx, dbstore.AddGroupMemberParams{
		GroupID: group.ID, MemberType: "user", MemberID: user.ID,
	})
	require.NoError(t, err)

	filter, err := q.CreateRowFilter(ctx, dbstore.CreateRowFilterParams{
		ID: uuid.New().String(), TableID: "1",
		FilterSql: `"Pclass" = 1`,
	})
	require.NoError(t, err)
	err = q.BindRowFilter(ctx, dbstore.BindRowFilterParams{
		ID: uuid.New().String(), RowFilterID: filter.ID, PrincipalID: group.ID, PrincipalType: "group",
	})
	require.NoError(t, err)

	results, err := cat.GetEffectiveRowFilters(ctx, "analyst", "1")
	if err != nil {
		t.Fatalf("get row filters: %v", err)
	}
	if len(results) == 0 {
		t.Fatal("expected row filter via group, got none")
	}
}

func TestAdminNoRowFilter(t *testing.T) {
	cat, q, ctx := setupTestService(t)

	_, err := q.CreatePrincipal(ctx, dbstore.CreatePrincipalParams{ID: uuid.New().String(),
		Name: "admin", Type: "user", IsAdmin: 1,
	})
	require.NoError(t, err)

	// Even if there's a filter, admin should bypass
	filter, err := q.CreateRowFilter(ctx, dbstore.CreateRowFilterParams{
		ID: uuid.New().String(), TableID: "1",
		FilterSql: `"Pclass" = 1`,
	})
	require.NoError(t, err)
	err = q.BindRowFilter(ctx, dbstore.BindRowFilterParams{
		ID: uuid.New().String(), RowFilterID: filter.ID, PrincipalID: "1", PrincipalType: "user",
	})
	require.NoError(t, err)

	results, err := cat.GetEffectiveRowFilters(ctx, "admin", "1")
	if err != nil {
		t.Fatalf("get row filters: %v", err)
	}
	if len(results) != 0 {
		t.Error("admin should have no row filters")
	}
}

func TestColumnMaskForPrincipal(t *testing.T) {
	cat, q, ctx := setupTestService(t)

	user, err := q.CreatePrincipal(ctx, dbstore.CreatePrincipalParams{ID: uuid.New().String(),
		Name: "analyst", Type: "user", IsAdmin: 0,
	})
	require.NoError(t, err)

	mask, err := q.CreateColumnMask(ctx, dbstore.CreateColumnMaskParams{
		ID: uuid.New().String(), TableID: "1",
		ColumnName:     "Name",
		MaskExpression: "'***'",
	})
	require.NoError(t, err)
	err = q.BindColumnMask(ctx, dbstore.BindColumnMaskParams{
		ID: uuid.New().String(), ColumnMaskID: mask.ID, PrincipalID: user.ID,
		PrincipalType: "user", SeeOriginal: 0,
	})
	require.NoError(t, err)

	masks, err := cat.GetEffectiveColumnMasks(ctx, "analyst", "1")
	if err != nil {
		t.Fatalf("get column masks: %v", err)
	}
	if masks == nil {
		t.Fatal("expected column masks, got nil")
	}
	if masks["name"] != "'***'" {
		t.Errorf("expected name mask = '***', got %q", masks["name"])
	}
}

func TestColumnMaskSeeOriginal(t *testing.T) {
	cat, q, ctx := setupTestService(t)

	user, err := q.CreatePrincipal(ctx, dbstore.CreatePrincipalParams{ID: uuid.New().String(),
		Name: "analyst", Type: "user", IsAdmin: 0,
	})
	require.NoError(t, err)

	mask, err := q.CreateColumnMask(ctx, dbstore.CreateColumnMaskParams{
		ID: uuid.New().String(), TableID: "1",
		ColumnName:     "Name",
		MaskExpression: "'***'",
	})
	require.NoError(t, err)
	// see_original = 1 means user sees the real value
	err = q.BindColumnMask(ctx, dbstore.BindColumnMaskParams{
		ID: uuid.New().String(), ColumnMaskID: mask.ID, PrincipalID: user.ID,
		PrincipalType: "user", SeeOriginal: 1,
	})
	require.NoError(t, err)

	masks, err := cat.GetEffectiveColumnMasks(ctx, "analyst", "1")
	if err != nil {
		t.Fatalf("get column masks: %v", err)
	}
	if masks != nil {
		t.Errorf("user with see_original=1 should have no masks, got %v", masks)
	}
}

func TestAdminNoColumnMasks(t *testing.T) {
	cat, q, ctx := setupTestService(t)

	_, err := q.CreatePrincipal(ctx, dbstore.CreatePrincipalParams{ID: uuid.New().String(),
		Name: "admin", Type: "user", IsAdmin: 1,
	})
	require.NoError(t, err)

	mask, err := q.CreateColumnMask(ctx, dbstore.CreateColumnMaskParams{
		ID: uuid.New().String(), TableID: "1",
		ColumnName:     "Name",
		MaskExpression: "'***'",
	})
	require.NoError(t, err)
	err = q.BindColumnMask(ctx, dbstore.BindColumnMaskParams{
		ID: uuid.New().String(), ColumnMaskID: mask.ID, PrincipalID: "1",
		PrincipalType: "user", SeeOriginal: 0,
	})
	require.NoError(t, err)

	masks, err := cat.GetEffectiveColumnMasks(ctx, "admin", "1")
	if err != nil {
		t.Fatalf("get column masks: %v", err)
	}
	if masks != nil {
		t.Error("admin should have no column masks")
	}
}

func TestUnknownPrincipalError(t *testing.T) {
	cat, _, ctx := setupTestService(t)

	_, err := cat.CheckPrivilege(ctx, "nobody", SecurableTable, "1", PrivSelect)
	if err == nil {
		t.Error("expected error for unknown principal")
	}
}

func TestLookupTableID(t *testing.T) {
	cat, _, ctx := setupTestService(t)

	tableID, schemaID, _, err := cat.LookupTableID(ctx, "main.titanic")
	if err != nil {
		t.Fatalf("lookup: %v", err)
	}
	if tableID != "1" {
		t.Errorf("expected tableID=1, got %s", tableID)
	}
	if schemaID != "0" {
		t.Errorf("expected schemaID=0, got %s", schemaID)
	}
}

func TestLookupTableIDNotFound(t *testing.T) {
	cat, _, ctx := setupTestService(t)

	_, _, _, err := cat.LookupTableID(ctx, "main.nonexistent")
	if err == nil {
		t.Error("expected error for nonexistent table")
	}
}

func TestLookupTableID_SchemaQualified(t *testing.T) {
	cat, _, ctx := setupTestService(t)

	tableID, schemaID, _, err := cat.LookupTableID(ctx, "main.titanic")
	require.NoError(t, err)
	assert.Equal(t, "1", tableID)
	assert.Equal(t, "0", schemaID)
}

func TestLookupTableID_AmbiguousUnqualified(t *testing.T) {
	db, _ := internaldb.OpenTestSQLite(t)

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
		VALUES (0, 'main', 0), (3, 'analytics', 0);
		INSERT INTO ducklake_table (table_id, table_name, schema_id, begin_snapshot)
		VALUES (1, 'titanic', 0, 1), (9, 'titanic', 3, 1);
	`)
	require.NoError(t, err)

	cat := NewAuthorizationService(
		repository.NewPrincipalRepo(db),
		repository.NewGroupRepo(db),
		repository.NewGrantRepo(db),
		repository.NewRowFilterRepo(db),
		repository.NewColumnMaskRepo(db),
		repository.NewIntrospectionRepo(db),
		nil,
	)

	_, _, _, err = cat.LookupTableID(context.Background(), "titanic")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "ambiguous")
}

func TestLookupTableID_CatalogQualified(t *testing.T) {
	cat, _, ctx := setupTestService(t)

	cat.SetCatalogTableLookup(func(_ context.Context, catalogName, schemaName, tableName string) (*domain.TableDetail, error) {
		require.Equal(t, "demo", catalogName)
		require.Equal(t, "titanic", schemaName)
		require.Equal(t, "passengers", tableName)
		return &domain.TableDetail{TableID: "table-42", TableType: domain.TableTypeManaged}, nil
	})

	tableID, schemaID, isExternal, err := cat.LookupTableID(ctx, "demo.titanic.passengers")
	require.NoError(t, err)
	assert.Equal(t, "table-42", tableID)
	assert.Equal(t, "", schemaID)
	assert.False(t, isExternal)
}

func TestLookupTableID_ViewSchemaQualified(t *testing.T) {
	cat, q, ctx := setupTestService(t)

	created, err := q.CreateView(ctx, dbstore.CreateViewParams{
		ID:             uuid.New().String(),
		SchemaID:       "0",
		Name:           "titanic_view",
		ViewDefinition: `SELECT 1 AS x`,
		Comment:        sql.NullString{},
		Properties:     sql.NullString{String: "{}", Valid: true},
		Owner:          "owner",
		SourceTables:   sql.NullString{String: "[]", Valid: true},
	})
	require.NoError(t, err)

	tableID, schemaID, isExternal, err := cat.LookupTableID(ctx, "main.titanic_view")
	require.NoError(t, err)
	assert.Equal(t, created.ID, tableID)
	assert.Equal(t, "0", schemaID)
	assert.False(t, isExternal)
}

func TestCheckPrivilege_ViewInheritsSchemaGrant(t *testing.T) {
	cat, q, ctx := setupTestService(t)

	user, err := q.CreatePrincipal(ctx, dbstore.CreatePrincipalParams{ID: uuid.New().String(),
		Name: "view_reader", Type: "user", IsAdmin: 0,
	})
	require.NoError(t, err)

	_, err = q.CreateView(ctx, dbstore.CreateViewParams{
		ID:             uuid.New().String(),
		SchemaID:       "0",
		Name:           "engagement_view",
		ViewDefinition: `SELECT 1 AS x`,
		Comment:        sql.NullString{},
		Properties:     sql.NullString{String: "{}", Valid: true},
		Owner:          "owner",
		SourceTables:   sql.NullString{String: "[]", Valid: true},
	})
	require.NoError(t, err)

	_, err = q.GrantPrivilege(ctx, dbstore.GrantPrivilegeParams{
		ID: uuid.New().String(), PrincipalID: user.ID, PrincipalType: "user",
		SecurableType: SecurableSchema, SecurableID: "0",
		Privilege: PrivUsage,
	})
	require.NoError(t, err)
	_, err = q.GrantPrivilege(ctx, dbstore.GrantPrivilegeParams{
		ID: uuid.New().String(), PrincipalID: user.ID, PrincipalType: "user",
		SecurableType: SecurableSchema, SecurableID: "0",
		Privilege: PrivSelect,
	})
	require.NoError(t, err)

	viewID, _, _, err := cat.LookupTableID(ctx, "main.engagement_view")
	require.NoError(t, err)

	allowed, err := cat.CheckPrivilege(ctx, "view_reader", SecurableTable, viewID, PrivSelect)
	require.NoError(t, err)
	assert.True(t, allowed)
}

func TestCheckPrivilege_ViewByIDWithoutPriorLookup(t *testing.T) {
	cat, q, ctx := setupTestService(t)

	user, err := q.CreatePrincipal(ctx, dbstore.CreatePrincipalParams{ID: uuid.New().String(),
		Name: "direct_view_reader", Type: "user", IsAdmin: 0,
	})
	require.NoError(t, err)

	view, err := q.CreateView(ctx, dbstore.CreateViewParams{
		ID:             uuid.New().String(),
		SchemaID:       "0",
		Name:           "direct_view",
		ViewDefinition: `SELECT 1 AS x`,
		Comment:        sql.NullString{},
		Properties:     sql.NullString{String: "{}", Valid: true},
		Owner:          "owner",
		SourceTables:   sql.NullString{String: "[]", Valid: true},
	})
	require.NoError(t, err)

	_, err = q.GrantPrivilege(ctx, dbstore.GrantPrivilegeParams{
		ID: uuid.New().String(), PrincipalID: user.ID, PrincipalType: "user",
		SecurableType: SecurableSchema, SecurableID: "0",
		Privilege: PrivUsage,
	})
	require.NoError(t, err)
	_, err = q.GrantPrivilege(ctx, dbstore.GrantPrivilegeParams{
		ID: uuid.New().String(), PrincipalID: user.ID, PrincipalType: "user",
		SecurableType: SecurableSchema, SecurableID: "0",
		Privilege: PrivSelect,
	})
	require.NoError(t, err)

	allowed, err := cat.CheckPrivilege(ctx, "direct_view_reader", SecurableTable, view.ID, PrivSelect)
	require.NoError(t, err)
	assert.True(t, allowed)
}

func TestLookupTableID_CatalogQualifiedViewSynthesizesID(t *testing.T) {
	cat, _, ctx := setupTestService(t)

	cat.SetCatalogViewLookup(func(_ context.Context, catalogName, schemaName, viewName string) (*domain.ViewDetail, error) {
		require.Equal(t, "lake", catalogName)
		require.Equal(t, "main", schemaName)
		require.Equal(t, "engagement_view", viewName)
		return &domain.ViewDetail{SchemaID: "0", Name: viewName}, nil
	})

	tableID, schemaID, isExternal, err := cat.LookupTableID(ctx, "lake.main.engagement_view")
	require.NoError(t, err)
	assert.Equal(t, "0", schemaID)
	assert.False(t, isExternal)
	assert.Equal(t, syntheticViewID("0", "engagement_view"), tableID)
}

func TestCheckPrivilege_SyntheticViewIDInheritsSchemaGrant(t *testing.T) {
	cat, q, ctx := setupTestService(t)

	user, err := q.CreatePrincipal(ctx, dbstore.CreatePrincipalParams{ID: uuid.New().String(),
		Name: "synthetic_view_reader", Type: "user", IsAdmin: 0,
	})
	require.NoError(t, err)

	_, err = q.GrantPrivilege(ctx, dbstore.GrantPrivilegeParams{
		ID: uuid.New().String(), PrincipalID: user.ID, PrincipalType: "user",
		SecurableType: SecurableSchema, SecurableID: "0",
		Privilege: PrivUsage,
	})
	require.NoError(t, err)
	_, err = q.GrantPrivilege(ctx, dbstore.GrantPrivilegeParams{
		ID: uuid.New().String(), PrincipalID: user.ID, PrincipalType: "user",
		SecurableType: SecurableSchema, SecurableID: "0",
		Privilege: PrivSelect,
	})
	require.NoError(t, err)

	allowed, err := cat.CheckPrivilege(ctx, "synthetic_view_reader", SecurableTable, syntheticViewID("0", "engagement_view"), PrivSelect)
	require.NoError(t, err)
	assert.True(t, allowed)
}

func TestLookupSchemaID(t *testing.T) {
	cat, _, ctx := setupTestService(t)

	schemaID, err := cat.LookupSchemaID(ctx, "main")
	if err != nil {
		t.Fatalf("lookup: %v", err)
	}
	if schemaID != "0" {
		t.Errorf("expected schemaID=0, got %s", schemaID)
	}
}

// === Catalog-Scoped Securable Tests ===

func TestCatalogScopedPrivilege_DirectGrant(t *testing.T) {
	tests := []struct {
		name          string
		securableType string
		privilege     string
	}{
		{"external_location with CREATE_EXTERNAL_LOCATION", SecurableExternalLocation, PrivCreateExternalLocation},
		{"storage_credential with CREATE_STORAGE_CREDENTIAL", SecurableStorageCredential, PrivCreateStorageCredential},
		{"volume with CREATE_VOLUME", SecurableVolume, PrivCreateVolume},
		{"volume with READ_VOLUME", SecurableVolume, PrivReadVolume},
		{"volume with WRITE_VOLUME", SecurableVolume, PrivWriteVolume},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			svc, q, ctx := setupTestService(t)

			user, err := q.CreatePrincipal(ctx, dbstore.CreatePrincipalParams{ID: uuid.New().String(),
				Name: "user1", Type: "user", IsAdmin: 0,
			})
			require.NoError(t, err)

			// Grant the specific privilege on the securable
			_, err = q.GrantPrivilege(ctx, dbstore.GrantPrivilegeParams{
				PrincipalID:   user.ID,
				PrincipalType: "user",
				SecurableType: tt.securableType,
				SecurableID:   "42", // arbitrary securable ID
				Privilege:     tt.privilege,
			})
			require.NoError(t, err)

			ok, err := svc.CheckPrivilege(ctx, "user1", tt.securableType, "42", tt.privilege)
			if err != nil {
				t.Fatalf("check: %v", err)
			}
			if !ok {
				t.Errorf("user with direct %s grant on %s should have access", tt.privilege, tt.securableType)
			}
		})
	}
}

func TestCatalogScopedPrivilege_InheritFromCatalog(t *testing.T) {
	tests := []struct {
		name          string
		securableType string
		privilege     string
	}{
		{"external_location inherits from catalog", SecurableExternalLocation, PrivCreateExternalLocation},
		{"storage_credential inherits from catalog", SecurableStorageCredential, PrivCreateStorageCredential},
		{"volume inherits CREATE_VOLUME from catalog", SecurableVolume, PrivCreateVolume},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			svc, q, ctx := setupTestService(t)

			user, err := q.CreatePrincipal(ctx, dbstore.CreatePrincipalParams{ID: uuid.New().String(),
				Name: "user1", Type: "user", IsAdmin: 0,
			})
			require.NoError(t, err)

			// Grant ALL_PRIVILEGES at catalog level (should inherit to catalog-scoped securables)
			_, err = q.GrantPrivilege(ctx, dbstore.GrantPrivilegeParams{
				PrincipalID:   user.ID,
				PrincipalType: "user",
				SecurableType: SecurableCatalog,
				SecurableID:   CatalogID,
				Privilege:     PrivAllPrivileges,
			})
			require.NoError(t, err)

			ok, err := svc.CheckPrivilege(ctx, "user1", tt.securableType, "99", tt.privilege)
			if err != nil {
				t.Fatalf("check: %v", err)
			}
			if !ok {
				t.Errorf("user with ALL_PRIVILEGES on catalog should inherit %s on %s", tt.privilege, tt.securableType)
			}
		})
	}
}

func TestCatalogScopedPrivilege_Denied(t *testing.T) {
	svc, q, ctx := setupTestService(t)

	_, err := q.CreatePrincipal(ctx, dbstore.CreatePrincipalParams{ID: uuid.New().String(),
		Name: "nobody", Type: "user", IsAdmin: 0,
	})
	require.NoError(t, err)

	// No grants at all -- should be denied on all new securable types
	for _, tt := range []struct {
		securableType string
		privilege     string
	}{
		{SecurableExternalLocation, PrivCreateExternalLocation},
		{SecurableStorageCredential, PrivCreateStorageCredential},
		{SecurableVolume, PrivCreateVolume},
		{SecurableVolume, PrivReadVolume},
		{SecurableVolume, PrivWriteVolume},
	} {
		ok, err := svc.CheckPrivilege(ctx, "nobody", tt.securableType, "1", tt.privilege)
		if err != nil {
			t.Fatalf("check %s/%s: %v", tt.securableType, tt.privilege, err)
		}
		if ok {
			t.Errorf("user with no grants should be denied %s on %s", tt.privilege, tt.securableType)
		}
	}
}

func TestCatalogScopedPrivilege_AdminBypass(t *testing.T) {
	svc, q, ctx := setupTestService(t)

	_, err := q.CreatePrincipal(ctx, dbstore.CreatePrincipalParams{ID: uuid.New().String(),
		Name: "admin", Type: "user", IsAdmin: 1,
	})
	require.NoError(t, err)

	// Admin should bypass all catalog-scoped privilege checks
	for _, tt := range []struct {
		securableType string
		privilege     string
	}{
		{SecurableExternalLocation, PrivCreateExternalLocation},
		{SecurableStorageCredential, PrivCreateStorageCredential},
		{SecurableVolume, PrivCreateVolume},
		{SecurableVolume, PrivReadVolume},
	} {
		ok, err := svc.CheckPrivilege(ctx, "admin", tt.securableType, "1", tt.privilege)
		if err != nil {
			t.Fatalf("check %s/%s: %v", tt.securableType, tt.privilege, err)
		}
		if !ok {
			t.Errorf("admin should bypass %s on %s", tt.privilege, tt.securableType)
		}
	}
}

func TestCatalogScopedPrivilege_GroupInheritance(t *testing.T) {
	svc, q, ctx := setupTestService(t)

	user, err := q.CreatePrincipal(ctx, dbstore.CreatePrincipalParams{ID: uuid.New().String(),
		Name: "user1", Type: "user", IsAdmin: 0,
	})
	require.NoError(t, err)

	group, err := q.CreateGroup(ctx, dbstore.CreateGroupParams{ID: uuid.New().String(), Name: "admins"})
	require.NoError(t, err)
	err = q.AddGroupMember(ctx, dbstore.AddGroupMemberParams{
		GroupID: group.ID, MemberType: "user", MemberID: user.ID,
	})
	require.NoError(t, err)

	// Grant CREATE_EXTERNAL_LOCATION to the group on a specific external location
	_, err = q.GrantPrivilege(ctx, dbstore.GrantPrivilegeParams{
		ID: uuid.New().String(), PrincipalID: group.ID,
		PrincipalType: "group",
		SecurableType: SecurableExternalLocation,
		SecurableID:   "10",
		Privilege:     PrivCreateExternalLocation,
	})
	require.NoError(t, err)

	ok, err := svc.CheckPrivilege(ctx, "user1", SecurableExternalLocation, "10", PrivCreateExternalLocation)
	if err != nil {
		t.Fatalf("check: %v", err)
	}
	if !ok {
		t.Error("user in group with CREATE_EXTERNAL_LOCATION should have access")
	}
}

// === Issue #43: SeeOriginal=true user exemption overridden by group mask ===

func TestColumnMask_SeeOriginalNotOverriddenByGroup(t *testing.T) {
	svc, q, ctx := setupTestService(t)

	user, err := q.CreatePrincipal(ctx, dbstore.CreatePrincipalParams{
		ID: uuid.New().String(), Name: "analyst", Type: "user", IsAdmin: 0,
	})
	require.NoError(t, err)

	group, err := q.CreateGroup(ctx, dbstore.CreateGroupParams{
		ID: uuid.New().String(), Name: "analysts",
	})
	require.NoError(t, err)
	err = q.AddGroupMember(ctx, dbstore.AddGroupMemberParams{
		GroupID: group.ID, MemberType: "user", MemberID: user.ID,
	})
	require.NoError(t, err)

	// Create mask on "Name" column
	mask, err := q.CreateColumnMask(ctx, dbstore.CreateColumnMaskParams{
		ID: uuid.New().String(), TableID: "1",
		ColumnName:     "Name",
		MaskExpression: "'***'",
	})
	require.NoError(t, err)

	// User binding: see_original=1 (user is exempted)
	err = q.BindColumnMask(ctx, dbstore.BindColumnMaskParams{
		ID: uuid.New().String(), ColumnMaskID: mask.ID, PrincipalID: user.ID,
		PrincipalType: "user", SeeOriginal: 1,
	})
	require.NoError(t, err)

	// Group binding: see_original=0 (group sees masked)
	err = q.BindColumnMask(ctx, dbstore.BindColumnMaskParams{
		ID: uuid.New().String(), ColumnMaskID: mask.ID, PrincipalID: group.ID,
		PrincipalType: "group", SeeOriginal: 0,
	})
	require.NoError(t, err)

	masks, err := svc.GetEffectiveColumnMasks(ctx, "analyst", "1")
	require.NoError(t, err)
	if masks != nil {
		t.Errorf("user with direct see_original=true should not be masked, got %v", masks)
	}
}

// === Issue #48: Case-insensitive column mask matching ===

func TestColumnMask_CaseInsensitiveLookup(t *testing.T) {
	svc, q, ctx := setupTestService(t)

	user, err := q.CreatePrincipal(ctx, dbstore.CreatePrincipalParams{
		ID: uuid.New().String(), Name: "analyst", Type: "user", IsAdmin: 0,
	})
	require.NoError(t, err)

	// Mask on lowercase "email"
	mask, err := q.CreateColumnMask(ctx, dbstore.CreateColumnMaskParams{
		ID: uuid.New().String(), TableID: "1",
		ColumnName:     "email",
		MaskExpression: "'***'",
	})
	require.NoError(t, err)
	err = q.BindColumnMask(ctx, dbstore.BindColumnMaskParams{
		ID: uuid.New().String(), ColumnMaskID: mask.ID, PrincipalID: user.ID,
		PrincipalType: "user", SeeOriginal: 0,
	})
	require.NoError(t, err)

	masks, err := svc.GetEffectiveColumnMasks(ctx, "analyst", "1")
	require.NoError(t, err)
	require.NotNil(t, masks)

	// The key should be normalized to lowercase
	require.Equal(t, "'***'", masks["email"], "mask should be stored with lowercase key")
	_, hasUpper := masks["Email"]
	require.False(t, hasUpper, "mask should not have mixed-case key")
}

// === Issue #46: ALL_PRIVILEGES expansion in hasGrant ===

func TestAllPrivileges_ExpandsToSelect(t *testing.T) {
	svc, q, ctx := setupTestService(t)

	user, err := q.CreatePrincipal(ctx, dbstore.CreatePrincipalParams{
		ID: uuid.New().String(), Name: "poweruser", Type: "user", IsAdmin: 0,
	})
	require.NoError(t, err)

	// Grant USAGE on schema (required gate)
	_, err = q.GrantPrivilege(ctx, dbstore.GrantPrivilegeParams{
		ID: uuid.New().String(), PrincipalID: user.ID, PrincipalType: "user",
		SecurableType: SecurableSchema, SecurableID: "0",
		Privilege: PrivUsage,
	})
	require.NoError(t, err)

	// Grant ALL_PRIVILEGES on the table (not SELECT specifically)
	_, err = q.GrantPrivilege(ctx, dbstore.GrantPrivilegeParams{
		ID: uuid.New().String(), PrincipalID: user.ID, PrincipalType: "user",
		SecurableType: SecurableTable, SecurableID: "1",
		Privilege: PrivAllPrivileges,
	})
	require.NoError(t, err)

	// Check SELECT — should be allowed via ALL_PRIVILEGES expansion
	ok, err := svc.CheckPrivilege(ctx, "poweruser", SecurableTable, "1", PrivSelect)
	require.NoError(t, err)
	require.True(t, ok, "ALL_PRIVILEGES on table should grant SELECT")

	// Check INSERT — should also be allowed
	ok, err = svc.CheckPrivilege(ctx, "poweruser", SecurableTable, "1", PrivInsert)
	require.NoError(t, err)
	require.True(t, ok, "ALL_PRIVILEGES on table should grant INSERT")
}

func TestAllPrivileges_OnSchema_ExpandsToUsageAndSelect(t *testing.T) {
	svc, q, ctx := setupTestService(t)

	user, err := q.CreatePrincipal(ctx, dbstore.CreatePrincipalParams{
		ID: uuid.New().String(), Name: "poweruser", Type: "user", IsAdmin: 0,
	})
	require.NoError(t, err)

	// Grant ALL_PRIVILEGES on schema (should expand for USAGE gate and SELECT inheritance)
	_, err = q.GrantPrivilege(ctx, dbstore.GrantPrivilegeParams{
		ID: uuid.New().String(), PrincipalID: user.ID, PrincipalType: "user",
		SecurableType: SecurableSchema, SecurableID: "0",
		Privilege: PrivAllPrivileges,
	})
	require.NoError(t, err)

	// Check SELECT on a table — USAGE gate should pass via ALL_PRIVILEGES, SELECT should inherit
	ok, err := svc.CheckPrivilege(ctx, "poweruser", SecurableTable, "1", PrivSelect)
	require.NoError(t, err)
	require.True(t, ok, "ALL_PRIVILEGES on schema should grant USAGE + SELECT on tables")
}

func TestAllPrivileges_GroupGrant_ExpandsToSelect(t *testing.T) {
	svc, q, ctx := setupTestService(t)

	user, err := q.CreatePrincipal(ctx, dbstore.CreatePrincipalParams{
		ID: uuid.New().String(), Name: "analyst", Type: "user", IsAdmin: 0,
	})
	require.NoError(t, err)

	group, err := q.CreateGroup(ctx, dbstore.CreateGroupParams{
		ID: uuid.New().String(), Name: "team",
	})
	require.NoError(t, err)
	err = q.AddGroupMember(ctx, dbstore.AddGroupMemberParams{
		GroupID: group.ID, MemberType: "user", MemberID: user.ID,
	})
	require.NoError(t, err)

	// Group has ALL_PRIVILEGES on schema
	_, err = q.GrantPrivilege(ctx, dbstore.GrantPrivilegeParams{
		ID: uuid.New().String(), PrincipalID: group.ID, PrincipalType: "group",
		SecurableType: SecurableSchema, SecurableID: "0",
		Privilege: PrivAllPrivileges,
	})
	require.NoError(t, err)

	ok, err := svc.CheckPrivilege(ctx, "analyst", SecurableTable, "1", PrivSelect)
	require.NoError(t, err)
	require.True(t, ok, "group ALL_PRIVILEGES on schema should grant SELECT on table to group member")
}
