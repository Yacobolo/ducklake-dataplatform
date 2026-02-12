package app

import (
	"context"
	"database/sql"
	"fmt"

	dbstore "duck-demo/internal/db/dbstore"
	"duck-demo/internal/domain"
	"duck-demo/internal/service/security"
)

// seedCatalog populates the metastore with demo principals, groups, grants,
// row filters, and column masks. Idempotent — checks if data already exists.
func seedCatalog(ctx context.Context, cat *security.AuthorizationService, q *dbstore.Queries) error {

	// Check if already seeded
	principals, _ := q.ListPrincipals(ctx)
	if len(principals) > 0 {
		return nil // already seeded
	}

	// --- Principals ---
	adminUser, err := q.CreatePrincipal(ctx, dbstore.CreatePrincipalParams{
		Name: "admin_user", Type: "user", IsAdmin: 1,
	})
	if err != nil {
		return fmt.Errorf("create admin_user: %w", err)
	}

	analyst1, err := q.CreatePrincipal(ctx, dbstore.CreatePrincipalParams{
		Name: "analyst1", Type: "user", IsAdmin: 0,
	})
	if err != nil {
		return fmt.Errorf("create analyst1: %w", err)
	}

	researcher1, err := q.CreatePrincipal(ctx, dbstore.CreatePrincipalParams{
		Name: "researcher1", Type: "user", IsAdmin: 0,
	})
	if err != nil {
		return fmt.Errorf("create researcher1: %w", err)
	}

	_, err = q.CreatePrincipal(ctx, dbstore.CreatePrincipalParams{
		Name: "no_access_user", Type: "user", IsAdmin: 0,
	})
	if err != nil {
		return fmt.Errorf("create no_access_user: %w", err)
	}

	// --- Groups ---
	adminsGroup, err := q.CreateGroup(ctx, dbstore.CreateGroupParams{
		Name:        "admins",
		Description: sql.NullString{String: "Administrators with full access", Valid: true},
	})
	if err != nil {
		return fmt.Errorf("create admins group: %w", err)
	}

	firstClassGroup, err := q.CreateGroup(ctx, dbstore.CreateGroupParams{
		Name:        "first_class_analysts",
		Description: sql.NullString{String: "Analysts restricted to first-class passengers", Valid: true},
	})
	if err != nil {
		return fmt.Errorf("create first_class_analysts group: %w", err)
	}

	survivorGroup, err := q.CreateGroup(ctx, dbstore.CreateGroupParams{
		Name:        "survivor_researchers",
		Description: sql.NullString{String: "Researchers restricted to survivors", Valid: true},
	})
	if err != nil {
		return fmt.Errorf("create survivor_researchers group: %w", err)
	}

	// --- Group memberships ---
	if err := q.AddGroupMember(ctx, dbstore.AddGroupMemberParams{
		GroupID: adminsGroup.ID, MemberType: "user", MemberID: adminUser.ID,
	}); err != nil {
		return fmt.Errorf("add admin to admins: %w", err)
	}
	if err := q.AddGroupMember(ctx, dbstore.AddGroupMemberParams{
		GroupID: firstClassGroup.ID, MemberType: "user", MemberID: analyst1.ID,
	}); err != nil {
		return fmt.Errorf("add analyst1 to first_class_analysts: %w", err)
	}
	if err := q.AddGroupMember(ctx, dbstore.AddGroupMemberParams{
		GroupID: survivorGroup.ID, MemberType: "user", MemberID: researcher1.ID,
	}); err != nil {
		return fmt.Errorf("add researcher1 to survivor_researchers: %w", err)
	}

	// --- Lookup DuckLake IDs ---
	schemaID, err := cat.LookupSchemaID(ctx, "main")
	if err != nil {
		return fmt.Errorf("lookup schema: %w", err)
	}

	titanicID, _, _, err := cat.LookupTableID(ctx, "titanic")
	if err != nil {
		return fmt.Errorf("lookup titanic: %w", err)
	}

	// --- Privilege grants ---
	_, err = q.GrantPrivilege(ctx, dbstore.GrantPrivilegeParams{
		PrincipalID: adminsGroup.ID, PrincipalType: "group",
		SecurableType: domain.SecurableCatalog, SecurableID: domain.CatalogID,
		Privilege: domain.PrivAllPrivileges,
	})
	if err != nil {
		return fmt.Errorf("grant admins ALL_PRIVILEGES: %w", err)
	}

	_, err = q.GrantPrivilege(ctx, dbstore.GrantPrivilegeParams{
		PrincipalID: firstClassGroup.ID, PrincipalType: "group",
		SecurableType: domain.SecurableSchema, SecurableID: schemaID,
		Privilege: domain.PrivUsage,
	})
	if err != nil {
		return fmt.Errorf("grant first_class USAGE: %w", err)
	}
	_, err = q.GrantPrivilege(ctx, dbstore.GrantPrivilegeParams{
		PrincipalID: firstClassGroup.ID, PrincipalType: "group",
		SecurableType: domain.SecurableTable, SecurableID: titanicID,
		Privilege: domain.PrivSelect,
	})
	if err != nil {
		return fmt.Errorf("grant first_class SELECT: %w", err)
	}

	_, err = q.GrantPrivilege(ctx, dbstore.GrantPrivilegeParams{
		PrincipalID: survivorGroup.ID, PrincipalType: "group",
		SecurableType: domain.SecurableSchema, SecurableID: schemaID,
		Privilege: domain.PrivUsage,
	})
	if err != nil {
		return fmt.Errorf("grant survivor USAGE: %w", err)
	}
	_, err = q.GrantPrivilege(ctx, dbstore.GrantPrivilegeParams{
		PrincipalID: survivorGroup.ID, PrincipalType: "group",
		SecurableType: domain.SecurableTable, SecurableID: titanicID,
		Privilege: domain.PrivSelect,
	})
	if err != nil {
		return fmt.Errorf("grant survivor SELECT: %w", err)
	}

	// --- Row filters ---
	firstClassFilter, err := q.CreateRowFilter(ctx, dbstore.CreateRowFilterParams{
		TableID:     titanicID,
		FilterSql:   `"Pclass" = 1`,
		Description: sql.NullString{String: "Only first-class passengers", Valid: true},
	})
	if err != nil {
		return fmt.Errorf("create first-class row filter: %w", err)
	}
	if err := q.BindRowFilter(ctx, dbstore.BindRowFilterParams{
		RowFilterID: firstClassFilter.ID, PrincipalID: firstClassGroup.ID, PrincipalType: "group",
	}); err != nil {
		return fmt.Errorf("bind first-class row filter: %w", err)
	}

	// --- Column masks ---
	nameMask, err := q.CreateColumnMask(ctx, dbstore.CreateColumnMaskParams{
		TableID:        titanicID,
		ColumnName:     "Name",
		MaskExpression: `'***'`,
		Description:    sql.NullString{String: "Hide passenger names", Valid: true},
	})
	if err != nil {
		return fmt.Errorf("create Name column mask: %w", err)
	}
	if err := q.BindColumnMask(ctx, dbstore.BindColumnMaskParams{
		ColumnMaskID: nameMask.ID, PrincipalID: firstClassGroup.ID,
		PrincipalType: "group", SeeOriginal: 0,
	}); err != nil {
		return fmt.Errorf("bind Name mask for analysts: %w", err)
	}
	if err := q.BindColumnMask(ctx, dbstore.BindColumnMaskParams{
		ColumnMaskID: nameMask.ID, PrincipalID: survivorGroup.ID,
		PrincipalType: "group", SeeOriginal: 1,
	}); err != nil {
		return fmt.Errorf("bind Name mask for researchers: %w", err)
	}

	return nil
}
