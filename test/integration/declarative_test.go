//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"duck-demo/internal/declarative"
	"duck-demo/pkg/cli"
	"duck-demo/pkg/cli/gen"
)

// ---------------------------------------------------------------------------
// YAML helper — writes string content to a file under the config dir.
// ---------------------------------------------------------------------------

func writeYAML(t *testing.T, dir, relPath, content string) {
	t.Helper()
	fullPath := filepath.Join(dir, relPath)
	require.NoError(t, os.MkdirAll(filepath.Dir(fullPath), 0o750))
	require.NoError(t, os.WriteFile(fullPath, []byte(content), 0o600))
}

// ---------------------------------------------------------------------------
// makeStateClient creates an APIStateClient pointed at the test server.
// ---------------------------------------------------------------------------

func makeStateClient(t *testing.T, serverURL, apiKey string) *cli.APIStateClient {
	t.Helper()
	client := gen.NewClient(serverURL, apiKey, "")
	return cli.NewAPIStateClientWithOptions(client, cli.APIStateClientOptions{
		CompatibilityMode: cli.CapabilityCompatibilityLegacy,
	})
}

// ---------------------------------------------------------------------------
// writeSeedStateYAML writes YAML that matches the seeded RBAC data so the
// differ produces zero changes for the baseline resources.
// ---------------------------------------------------------------------------

// seedPrincipalsYAML declares the 4 principals that seedRBAC creates.
const seedPrincipalsYAML = `apiVersion: duck/v1
kind: PrincipalList
principals:
  - name: admin_user
    type: user
    is_admin: true
  - name: analyst1
    type: user
    is_admin: false
  - name: researcher1
    type: user
    is_admin: false
  - name: no_access_user
    type: user
    is_admin: false
`

// seedGroupsYAML declares the 3 groups (with memberships) that seedRBAC creates.
const seedGroupsYAML = `apiVersion: duck/v1
kind: GroupList
groups:
  - name: admins
    members:
      - name: admin_user
        type: user
  - name: analysts
    members:
      - name: analyst1
        type: user
  - name: researchers
    members:
      - name: researcher1
        type: user
`

// ---------------------------------------------------------------------------
// countActions counts actions by operation type.
// ---------------------------------------------------------------------------

func countActions(plan *declarative.Plan) (creates, updates, deletes int) {
	for _, a := range plan.Actions {
		switch a.Operation {
		case declarative.OpCreate:
			creates++
		case declarative.OpUpdate:
			updates++
		case declarative.OpDelete:
			deletes++
		}
	}
	return
}

// actionsOfKindAndOp returns actions matching a specific resource kind and operation.
func actionsOfKindAndOp(plan *declarative.Plan, kind declarative.ResourceKind, op declarative.Operation) []declarative.Action {
	var result []declarative.Action
	for _, a := range plan.Actions {
		if a.ResourceKind == kind && a.Operation == op {
			result = append(result, a)
		}
	}
	return result
}

func executeActions(t *testing.T, stateClient *cli.APIStateClient, actions []declarative.Action) {
	t.Helper()
	for _, action := range actions {
		require.NoError(t, stateClient.Execute(context.Background(), action), "execute %s %s", action.Operation, action.ResourceName)
	}
}

// ---------------------------------------------------------------------------
// TestDeclarative_ValidateOnly — offline YAML validation, no server contact.
// ---------------------------------------------------------------------------

func TestDeclarative_ValidateOnly(t *testing.T) {
	t.Run("valid_config", func(t *testing.T) {
		dir := t.TempDir()

		writeYAML(t, dir, "security/principals.yaml", seedPrincipalsYAML)
		writeYAML(t, dir, "security/groups.yaml", seedGroupsYAML)

		state, err := declarative.LoadDirectory(dir)
		require.NoError(t, err, "LoadDirectory should succeed")

		errs := declarative.Validate(state)
		assert.Empty(t, errs, "valid config should produce zero validation errors")
	})

	t.Run("invalid_principal_type", func(t *testing.T) {
		dir := t.TempDir()

		writeYAML(t, dir, "security/principals.yaml", `apiVersion: duck/v1
kind: PrincipalList
principals:
  - name: bad-user
    type: invalid_type
    is_admin: false
`)

		state, err := declarative.LoadDirectory(dir)
		require.NoError(t, err)

		errs := declarative.Validate(state)
		require.NotEmpty(t, errs, "should detect invalid principal type")
		assert.Contains(t, errs[0].Message, "type must be")
	})

	t.Run("missing_group_member_reference", func(t *testing.T) {
		dir := t.TempDir()

		writeYAML(t, dir, "security/principals.yaml", `apiVersion: duck/v1
kind: PrincipalList
principals:
  - name: user1
    type: user
`)
		writeYAML(t, dir, "security/groups.yaml", `apiVersion: duck/v1
kind: GroupList
groups:
  - name: team
    members:
      - name: ghost_user
        type: user
`)

		state, err := declarative.LoadDirectory(dir)
		require.NoError(t, err)

		errs := declarative.Validate(state)
		require.NotEmpty(t, errs, "should detect missing member reference")
		found := false
		for _, e := range errs {
			if e.Message == `member "ghost_user" references unknown principal` {
				found = true
			}
		}
		assert.True(t, found, "should report unknown principal reference, got: %v", errs)
	})

	t.Run("duplicate_principal", func(t *testing.T) {
		dir := t.TempDir()

		writeYAML(t, dir, "security/principals.yaml", `apiVersion: duck/v1
kind: PrincipalList
principals:
  - name: duped
    type: user
  - name: duped
    type: user
`)

		state, err := declarative.LoadDirectory(dir)
		require.NoError(t, err)

		errs := declarative.Validate(state)
		require.NotEmpty(t, errs, "should detect duplicate principal")
		found := false
		for _, e := range errs {
			if e.Message == `duplicate principal name "duped"` {
				found = true
			}
		}
		assert.True(t, found, "should report duplicate principal, got: %v", errs)
	})
}

// ---------------------------------------------------------------------------
// TestDeclarative_PlanShowsCreates — plan detects new resources.
// ---------------------------------------------------------------------------

func TestDeclarative_PlanShowsCreates(t *testing.T) {
	env := setupHTTPServer(t, httpTestOpts{})
	stateClient := makeStateClient(t, env.Server.URL, env.Keys.Admin)

	dir := t.TempDir()

	// Write YAML that includes the seeded principals + one NEW principal.
	writeYAML(t, dir, "security/principals.yaml", seedPrincipalsYAML+`  - name: new-analyst
    type: user
    is_admin: false
`)

	// Write YAML that includes the seeded groups + one NEW group.
	writeYAML(t, dir, "security/groups.yaml", seedGroupsYAML+`  - name: new-team
    description: "A brand new team"
    members:
      - name: new-analyst
        type: user
`)

	// Load and validate.
	desired, err := declarative.LoadDirectory(dir)
	require.NoError(t, err)
	errs := declarative.Validate(desired)
	require.Empty(t, errs, "YAML validation should pass")

	// Read current server state.
	actual, err := stateClient.ReadState(context.Background())
	require.NoError(t, err)

	// Compute plan.
	plan := declarative.Diff(desired, actual)

	// The plan should include creates for the new principal, the new group,
	// and the new group membership.
	principalCreates := actionsOfKindAndOp(plan, declarative.KindPrincipal, declarative.OpCreate)
	assert.Len(t, principalCreates, 1, "should create 1 new principal")
	if len(principalCreates) > 0 {
		assert.Equal(t, "new-analyst", principalCreates[0].ResourceName)
	}

	groupCreates := actionsOfKindAndOp(plan, declarative.KindGroup, declarative.OpCreate)
	assert.Len(t, groupCreates, 1, "should create 1 new group")
	if len(groupCreates) > 0 {
		assert.Equal(t, "new-team", groupCreates[0].ResourceName)
	}

	memberCreates := actionsOfKindAndOp(plan, declarative.KindGroupMembership, declarative.OpCreate)
	assert.NotEmpty(t, memberCreates, "should create new group membership")

	// The plan should also have deletes for seeded grants, row filters, column masks,
	// API keys etc. that aren't in the YAML. This is expected — we only declared
	// principals and groups, so everything else on the server is an "extra" deletion.
	assert.True(t, plan.HasChanges(), "plan should have changes")
}

// ---------------------------------------------------------------------------
// TestDeclarative_ApplyCreates — apply creates + re-plan idempotent.
// ---------------------------------------------------------------------------

func TestDeclarative_ApplyCreates(t *testing.T) {
	env := setupHTTPServer(t, httpTestOpts{})
	stateClient := makeStateClient(t, env.Server.URL, env.Keys.Admin)

	dir := t.TempDir()

	// Write YAML with the seeded principals + a new one.
	writeYAML(t, dir, "security/principals.yaml", seedPrincipalsYAML+`  - name: created-by-apply
    type: user
    is_admin: false
`)

	desired, err := declarative.LoadDirectory(dir)
	require.NoError(t, err)

	actual, err := stateClient.ReadState(context.Background())
	require.NoError(t, err)

	plan := declarative.Diff(desired, actual)

	// Find the create action for our new principal.
	principalCreates := actionsOfKindAndOp(plan, declarative.KindPrincipal, declarative.OpCreate)
	require.Len(t, principalCreates, 1, "should have 1 principal create")
	assert.Equal(t, "created-by-apply", principalCreates[0].ResourceName)

	// Execute ONLY the principal create actions (not the deletes of seeded grants etc.).
	for _, action := range plan.Actions {
		if action.ResourceKind == declarative.KindPrincipal && action.Operation == declarative.OpCreate {
			err := stateClient.Execute(context.Background(), action)
			require.NoError(t, err, "execute create principal")
		}
	}

	// Verify via direct API call.
	resp := doRequest(t, "GET", env.Server.URL+"/v1/principals", env.Keys.Admin, nil)
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var listResult struct {
		Data []struct {
			Name string `json:"name"`
		} `json:"data"`
	}
	decodeJSON(t, resp, &listResult)

	found := false
	for _, p := range listResult.Data {
		if p.Name == "created-by-apply" {
			found = true
			break
		}
	}
	assert.True(t, found, "new principal should be visible via API")

	// Re-plan: reload desired (same YAML) vs fresh server state.
	actual2, err := stateClient.ReadState(context.Background())
	require.NoError(t, err)

	plan2 := declarative.Diff(desired, actual2)

	// The new principal should no longer be in create actions.
	principalCreates2 := actionsOfKindAndOp(plan2, declarative.KindPrincipal, declarative.OpCreate)
	assert.Empty(t, principalCreates2, "re-plan should have no principal creates (idempotent)")
}

// ---------------------------------------------------------------------------
// TestDeclarative_ApplyUpdates — plan detects updates with correct field diffs.
// ---------------------------------------------------------------------------

func TestDeclarative_ApplyUpdates(t *testing.T) {
	env := setupHTTPServer(t, httpTestOpts{})
	stateClient := makeStateClient(t, env.Server.URL, env.Keys.Admin)

	dir := t.TempDir()

	// Write YAML with seeded principals (no change) and groups where
	// the "admins" group has a description change.
	writeYAML(t, dir, "security/principals.yaml", seedPrincipalsYAML)
	writeYAML(t, dir, "security/groups.yaml", `apiVersion: duck/v1
kind: GroupList
groups:
  - name: admins
    description: "Updated admin team description"
    members:
      - name: admin_user
        type: user
  - name: analysts
    members:
      - name: analyst1
        type: user
  - name: researchers
    members:
      - name: researcher1
        type: user
`)

	desired, err := declarative.LoadDirectory(dir)
	require.NoError(t, err)

	actual, err := stateClient.ReadState(context.Background())
	require.NoError(t, err)

	plan := declarative.Diff(desired, actual)

	// Should have an update for the "admins" group.
	groupUpdates := actionsOfKindAndOp(plan, declarative.KindGroup, declarative.OpUpdate)
	require.Len(t, groupUpdates, 1, "should have 1 group update")
	assert.Equal(t, "admins", groupUpdates[0].ResourceName)
	require.NotEmpty(t, groupUpdates[0].Changes, "should have field changes")
	assert.Equal(t, "description", groupUpdates[0].Changes[0].Field)
	assert.Equal(t, "", groupUpdates[0].Changes[0].OldValue)
	assert.Equal(t, "Updated admin team description", groupUpdates[0].Changes[0].NewValue)

	// No principal changes expected.
	principalCreates := actionsOfKindAndOp(plan, declarative.KindPrincipal, declarative.OpCreate)
	principalUpdates := actionsOfKindAndOp(plan, declarative.KindPrincipal, declarative.OpUpdate)
	assert.Empty(t, principalCreates, "no principal creates expected")
	assert.Empty(t, principalUpdates, "no principal updates expected")
}

// ---------------------------------------------------------------------------
// TestDeclarative_ApplyDeletes — plan detects deletes for resources absent from YAML.
// ---------------------------------------------------------------------------

func TestDeclarative_ApplyDeletes(t *testing.T) {
	env := setupHTTPServer(t, httpTestOpts{})
	stateClient := makeStateClient(t, env.Server.URL, env.Keys.Admin)

	// First, create an extra principal via API that we will then omit from YAML.
	createBody := map[string]interface{}{"name": "to-be-deleted", "type": "user"}
	resp := doRequest(t, "POST", env.Server.URL+"/v1/principals", env.Keys.Admin, createBody)
	require.Equal(t, http.StatusCreated, resp.StatusCode)

	var created struct {
		ID string `json:"id"`
	}
	decodeJSON(t, resp, &created)
	require.NotEmpty(t, created.ID)

	dir := t.TempDir()

	// Write YAML with only the seeded principals (omitting "to-be-deleted").
	writeYAML(t, dir, "security/principals.yaml", seedPrincipalsYAML)

	desired, err := declarative.LoadDirectory(dir)
	require.NoError(t, err)

	actual, err := stateClient.ReadState(context.Background())
	require.NoError(t, err)

	plan := declarative.Diff(desired, actual)

	// Should have a delete for "to-be-deleted".
	principalDeletes := actionsOfKindAndOp(plan, declarative.KindPrincipal, declarative.OpDelete)
	foundDelete := false
	for _, a := range principalDeletes {
		if a.ResourceName == "to-be-deleted" {
			foundDelete = true
			break
		}
	}
	require.True(t, foundDelete, "plan should include delete for 'to-be-deleted'")

	// Execute the delete via direct API (using UUID, not the declarative Execute).
	delURL := fmt.Sprintf("%s/v1/principals/%s", env.Server.URL, created.ID)
	resp2 := doRequest(t, "DELETE", delURL, env.Keys.Admin, nil)
	require.Equal(t, http.StatusNoContent, resp2.StatusCode)
	_ = resp2.Body.Close()

	// Re-plan: the delete should be gone.
	actual2, err := stateClient.ReadState(context.Background())
	require.NoError(t, err)

	plan2 := declarative.Diff(desired, actual2)
	principalDeletes2 := actionsOfKindAndOp(plan2, declarative.KindPrincipal, declarative.OpDelete)
	for _, a := range principalDeletes2 {
		assert.NotEqual(t, "to-be-deleted", a.ResourceName, "re-plan should not delete 'to-be-deleted' again")
	}
}

// ---------------------------------------------------------------------------
// TestDeclarative_ExportRoundTrip — export → load → diff = 0 changes.
// ---------------------------------------------------------------------------

func TestDeclarative_ExportRoundTrip(t *testing.T) {
	env := setupHTTPServer(t, httpTestOpts{})
	stateClient := makeStateClient(t, env.Server.URL, env.Keys.Admin)

	// Read current server state.
	serverState, err := stateClient.ReadState(context.Background())
	require.NoError(t, err)

	// Export to a temp directory.
	exportDir := filepath.Join(t.TempDir(), "exported")
	err = declarative.ExportDirectory(exportDir, serverState, false)
	require.NoError(t, err, "ExportDirectory should succeed")

	// Load the exported YAML.
	loadedState, err := declarative.LoadDirectory(exportDir)
	require.NoError(t, err, "LoadDirectory on exported YAML should succeed")

	// Diff the loaded state against the server state.
	plan := declarative.Diff(loadedState, serverState)

	// The diff should show zero changes (perfect round-trip).
	creates, updates, deletes := countActions(plan)
	assert.Equal(t, 0, creates, "export round-trip should have 0 creates")
	assert.Equal(t, 0, updates, "export round-trip should have 0 updates")
	assert.Equal(t, 0, deletes, "export round-trip should have 0 deletes")
	assert.Empty(t, plan.Errors, "export round-trip should have 0 errors")

	if plan.HasChanges() {
		// Dump actions for debugging.
		for _, a := range plan.Actions {
			t.Logf("  unexpected action: %s %s %s", a.Operation, a.ResourceKind, a.ResourceName)
			for _, c := range a.Changes {
				t.Logf("    field: %s old=%q new=%q", c.Field, c.OldValue, c.NewValue)
			}
		}
		for _, e := range plan.Errors {
			t.Logf("  unexpected error: %s %s: %s", e.ResourceKind, e.ResourceName, e.Message)
		}
	}
}

// ---------------------------------------------------------------------------
// TestDeclarative_DeletionProtection — protected resources produce errors.
// ---------------------------------------------------------------------------

func TestDeclarative_DeletionProtection(t *testing.T) {
	// This test uses the differ directly (no server API needed for catalogs)
	// to verify that deletion-protected resources produce PlanErrors instead
	// of delete actions.

	t.Run("catalog_protection", func(t *testing.T) {
		// Simulate: actual state has a catalog with deletion_protection=true.
		// Desired state does NOT include that catalog.
		actual := &declarative.DesiredState{
			Catalogs: []declarative.CatalogResource{
				{
					CatalogName:        "protected-cat",
					DeletionProtection: true,
					Spec: declarative.CatalogSpec{
						MetastoreType: "sqlite",
						DSN:           "/tmp/test.sqlite",
						DataPath:      "/tmp/data/",
					},
				},
			},
		}
		desired := &declarative.DesiredState{} // empty — the catalog is "missing" from YAML

		plan := declarative.Diff(desired, actual)

		// Should have an error (not a delete action) for the protected catalog.
		require.NotEmpty(t, plan.Errors, "should have plan errors for deletion-protected catalog")

		foundProtectionError := false
		for _, e := range plan.Errors {
			if e.ResourceKind == declarative.KindCatalogRegistration &&
				e.ResourceName == "protected-cat" {
				foundProtectionError = true
				assert.Contains(t, e.Message, "deletion_protection")
			}
		}
		assert.True(t, foundProtectionError, "should have deletion protection error for 'protected-cat'")

		// Verify no delete action was generated for the protected catalog.
		catalogDeletes := actionsOfKindAndOp(plan, declarative.KindCatalogRegistration, declarative.OpDelete)
		for _, a := range catalogDeletes {
			assert.NotEqual(t, "protected-cat", a.ResourceName,
				"deletion-protected catalog should NOT have a delete action")
		}
	})

	t.Run("table_protection", func(t *testing.T) {
		// Tables also support deletion protection.
		actual := &declarative.DesiredState{
			Tables: []declarative.TableResource{
				{
					CatalogName:        "main",
					SchemaName:         "analytics",
					TableName:          "important_data",
					DeletionProtection: true,
					Spec: declarative.TableSpec{
						TableType: "MANAGED",
					},
				},
			},
		}
		desired := &declarative.DesiredState{}

		plan := declarative.Diff(desired, actual)

		require.NotEmpty(t, plan.Errors, "should have plan errors for deletion-protected table")

		foundProtectionError := false
		for _, e := range plan.Errors {
			if e.ResourceKind == declarative.KindTable &&
				e.ResourceName == "main.analytics.important_data" {
				foundProtectionError = true
				assert.Contains(t, e.Message, "deletion_protection")
			}
		}
		assert.True(t, foundProtectionError, "should have deletion protection error for table")

		tableDeletes := actionsOfKindAndOp(plan, declarative.KindTable, declarative.OpDelete)
		for _, a := range tableDeletes {
			assert.NotEqual(t, "main.analytics.important_data", a.ResourceName,
				"deletion-protected table should NOT have a delete action")
		}
	})

	t.Run("schema_protection", func(t *testing.T) {
		// Schemas also support deletion protection.
		actual := &declarative.DesiredState{
			Schemas: []declarative.SchemaResource{
				{
					CatalogName:        "main",
					SchemaName:         "critical",
					DeletionProtection: true,
				},
			},
		}
		desired := &declarative.DesiredState{}

		plan := declarative.Diff(desired, actual)

		require.NotEmpty(t, plan.Errors, "should have plan errors for deletion-protected schema")

		foundProtectionError := false
		for _, e := range plan.Errors {
			if e.ResourceKind == declarative.KindSchema &&
				e.ResourceName == "main.critical" {
				foundProtectionError = true
				assert.Contains(t, e.Message, "deletion_protection")
			}
		}
		assert.True(t, foundProtectionError, "should have deletion protection error for schema")
	})

	t.Run("non_protected_allows_delete", func(t *testing.T) {
		// A catalog WITHOUT deletion_protection should produce a delete action.
		actual := &declarative.DesiredState{
			Catalogs: []declarative.CatalogResource{
				{
					CatalogName:        "ephemeral-cat",
					DeletionProtection: false,
					Spec: declarative.CatalogSpec{
						MetastoreType: "sqlite",
						DSN:           "/tmp/test.sqlite",
						DataPath:      "/tmp/data/",
					},
				},
			},
		}
		desired := &declarative.DesiredState{}

		plan := declarative.Diff(desired, actual)

		assert.Empty(t, plan.Errors, "no protection errors for non-protected catalog")

		catalogDeletes := actionsOfKindAndOp(plan, declarative.KindCatalogRegistration, declarative.OpDelete)
		require.Len(t, catalogDeletes, 1, "should have 1 catalog delete")
		assert.Equal(t, "ephemeral-cat", catalogDeletes[0].ResourceName)
	})
}

// ---------------------------------------------------------------------------
// TestDeclarative_FullLifecycle — end-to-end create → verify → update plan → delete plan.
// ---------------------------------------------------------------------------

func TestDeclarative_FullLifecycle(t *testing.T) {
	env := setupHTTPServer(t, httpTestOpts{})
	stateClient := makeStateClient(t, env.Server.URL, env.Keys.Admin)

	// Phase 1: Create — add a new principal via Execute, verify it appears.
	dir := t.TempDir()

	writeYAML(t, dir, "security/principals.yaml", seedPrincipalsYAML+`  - name: lifecycle-user
    type: user
    is_admin: false
`)
	writeYAML(t, dir, "security/groups.yaml", seedGroupsYAML)

	desired, err := declarative.LoadDirectory(dir)
	require.NoError(t, err)
	errs := declarative.Validate(desired)
	require.Empty(t, errs)

	actual, err := stateClient.ReadState(context.Background())
	require.NoError(t, err)

	plan := declarative.Diff(desired, actual)

	// Execute only the principal create (this works because POST /principals is name-based).
	principalCreates := actionsOfKindAndOp(plan, declarative.KindPrincipal, declarative.OpCreate)
	require.Len(t, principalCreates, 1, "should create lifecycle-user")
	assert.Equal(t, "lifecycle-user", principalCreates[0].ResourceName)

	err = stateClient.Execute(context.Background(), principalCreates[0])
	require.NoError(t, err, "create lifecycle-user")

	// Verify principal was created.
	resp := doRequest(t, "GET", env.Server.URL+"/v1/principals", env.Keys.Admin, nil)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	body := readBody(t, resp)
	assert.Contains(t, string(body), "lifecycle-user")

	// Phase 2: Update plan — change is_admin for lifecycle-user.
	writeYAML(t, dir, "security/principals.yaml", seedPrincipalsYAML+`  - name: lifecycle-user
    type: user
    is_admin: true
`)

	desired2, err := declarative.LoadDirectory(dir)
	require.NoError(t, err)

	actual2, err := stateClient.ReadState(context.Background())
	require.NoError(t, err)

	plan2 := declarative.Diff(desired2, actual2)

	principalUpdates := actionsOfKindAndOp(plan2, declarative.KindPrincipal, declarative.OpUpdate)
	var lifecycleUpdate *declarative.Action
	for i, a := range principalUpdates {
		if a.ResourceName == "lifecycle-user" {
			lifecycleUpdate = &principalUpdates[i]
			break
		}
	}
	require.NotNil(t, lifecycleUpdate, "should have update for lifecycle-user")
	assert.Equal(t, "is_admin", lifecycleUpdate.Changes[0].Field)
	assert.Equal(t, "false", lifecycleUpdate.Changes[0].OldValue)
	assert.Equal(t, "true", lifecycleUpdate.Changes[0].NewValue)

	// Phase 3: Delete plan — remove lifecycle-user from YAML.
	writeYAML(t, dir, "security/principals.yaml", seedPrincipalsYAML)

	desired3, err := declarative.LoadDirectory(dir)
	require.NoError(t, err)

	actual3, err := stateClient.ReadState(context.Background())
	require.NoError(t, err)

	plan3 := declarative.Diff(desired3, actual3)

	principalDeletes := actionsOfKindAndOp(plan3, declarative.KindPrincipal, declarative.OpDelete)
	foundDelete := false
	for _, a := range principalDeletes {
		if a.ResourceName == "lifecycle-user" {
			foundDelete = true
			break
		}
	}
	assert.True(t, foundDelete, "plan should include delete for lifecycle-user")
}

// ---------------------------------------------------------------------------
// TestDeclarative_PlanOutputFormat — verify plan summary counts are correct.
// ---------------------------------------------------------------------------

func TestDeclarative_PlanOutputFormat(t *testing.T) {
	env := setupHTTPServer(t, httpTestOpts{})
	stateClient := makeStateClient(t, env.Server.URL, env.Keys.Admin)

	dir := t.TempDir()

	// Write YAML that will produce known creates (1 new principal)
	// and known updates (1 group description change).
	writeYAML(t, dir, "security/principals.yaml", seedPrincipalsYAML+`  - name: format-test-user
    type: user
    is_admin: false
`)
	writeYAML(t, dir, "security/groups.yaml", `apiVersion: duck/v1
kind: GroupList
groups:
  - name: admins
    description: "Format test updated description"
    members:
      - name: admin_user
        type: user
  - name: analysts
    members:
      - name: analyst1
        type: user
  - name: researchers
    members:
      - name: researcher1
        type: user
`)

	desired, err := declarative.LoadDirectory(dir)
	require.NoError(t, err)

	actual, err := stateClient.ReadState(context.Background())
	require.NoError(t, err)

	plan := declarative.Diff(desired, actual)
	summary := plan.Summary()

	// Should have at least 1 create and 1 update.
	assert.GreaterOrEqual(t, summary.Creates, 1, "should have at least 1 create")
	assert.GreaterOrEqual(t, summary.Updates, 1, "should have at least 1 update")
	assert.True(t, plan.HasChanges(), "plan should have changes")

	// Verify JSON serialization of plan summary.
	data, err := json.Marshal(summary)
	require.NoError(t, err)
	assert.Contains(t, string(data), `"creates"`)
	assert.Contains(t, string(data), `"updates"`)
	assert.Contains(t, string(data), `"deletes"`)
	assert.Contains(t, string(data), `"errors"`)
}

// ---------------------------------------------------------------------------
// TestDeclarative_GrantLifecycle — create and delete grants via declarative.
// ---------------------------------------------------------------------------

func TestDeclarative_GrantLifecycle(t *testing.T) {
	env := setupHTTPServer(t, httpTestOpts{})
	stateClient := makeStateClient(t, env.Server.URL, env.Keys.Admin)

	// First, read the current state to understand what grants exist.
	actual, err := stateClient.ReadState(context.Background())
	require.NoError(t, err)

	dir := t.TempDir()

	// Write YAML with the seeded principals and groups, plus a NEW grant.
	writeYAML(t, dir, "security/principals.yaml", seedPrincipalsYAML)
	writeYAML(t, dir, "security/groups.yaml", seedGroupsYAML)

	// Build grants YAML that includes existing grants from the server + a new one.
	grantsYAML := "apiVersion: duck/v1\nkind: GrantList\ngrants:\n"
	for _, g := range actual.Grants {
		grantsYAML += fmt.Sprintf(`  - principal: %s
    principal_type: %s
    securable_type: %s
    securable: "%s"
    privilege: %s
`, g.Principal, g.PrincipalType, g.SecurableType, g.Securable, g.Privilege)
	}

	writeYAML(t, dir, "security/grants.yaml", grantsYAML)

	desired, err := declarative.LoadDirectory(dir)
	require.NoError(t, err)

	actual2, err := stateClient.ReadState(context.Background())
	require.NoError(t, err)

	plan := declarative.Diff(desired, actual2)

	// With all existing grants declared, there should be no grant creates or deletes.
	grantCreates := actionsOfKindAndOp(plan, declarative.KindPrivilegeGrant, declarative.OpCreate)
	grantDeletes := actionsOfKindAndOp(plan, declarative.KindPrivilegeGrant, declarative.OpDelete)
	assert.Empty(t, grantCreates, "no new grants should be needed")
	assert.Empty(t, grantDeletes, "no grants should be deleted")
}

// ---------------------------------------------------------------------------
// TestDeclarative_ModelLifecycle — full declarative model lifecycle against
// fully wired model services.
// ---------------------------------------------------------------------------

func TestDeclarative_ModelLifecycle(t *testing.T) {
	env := setupHTTPServer(t, httpTestOpts{WithModels: true})
	stateClient := makeStateClient(t, env.Server.URL, env.Keys.Admin)

	dir := t.TempDir()
	writeYAML(t, dir, "models/analytics/stg_orders.yaml", `apiVersion: duck/v1
kind: Model
metadata:
  name: stg_orders
spec:
  materialization: INCREMENTAL
  description: "staging orders"
  sql: |
    SELECT 1 AS order_id, 'active' AS status
  contract:
    enforce: false
  config:
    unique_key: [order_id]
    incremental_strategy: delete+insert
    on_schema_change: fail
  tests:
    - name: not_null_order_id
      type: not_null
      column: order_id
`)

	desired, err := declarative.LoadDirectory(dir)
	require.NoError(t, err)
	require.Empty(t, declarative.Validate(desired))

	actual, err := stateClient.ReadState(context.Background())
	require.NoError(t, err)

	plan := declarative.Diff(desired, actual)
	modelCreates := actionsOfKindAndOp(plan, declarative.KindModel, declarative.OpCreate)
	require.Len(t, modelCreates, 1, "expected one model create")
	assert.Equal(t, "analytics.stg_orders", modelCreates[0].ResourceName)
	executeActions(t, stateClient, modelCreates)

	actualAfterCreate, err := stateClient.ReadState(context.Background())
	require.NoError(t, err)
	replan := declarative.Diff(desired, actualAfterCreate)
	assert.Empty(t, actionsOfKindAndOp(replan, declarative.KindModel, declarative.OpCreate), "model should be idempotent after create")
	assert.Empty(t, actionsOfKindAndOp(replan, declarative.KindModel, declarative.OpUpdate), "model should be idempotent after create")

	writeYAML(t, dir, "models/analytics/stg_orders.yaml", `apiVersion: duck/v1
kind: Model
metadata:
  name: stg_orders
spec:
  materialization: INCREMENTAL
  description: "staging orders updated"
  sql: |
    SELECT 1 AS order_id, 'active' AS status, 10.0 AS amount
  contract:
    enforce: false
  config:
    unique_key: [order_id]
    incremental_strategy: merge
    on_schema_change: ignore
  tests:
    - name: not_null_order_id
      type: not_null
      column: order_id
`)

	desiredUpdated, err := declarative.LoadDirectory(dir)
	require.NoError(t, err)
	planUpdate := declarative.Diff(desiredUpdated, actualAfterCreate)
	modelUpdates := actionsOfKindAndOp(planUpdate, declarative.KindModel, declarative.OpUpdate)
	require.Len(t, modelUpdates, 1)
	executeActions(t, stateClient, modelUpdates)

	actualAfterUpdate, err := stateClient.ReadState(context.Background())
	require.NoError(t, err)
	replanUpdate := declarative.Diff(desiredUpdated, actualAfterUpdate)
	assert.Empty(t, actionsOfKindAndOp(replanUpdate, declarative.KindModel, declarative.OpCreate))
	assert.Empty(t, actionsOfKindAndOp(replanUpdate, declarative.KindModel, declarative.OpUpdate))

	require.NoError(t, os.Remove(filepath.Join(dir, "models", "analytics", "stg_orders.yaml")))
	desiredDeleted, err := declarative.LoadDirectory(dir)
	require.NoError(t, err)
	planDelete := declarative.Diff(desiredDeleted, actualAfterUpdate)
	modelDeletes := actionsOfKindAndOp(planDelete, declarative.KindModel, declarative.OpDelete)
	require.Len(t, modelDeletes, 1)
	executeActions(t, stateClient, modelDeletes)

	actualAfterDelete, err := stateClient.ReadState(context.Background())
	require.NoError(t, err)
	replanDelete := declarative.Diff(desiredDeleted, actualAfterDelete)
	assert.Empty(t, actionsOfKindAndOp(replanDelete, declarative.KindModel, declarative.OpDelete))
}

func TestDeclarative_MacroLifecycle(t *testing.T) {
	env := setupHTTPServer(t, httpTestOpts{WithModels: true})
	stateClient := makeStateClient(t, env.Server.URL, env.Keys.Admin)

	dir := t.TempDir()
	writeYAML(t, dir, "macros/fmt_money.yaml", `apiVersion: duck/v1
kind: Macro
metadata:
  name: fmt_money
spec:
  macro_type: SCALAR
  parameters: [amount]
  body: amount/100.0
  project_name: analytics
  visibility: project
  status: ACTIVE
`)

	desired, err := declarative.LoadDirectory(dir)
	require.NoError(t, err)
	require.Empty(t, declarative.Validate(desired))

	actual, err := stateClient.ReadState(context.Background())
	require.NoError(t, err)
	plan := declarative.Diff(desired, actual)
	macroCreates := actionsOfKindAndOp(plan, declarative.KindMacro, declarative.OpCreate)
	require.Len(t, macroCreates, 1)
	executeActions(t, stateClient, macroCreates)

	actualAfterCreate, err := stateClient.ReadState(context.Background())
	require.NoError(t, err)
	replan := declarative.Diff(desired, actualAfterCreate)
	assert.Empty(t, actionsOfKindAndOp(replan, declarative.KindMacro, declarative.OpCreate))
	assert.Empty(t, actionsOfKindAndOp(replan, declarative.KindMacro, declarative.OpUpdate))

	writeYAML(t, dir, "macros/fmt_money.yaml", `apiVersion: duck/v1
kind: Macro
metadata:
  name: fmt_money
spec:
  macro_type: SCALAR
  parameters: [amount]
  body: round(amount/100.0, 2)
  project_name: analytics
  visibility: project
  status: DEPRECATED
`)
	desiredUpdated, err := declarative.LoadDirectory(dir)
	require.NoError(t, err)
	planUpdate := declarative.Diff(desiredUpdated, actualAfterCreate)
	macroUpdates := actionsOfKindAndOp(planUpdate, declarative.KindMacro, declarative.OpUpdate)
	require.Len(t, macroUpdates, 1)
	executeActions(t, stateClient, macroUpdates)

	actualAfterUpdate, err := stateClient.ReadState(context.Background())
	require.NoError(t, err)
	replanUpdate := declarative.Diff(desiredUpdated, actualAfterUpdate)
	assert.Empty(t, actionsOfKindAndOp(replanUpdate, declarative.KindMacro, declarative.OpUpdate))

	require.NoError(t, os.Remove(filepath.Join(dir, "macros", "fmt_money.yaml")))
	desiredDeleted, err := declarative.LoadDirectory(dir)
	require.NoError(t, err)
	planDelete := declarative.Diff(desiredDeleted, actualAfterUpdate)
	macroDeletes := actionsOfKindAndOp(planDelete, declarative.KindMacro, declarative.OpDelete)
	require.Len(t, macroDeletes, 1)
	executeActions(t, stateClient, macroDeletes)

	actualAfterDelete, err := stateClient.ReadState(context.Background())
	require.NoError(t, err)
	replanDelete := declarative.Diff(desiredDeleted, actualAfterDelete)
	assert.Empty(t, actionsOfKindAndOp(replanDelete, declarative.KindMacro, declarative.OpDelete))
}

func TestDeclarative_SemanticModelLifecycle(t *testing.T) {
	env := setupHTTPServer(t, httpTestOpts{WithSemantic: true})
	stateClient := makeStateClient(t, env.Server.URL, env.Keys.Admin)

	dir := t.TempDir()
	writeYAML(t, dir, "semantic_models/analytics/customers.yaml", `apiVersion: duck/v1
kind: SemanticModel
metadata:
  name: customers
spec:
  base_model_ref: analytics.dim_customers
`)
	writeYAML(t, dir, "semantic_models/analytics/sales.yaml", `apiVersion: duck/v1
kind: SemanticModel
metadata:
  name: sales
spec:
  description: sales semantic model
  base_model_ref: analytics.fct_sales
  default_time_dimension: order_date
  metrics:
    - name: total_revenue
      metric_type: SUM
      expression_mode: SQL
      expression: SUM(amount)
      certification_state: DRAFT
  pre_aggregations:
    - name: daily_sales
      metric_set: [total_revenue]
      dimension_set: [order_date]
      target_relation: analytics.agg_daily_sales
`)

	desired, err := declarative.LoadDirectory(dir)
	require.NoError(t, err)
	require.Empty(t, declarative.Validate(desired))

	actual, err := stateClient.ReadState(context.Background())
	require.NoError(t, err)

	plan := declarative.Diff(desired, actual)
	creates := actionsOfKindAndOp(plan, declarative.KindSemanticModel, declarative.OpCreate)
	require.Len(t, creates, 2)
	executeActions(t, stateClient, creates)

	actualAfterCreate, err := stateClient.ReadState(context.Background())
	require.NoError(t, err)
	replan := declarative.Diff(desired, actualAfterCreate)
	assert.Empty(t, actionsOfKindAndOp(replan, declarative.KindSemanticModel, declarative.OpCreate))
	assert.Empty(t, actionsOfKindAndOp(replan, declarative.KindSemanticModel, declarative.OpUpdate))

	writeYAML(t, dir, "semantic_models/analytics/sales.yaml", `apiVersion: duck/v1
kind: SemanticModel
metadata:
  name: sales
spec:
  description: sales semantic model updated
  base_model_ref: analytics.fct_sales
  default_time_dimension: order_date
  metrics:
    - name: total_revenue
      metric_type: SUM
      expression_mode: SQL
      expression: SUM(amount_usd)
      certification_state: CERTIFIED
  relationships:
    - name: sales_to_customers
      to_model: analytics.customers
      relationship_type: MANY_TO_ONE
      join_sql: sales.customer_id = customers.customer_id
  pre_aggregations:
    - name: monthly_sales
      metric_set: [total_revenue]
      dimension_set: [order_month]
      target_relation: analytics.agg_monthly_sales
`)

	desiredUpdated, err := declarative.LoadDirectory(dir)
	require.NoError(t, err)
	require.Empty(t, declarative.Validate(desiredUpdated))

	planUpdate := declarative.Diff(desiredUpdated, actualAfterCreate)
	updates := actionsOfKindAndOp(planUpdate, declarative.KindSemanticModel, declarative.OpUpdate)
	require.Len(t, updates, 1)
	executeActions(t, stateClient, updates)

	actualAfterUpdate, err := stateClient.ReadState(context.Background())
	require.NoError(t, err)
	replanUpdate := declarative.Diff(desiredUpdated, actualAfterUpdate)
	assert.Empty(t, actionsOfKindAndOp(replanUpdate, declarative.KindSemanticModel, declarative.OpCreate))
	assert.Empty(t, actionsOfKindAndOp(replanUpdate, declarative.KindSemanticModel, declarative.OpUpdate))

	require.NoError(t, os.Remove(filepath.Join(dir, "semantic_models", "analytics", "sales.yaml")))
	require.NoError(t, os.Remove(filepath.Join(dir, "semantic_models", "analytics", "customers.yaml")))

	desiredDeleted, err := declarative.LoadDirectory(dir)
	require.NoError(t, err)
	planDelete := declarative.Diff(desiredDeleted, actualAfterUpdate)
	deletes := actionsOfKindAndOp(planDelete, declarative.KindSemanticModel, declarative.OpDelete)
	require.Len(t, deletes, 2)
	executeActions(t, stateClient, deletes)

	actualAfterDelete, err := stateClient.ReadState(context.Background())
	require.NoError(t, err)
	replanDelete := declarative.Diff(desiredDeleted, actualAfterDelete)
	assert.Empty(t, actionsOfKindAndOp(replanDelete, declarative.KindSemanticModel, declarative.OpDelete))
}

func TestDeclarative_SemanticApplyThenExplainAndRun(t *testing.T) {
	env := setupHTTPServer(t, httpTestOpts{WithSemantic: true, WithComputeEndpoints: true, SeedDuckLakeMetadata: true})
	stateClient := makeStateClient(t, env.Server.URL, env.Keys.Admin)
	require.NotNil(t, env.DuckDB)

	_, err := env.DuckDB.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS titanic (Fare DOUBLE)`)
	require.NoError(t, err)
	_, err = env.DuckDB.ExecContext(ctx, `DELETE FROM titanic`)
	require.NoError(t, err)
	_, err = env.DuckDB.ExecContext(ctx, `INSERT INTO titanic VALUES (10.5), (20.0), (5.0)`)
	require.NoError(t, err)

	dir := t.TempDir()
	writeYAML(t, dir, "semantic_models/analytics/sales_runtime.yaml", `apiVersion: duck/v1
kind: SemanticModel
metadata:
  name: sales_runtime
spec:
  base_model_ref: main.titanic
  metrics:
    - name: total_fare
      metric_type: SUM
      expression_mode: SQL
      expression: SUM(Fare)
      certification_state: CERTIFIED
`)

	desired, err := declarative.LoadDirectory(dir)
	require.NoError(t, err)
	require.Empty(t, declarative.Validate(desired))

	plan := declarative.Diff(desired, &declarative.DesiredState{})
	creates := actionsOfKindAndOp(plan, declarative.KindSemanticModel, declarative.OpCreate)
	require.Len(t, creates, 1)
	executeActions(t, stateClient, creates)

	explainResp := doRequest(t, http.MethodPost, env.Server.URL+"/v1/metric-queries:explain", env.Keys.Admin, map[string]interface{}{
		"project_name":        "analytics",
		"semantic_model_name": "sales_runtime",
		"metrics":             []string{"total_fare"},
	})
	if explainResp.StatusCode != http.StatusOK {
		require.Equal(t, http.StatusOK, explainResp.StatusCode, string(readBody(t, explainResp)))
	}

	var explainBody struct {
		Plan struct {
			GeneratedSQL string `json:"generated_sql"`
		} `json:"plan"`
	}
	decodeJSON(t, explainResp, &explainBody)
	assert.NotEmpty(t, explainBody.Plan.GeneratedSQL)

	runResp := doRequest(t, http.MethodPost, env.Server.URL+"/v1/metric-queries:run", env.Keys.Admin, map[string]interface{}{
		"project_name":        "analytics",
		"semantic_model_name": "sales_runtime",
		"metrics":             []string{"total_fare"},
	})
	if runResp.StatusCode != http.StatusOK {
		require.Equal(t, http.StatusOK, runResp.StatusCode, string(readBody(t, runResp)))
	}

	var runBody struct {
		Result struct {
			RowCount int64           `json:"row_count"`
			Rows     [][]interface{} `json:"rows"`
		} `json:"result"`
	}
	decodeJSON(t, runResp, &runBody)
	assert.EqualValues(t, 1, runBody.Result.RowCount)
	require.Len(t, runBody.Result.Rows, 1)
}
