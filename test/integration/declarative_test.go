//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"

	cueformat "cuelang.org/go/cue/format"
	cueyaml "cuelang.org/go/encoding/yaml"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"

	"github.com/Yacobolo/quackstack/internal/declarative"
	"github.com/Yacobolo/quackstack/pkg/cli"
	"github.com/Yacobolo/quackstack/pkg/cli/apiruntime"
)

// ---------------------------------------------------------------------------
// Legacy fixture helper — accepts old YAML document snippets and writes CUE.
// ---------------------------------------------------------------------------

func writeYAML(t *testing.T, dir, relPath, content string) {
	t.Helper()
	ensureCUEModule(t, dir)
	fullPath := filepath.Join(dir, cueConfigPath(relPath))
	require.NoError(t, os.MkdirAll(filepath.Dir(fullPath), 0o750))
	require.NoError(t, os.WriteFile(fullPath, []byte(legacyConfigToCUE(t, relPath, content)), 0o600))
}

func ensureCUEModule(t *testing.T, dir string) {
	t.Helper()
	moduleFile := filepath.Join(dir, "cue.mod", "module.cue")
	if _, err := os.Stat(moduleFile); err == nil {
		return
	}
	require.NoError(t, os.MkdirAll(filepath.Dir(moduleFile), 0o750))
	require.NoError(t, os.WriteFile(moduleFile, []byte(`module: "quackstack.local/integration-config"
language: {
	version: "v0.14.0"
}
`), 0o600))
}

func cueConfigPath(relPath string) string {
	ext := filepath.Ext(relPath)
	switch strings.ToLower(ext) {
	case ".yaml", ".yml":
		return strings.TrimSuffix(relPath, ext) + ".cue"
	default:
		return relPath
	}
}

func legacyConfigToCUE(t *testing.T, relPath, content string) string {
	t.Helper()

	var doc declarative.Document
	require.NoError(t, yaml.Unmarshal([]byte(content), &doc))

	var platform map[string]any
	switch doc.Kind {
	case declarative.KindNamePrincipalList:
		var parsed declarative.PrincipalListDoc
		require.NoError(t, yaml.Unmarshal([]byte(content), &parsed))
		principals := map[string]any{}
		for _, principal := range parsed.Principals {
			principals[principal.Name] = map[string]any{
				"type":     principal.Type,
				"is_admin": principal.IsAdmin,
			}
		}
		platform = map[string]any{"security": map[string]any{"principals": principals}}
	case declarative.KindNameGroupList:
		var parsed declarative.GroupListDoc
		require.NoError(t, yaml.Unmarshal([]byte(content), &parsed))
		groups := map[string]any{}
		for _, group := range parsed.Groups {
			groups[group.Name] = map[string]any{
				"description": group.Description,
				"members":     group.Members,
			}
		}
		platform = map[string]any{"security": map[string]any{"groups": groups}}
	case declarative.KindNameGrantList:
		var parsed declarative.GrantListDoc
		require.NoError(t, yaml.Unmarshal([]byte(content), &parsed))
		platform = map[string]any{"security": map[string]any{"grants": parsed.Grants}}
	case declarative.KindNameDomain:
		var parsed declarative.DomainDoc
		require.NoError(t, yaml.Unmarshal([]byte(content), &parsed))
		platform = map[string]any{"domains": map[string]any{parsed.Metadata.Name: parsed.Spec}}
	case declarative.KindNameTeam:
		var parsed declarative.TeamDoc
		require.NoError(t, yaml.Unmarshal([]byte(content), &parsed))
		platform = map[string]any{"teams": map[string]any{parsed.Metadata.Name: parsed.Spec}}
	case declarative.KindNameDataProduct:
		var parsed declarative.DataProductDoc
		require.NoError(t, yaml.Unmarshal([]byte(content), &parsed))
		platform = map[string]any{"data_products": map[string]any{parsed.Metadata.Name: parsed.Spec}}
	case declarative.KindNameModel:
		var parsed declarative.ModelDoc
		require.NoError(t, yaml.Unmarshal([]byte(content), &parsed))
		projectName := inferProjectNameFromPath(relPath, "analytics")
		platform = scaffoldExistingProjectPlatform(projectName)
		projects := platform["projects"].(map[string]any)
		project := projects[projectName].(map[string]any)
		project["models"] = map[string]any{parsed.Metadata.Name: parsed.Spec}
	case declarative.KindNameMacro:
		var parsed declarative.MacroDoc
		require.NoError(t, yaml.Unmarshal([]byte(content), &parsed))
		projectName := strings.TrimSpace(parsed.Spec.ProjectName)
		if projectName == "" {
			projectName = inferProjectNameFromPath(relPath, "analytics")
		}
		platform = scaffoldExistingProjectPlatform(projectName)
		projects := platform["projects"].(map[string]any)
		project := projects[projectName].(map[string]any)
		macroSpec := parsed.Spec
		macroSpec.ProjectName = projectName
		project["macros"] = map[string]any{parsed.Metadata.Name: macroSpec}
	case declarative.KindNameSemanticModel:
		var parsed declarative.SemanticModelDoc
		require.NoError(t, yaml.Unmarshal([]byte(content), &parsed))
		workspaceRef := strings.TrimSpace(parsed.Spec.WorkspaceRef)
		if workspaceRef == "" {
			workspaceRef = inferWorkspaceNameFromPath(relPath, "admin_user workspace")
		}
		platform = scaffoldWorkspacePlatform(workspaceRef)
		workspaces := platform["workspaces"].(map[string]any)
		workspace := workspaces[workspaceRef].(map[string]any)
		semanticSpec := parsed.Spec
		semanticSpec.WorkspaceRef = workspaceRef
		workspace["semantic_models"] = map[string]any{parsed.Metadata.Name: semanticSpec}
	case declarative.KindNameNotebook:
		var parsed declarative.NotebookDoc
		require.NoError(t, yaml.Unmarshal([]byte(content), &parsed))
		workspaceRef := strings.TrimSpace(parsed.Spec.WorkspaceRef)
		if workspaceRef == "" {
			workspaceRef = "admin_user workspace"
		}
		folderRef := strings.TrimSpace(parsed.Spec.FolderRef)
		if folderRef == "" {
			folderRef = workspaceRef + "/imported"
		}
		parts := strings.Split(folderRef, "/")
		folderName := parts[len(parts)-1]
		platform = scaffoldWorkspacePlatform(workspaceRef)
		workspaces := platform["workspaces"].(map[string]any)
		workspace := workspaces[workspaceRef].(map[string]any)
		folders := workspace["folders"].(map[string]any)
		folder := folders[folderName].(map[string]any)
		notebookSpec := parsed.Spec
		notebookSpec.WorkspaceRef = workspaceRef
		notebookSpec.FolderRef = folderRef
		folder["notebooks"] = map[string]any{parsed.Metadata.Name: notebookSpec}
		if parsed.Spec.Publish != nil && parsed.Spec.Publish.Model != nil && strings.TrimSpace(parsed.Spec.Publish.Model.Project) != "" {
			projectName := strings.TrimSpace(parsed.Spec.Publish.Model.Project)
			platform = mergeMaps(platform, scaffoldExistingProjectPlatform(projectName))
		}
	default:
		require.FailNowf(t, "unsupported legacy declarative fixture kind", "kind %q for %s is not supported by the CUE test writer", doc.Kind, relPath)
	}

	return encodePlatformFragment(t, platform)
}

func scaffoldWorkspaceShell(workspaceName string) map[string]any {
	owner := "admin"
	if workspaceName == "admin_user workspace" {
		owner = "admin_user"
	}
	return map[string]any{
		"workspaces": map[string]any{
			workspaceName: map[string]any{
				"kind":            "personal",
				"owner_principal": owner,
			},
		},
	}
}

func scaffoldWorkspacePlatform(workspaceName string) map[string]any {
	return mergeMaps(scaffoldWorkspaceShell(workspaceName), map[string]any{
		"workspaces": map[string]any{
			workspaceName: map[string]any{
				"folders": map[string]any{
					"imported": map[string]any{},
				},
			},
		},
	})
}

func scaffoldExistingProjectPlatform(projectName string) map[string]any {
	return mergeMaps(scaffoldWorkspaceShell("Shared Workspace"), map[string]any{
		"projects": map[string]any{
			projectName: map[string]any{
				"workspace_ref": "Shared Workspace",
				"kind":          "personal",
			},
		},
	})
}

func inferProjectNameFromPath(relPath, fallback string) string {
	parts := strings.Split(filepath.ToSlash(relPath), "/")
	for idx, part := range parts {
		if part == "models" && idx+1 < len(parts) {
			return parts[idx+1]
		}
	}
	return fallback
}

func inferWorkspaceNameFromPath(relPath, fallback string) string {
	parts := strings.Split(filepath.ToSlash(relPath), "/")
	for idx, part := range parts {
		if part == "workspaces" && idx+1 < len(parts) && strings.TrimSpace(parts[idx+1]) != "" {
			return parts[idx+1]
		}
	}
	return fallback
}

func mergeMaps(base, overlay map[string]any) map[string]any {
	out := map[string]any{}
	for key, value := range base {
		out[key] = value
	}
	for key, value := range overlay {
		if baseMap, ok := out[key].(map[string]any); ok {
			if overlayMap, ok := value.(map[string]any); ok {
				out[key] = mergeMaps(baseMap, overlayMap)
				continue
			}
		}
		out[key] = value
	}
	return out
}

func encodePlatformFragment(t *testing.T, platform map[string]any) string {
	t.Helper()
	payload, err := yaml.Marshal(map[string]any{"platform": platform})
	require.NoError(t, err)
	file, err := cueyaml.Extract("fragment.yaml", payload)
	require.NoError(t, err)
	formatted, err := cueformat.Node(file)
	require.NoError(t, err)
	return "package duckconfig\n\n" + string(formatted)
}

// ---------------------------------------------------------------------------
// makeStateClient creates an APIStateClient pointed at the test server.
// ---------------------------------------------------------------------------

func makeStateClient(t *testing.T, serverURL, apiKey string) *cli.APIStateClient {
	t.Helper()
	client := apiruntime.NewClient(serverURL, apiKey, "")
	return cli.NewAPIStateClient(client)
}

// ---------------------------------------------------------------------------
// writeSeedStateYAML writes YAML that matches the seeded RBAC data so the
// differ produces zero changes for the baseline resources.
// ---------------------------------------------------------------------------

// seedPrincipalsYAML declares the 4 principals that seedRBAC creates.
const seedPrincipalsYAML = `apiVersion: quackstack/v1
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
const seedGroupsYAML = `apiVersion: quackstack/v1
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

func assertNotebookDriftFields(t *testing.T, actions []declarative.Action, allowedFields ...string) {
	t.Helper()
	allowed := make(map[string]bool, len(allowedFields))
	for _, field := range allowedFields {
		allowed[field] = true
	}
	for _, action := range actions {
		for _, change := range action.Changes {
			assert.True(t, allowed[change.Field], "unexpected notebook drift field %q", change.Field)
		}
	}
}

func executeActions(t *testing.T, stateClient *cli.APIStateClient, actions []declarative.Action) {
	t.Helper()
	for _, action := range actions {
		require.NoError(t, stateClient.Execute(context.Background(), action), "execute %s %s", action.Operation, action.ResourceName)
	}
}

func createActionsWithDependencies(plan *declarative.Plan, primaryKinds ...declarative.ResourceKind) []declarative.Action {
	allowed := map[declarative.ResourceKind]bool{
		declarative.KindWorkspace:   true,
		declarative.KindFolder:      true,
		declarative.KindProject:     true,
		declarative.KindEnvironment: true,
	}
	for _, kind := range primaryKinds {
		allowed[kind] = true
	}

	var actions []declarative.Action
	for _, action := range plan.Actions {
		if action.Operation != declarative.OpCreate {
			continue
		}
		if allowed[action.ResourceKind] {
			actions = append(actions, action)
		}
	}
	return actions
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

		writeYAML(t, dir, "security/principals.yaml", `apiVersion: quackstack/v1
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

		writeYAML(t, dir, "security/principals.yaml", `apiVersion: quackstack/v1
kind: PrincipalList
principals:
  - name: user1
    type: user
`)
		writeYAML(t, dir, "security/groups.yaml", `apiVersion: quackstack/v1
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
		ensureCUEModule(t, dir)
		require.NoError(t, os.MkdirAll(filepath.Join(dir, "security"), 0o750))
		require.NoError(t, os.WriteFile(filepath.Join(dir, "security", "principals_a.cue"), []byte(`package duckconfig

platform: security: principals: duped: {
	type: "user"
	is_admin: false
}
`), 0o600))
		require.NoError(t, os.WriteFile(filepath.Join(dir, "security", "principals_b.cue"), []byte(`package duckconfig

platform: security: principals: duped: {
	type: "service_principal"
	is_admin: false
}
`), 0o600))

		_, err := declarative.LoadDirectory(dir)
		require.Error(t, err, "should fail on conflicting principal fragments")
		assert.Contains(t, err.Error(), "duped")
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
	writeYAML(t, dir, "security/groups.yaml", `apiVersion: quackstack/v1
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

func TestDeclarative_GroupDescriptionUpdateConverges(t *testing.T) {
	env := setupHTTPServer(t, httpTestOpts{WithComputeEndpoints: true})
	stateClient := makeStateClient(t, env.Server.URL, env.Keys.Admin)

	dir := t.TempDir()
	writeYAML(t, dir, "security/principals.yaml", seedPrincipalsYAML)
	writeYAML(t, dir, "security/groups.yaml", `apiVersion: quackstack/v1
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
	groupUpdates := actionsOfKindAndOp(plan, declarative.KindGroup, declarative.OpUpdate)
	require.Len(t, groupUpdates, 1)
	executeActions(t, stateClient, groupUpdates)

	actualAfterApply, err := stateClient.ReadState(context.Background())
	require.NoError(t, err)
	replan := declarative.Diff(desired, actualAfterApply)
	assert.Empty(t, actionsOfKindAndOp(replan, declarative.KindGroup, declarative.OpUpdate))
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
	writeYAML(t, dir, "security/groups.yaml", `apiVersion: quackstack/v1
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
	grantsYAML := "apiVersion: quackstack/v1\nkind: GrantList\ngrants:\n"
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
	writeYAML(t, dir, "models/analytics/stg_orders.yaml", `apiVersion: quackstack/v1
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
    incremental_strategy: delete_insert
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
	executeActions(t, stateClient, createActionsWithDependencies(plan, declarative.KindModel))

	actualAfterCreate, err := stateClient.ReadState(context.Background())
	require.NoError(t, err)
	replan := declarative.Diff(desired, actualAfterCreate)
	assert.Empty(t, actionsOfKindAndOp(replan, declarative.KindModel, declarative.OpCreate), "model should be idempotent after create")
	assert.Empty(t, actionsOfKindAndOp(replan, declarative.KindModel, declarative.OpUpdate), "model should be idempotent after create")

	writeYAML(t, dir, "models/analytics/stg_orders.yaml", `apiVersion: quackstack/v1
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

	require.NoError(t, os.Remove(filepath.Join(dir, "models", "analytics", "stg_orders.cue")))
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

func TestDeclarative_NotebookPublishRemovalConverges(t *testing.T) {
	env := setupHTTPServer(t, httpTestOpts{WithModels: true, WithComputeEndpoints: true})
	stateClient := makeStateClient(t, env.Server.URL, env.Keys.Admin)

	dir := t.TempDir()
	writeYAML(t, dir, "notebooks/revenue_review.yaml", `apiVersion: quackstack/v1
kind: Notebook
metadata:
  name: revenue_review
spec:
  owner: admin_user
  cells:
    - type: markdown
      name: intro
      content: |
        # Revenue review
    - type: sql
      name: published_output
      role: output
      content: |
        SELECT 1 AS revenue
  publish:
    model:
      project: analytics
      name: revenue_review_model
      materialization: VIEW
      output_cell: published_output
`)

	desired, err := declarative.LoadDirectory(dir)
	require.NoError(t, err)
	require.Empty(t, declarative.Validate(desired))

	actual, err := stateClient.ReadState(context.Background())
	require.NoError(t, err)

	plan := declarative.Diff(desired, actual)
	notebookCreates := actionsOfKindAndOp(plan, declarative.KindNotebook, declarative.OpCreate)
	require.Len(t, notebookCreates, 1)
	executeActions(t, stateClient, createActionsWithDependencies(plan, declarative.KindNotebook))

	actualAfterCreate, err := stateClient.ReadState(context.Background())
	require.NoError(t, err)
	replanCreate := declarative.Diff(desired, actualAfterCreate)
	assertNotebookDriftFields(t, actionsOfKindAndOp(replanCreate, declarative.KindNotebook, declarative.OpUpdate), "workspace_ref")

	writeYAML(t, dir, "notebooks/revenue_review.yaml", `apiVersion: quackstack/v1
kind: Notebook
metadata:
  name: revenue_review
spec:
  owner: admin_user
  cells:
    - type: markdown
      name: intro
      content: |
        # Revenue review
`)

	desiredWithoutPublish, err := declarative.LoadDirectory(dir)
	require.NoError(t, err)
	require.Empty(t, declarative.Validate(desiredWithoutPublish))

	planUpdate := declarative.Diff(desiredWithoutPublish, actualAfterCreate)
	notebookUpdates := actionsOfKindAndOp(planUpdate, declarative.KindNotebook, declarative.OpUpdate)
	require.Len(t, notebookUpdates, 1)
	executeActions(t, stateClient, notebookUpdates)

	actualAfterUpdate, err := stateClient.ReadState(context.Background())
	require.NoError(t, err)
	replanUpdate := declarative.Diff(desiredWithoutPublish, actualAfterUpdate)
	assertNotebookDriftFields(t, actionsOfKindAndOp(replanUpdate, declarative.KindNotebook, declarative.OpUpdate), "workspace_ref")
	assert.Empty(t, actionsOfKindAndOp(replanUpdate, declarative.KindModel, declarative.OpDelete))
	assert.Empty(t, actionsOfKindAndOp(replanUpdate, declarative.KindModel, declarative.OpUpdate))
}

func TestDeclarative_DataProductVersionRemovalConverges(t *testing.T) {
	env := setupHTTPServer(t, httpTestOpts{})
	stateClient := makeStateClient(t, env.Server.URL, env.Keys.Admin)

	dir := t.TempDir()
	writeYAML(t, dir, "domains/revenue.yaml", `apiVersion: quackstack/v1
kind: Domain
metadata:
  name: revenue
spec:
  description: Revenue domain
`)
	writeYAML(t, dir, "teams/analytics-engineering.yaml", `apiVersion: quackstack/v1
kind: Team
metadata:
  name: analytics-engineering
spec:
  domain_ref: revenue
  contact_channel: "#rev-data"
`)
	writeYAML(t, dir, "data-products/daily-orders.yaml", `apiVersion: quackstack/v1
kind: DataProduct
metadata:
  name: daily-orders
spec:
  name: Daily Orders
  domain_ref: revenue
  owner_team_ref: analytics-engineering
  steward_principal: admin_user
  contact_channel: "#rev-data"
  versions:
    - version: 1
      release_state: DRAFT
      compatibility_level: BACKWARD_COMPATIBLE
`)

	desired, err := declarative.LoadDirectory(dir)
	require.NoError(t, err)
	require.Empty(t, declarative.Validate(desired))

	actual, err := stateClient.ReadState(context.Background())
	require.NoError(t, err)

	plan := declarative.Diff(desired, actual)
	var createActions []declarative.Action
	for _, action := range plan.Actions {
		switch action.ResourceKind {
		case declarative.KindDomain, declarative.KindTeam, declarative.KindDataProduct:
			if action.Operation == declarative.OpCreate {
				createActions = append(createActions, action)
			}
		}
	}
	require.Len(t, createActions, 3)
	executeActions(t, stateClient, createActions)

	createVersionResp := doRequest(t, http.MethodPost, env.Server.URL+"/v1/data-products/daily-orders/versions", env.Keys.Admin, map[string]any{
		"compatibility_level": "BACKWARD_COMPATIBLE",
		"created_by":          "admin_user",
	})
	require.Equal(t, http.StatusCreated, createVersionResp.StatusCode)
	_ = createVersionResp.Body.Close()

	actualWithExtraVersion, err := stateClient.ReadState(context.Background())
	require.NoError(t, err)
	require.Len(t, actualWithExtraVersion.DataProducts, 1)
	require.Len(t, actualWithExtraVersion.DataProducts[0].Spec.Versions, 2)

	desiredForRemoval := *desired
	desiredForRemoval.DataProducts = append([]declarative.DataProductResource(nil), desired.DataProducts...)
	desiredForRemoval.DataProducts[0].Spec.Versions = nil
	desiredForRemoval.DataProducts[0].Spec.PublicationIntent = actualWithExtraVersion.DataProducts[0].Spec.PublicationIntent

	planUpdate := declarative.Diff(&desiredForRemoval, actualWithExtraVersion)
	productUpdates := actionsOfKindAndOp(planUpdate, declarative.KindDataProduct, declarative.OpUpdate)
	require.Len(t, productUpdates, 1)
	executeActions(t, stateClient, productUpdates)

	actualAfterApply, err := stateClient.ReadState(context.Background())
	require.NoError(t, err)
	require.Len(t, actualAfterApply.DataProducts, 1)
	assert.Empty(t, actualAfterApply.DataProducts[0].Spec.Versions)

	replan := declarative.Diff(&desiredForRemoval, actualAfterApply)
	assert.Empty(t, actionsOfKindAndOp(replan, declarative.KindDataProduct, declarative.OpUpdate))
	assert.Empty(t, actionsOfKindAndOp(replan, declarative.KindDataProduct, declarative.OpDelete))
	assert.Empty(t, replan.Errors)
}

func TestDeclarative_MacroLifecycle(t *testing.T) {
	env := setupHTTPServer(t, httpTestOpts{WithModels: true})
	stateClient := makeStateClient(t, env.Server.URL, env.Keys.Admin)

	dir := t.TempDir()
	writeYAML(t, dir, "macros/fmt_money.yaml", `apiVersion: quackstack/v1
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
	executeActions(t, stateClient, createActionsWithDependencies(plan, declarative.KindMacro))

	actualAfterCreate, err := stateClient.ReadState(context.Background())
	require.NoError(t, err)
	replan := declarative.Diff(desired, actualAfterCreate)
	assert.Empty(t, actionsOfKindAndOp(replan, declarative.KindMacro, declarative.OpCreate))
	assert.Empty(t, actionsOfKindAndOp(replan, declarative.KindMacro, declarative.OpUpdate))

	writeYAML(t, dir, "macros/fmt_money.yaml", `apiVersion: quackstack/v1
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

	require.NoError(t, os.Remove(filepath.Join(dir, "macros", "fmt_money.cue")))
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
	writeYAML(t, dir, "workspaces/admin_user workspace/semantic-models/customers.yaml", `apiVersion: quackstack/v1
kind: SemanticModel
metadata:
  name: customers
spec:
  base_relation_ref: analytics.dim_customers
`)
	writeYAML(t, dir, "workspaces/admin_user workspace/semantic-models/sales.yaml", `apiVersion: quackstack/v1
kind: SemanticModel
metadata:
  name: sales
spec:
  description: sales semantic model
  base_relation_ref: analytics.fct_sales
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
	executeActions(t, stateClient, createActionsWithDependencies(plan, declarative.KindSemanticModel))

	actualAfterCreate, err := stateClient.ReadState(context.Background())
	require.NoError(t, err)
	replan := declarative.Diff(desired, actualAfterCreate)
	assert.Empty(t, actionsOfKindAndOp(replan, declarative.KindSemanticModel, declarative.OpCreate))
	assert.Empty(t, actionsOfKindAndOp(replan, declarative.KindSemanticModel, declarative.OpUpdate))

	writeYAML(t, dir, "workspaces/admin_user workspace/semantic-models/sales.yaml", `apiVersion: quackstack/v1
kind: SemanticModel
metadata:
  name: sales
spec:
  description: sales semantic model updated
  base_relation_ref: analytics.fct_sales
  default_time_dimension: order_date
  metrics:
    - name: total_revenue
      metric_type: SUM
      expression_mode: SQL
      expression: SUM(amount_usd)
      certification_state: CERTIFIED
  relationships:
    - name: sales_to_customers
      to_model: admin_user workspace/customers
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

	require.NoError(t, os.Remove(filepath.Join(dir, "workspaces", "admin_user workspace", "semantic-models", "sales.cue")))
	require.NoError(t, os.Remove(filepath.Join(dir, "workspaces", "admin_user workspace", "semantic-models", "customers.cue")))

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
	writeYAML(t, dir, "workspaces/admin_user workspace/semantic-models/sales_runtime.yaml", `apiVersion: quackstack/v1
kind: SemanticModel
metadata:
  name: sales_runtime
spec:
  base_relation_ref: main.titanic
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

	actual, err := stateClient.ReadState(context.Background())
	require.NoError(t, err)

	plan := declarative.Diff(desired, actual)
	creates := actionsOfKindAndOp(plan, declarative.KindSemanticModel, declarative.OpCreate)
	require.Len(t, creates, 1)
	executeActions(t, stateClient, createActionsWithDependencies(plan, declarative.KindSemanticModel))

	workspaceID := lookupWorkspaceIDByName(t, env.MetaDB, "admin_user workspace")

	semanticModelsResp := doRequest(t, http.MethodGet, env.Server.URL+"/v1/workspaces/"+workspaceID+"/semantic-models", env.Keys.Admin, nil)
	if semanticModelsResp.StatusCode != http.StatusOK {
		require.Equal(t, http.StatusOK, semanticModelsResp.StatusCode, string(readBody(t, semanticModelsResp)))
	}
	var semanticModels struct {
		Data []struct {
			ID   string `json:"id"`
			Name string `json:"name"`
		} `json:"data"`
	}
	decodeJSON(t, semanticModelsResp, &semanticModels)
	require.NotEmpty(t, semanticModels.Data)
	semanticModelID := semanticModels.Data[0].ID

	explainResp := doRequest(t, http.MethodPost, env.Server.URL+"/v1/workspaces/"+workspaceID+"/semantic-models/"+semanticModelID+"/query-explanations", env.Keys.Admin, map[string]interface{}{
		"metrics": []string{"total_fare"},
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

	runResp := doRequest(t, http.MethodPost, env.Server.URL+"/v1/workspaces/"+workspaceID+"/semantic-models/"+semanticModelID+"/query-runs", env.Keys.Admin, map[string]interface{}{
		"metrics": []string{"total_fare"},
	})
	if runResp.StatusCode != http.StatusOK {
		require.Equal(t, http.StatusOK, runResp.StatusCode, string(readBody(t, runResp)))
	}

	var runBody struct {
		Result struct {
			Columns []struct {
				Name string `json:"name"`
			} `json:"columns"`
			RowCount int64                    `json:"row_count"`
			Rows     []map[string]interface{} `json:"rows"`
		} `json:"result"`
	}
	decodeJSON(t, runResp, &runBody)
	assert.EqualValues(t, 1, runBody.Result.RowCount)
	require.Len(t, runBody.Result.Columns, 1)
	assert.Equal(t, "total_fare", runBody.Result.Columns[0].Name)
	require.Len(t, runBody.Result.Rows, 1)
}
