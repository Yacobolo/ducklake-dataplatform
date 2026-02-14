package declarative

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFormatText_NoChanges(t *testing.T) {
	plan := &Plan{}
	var buf bytes.Buffer
	FormatText(&buf, plan, true)
	assert.Contains(t, buf.String(), "No changes")
}

func TestFormatText_WithActions(t *testing.T) {
	plan := &Plan{
		Actions: []Action{
			{Operation: OpCreate, ResourceKind: KindPrincipal, ResourceName: "user1", FilePath: "security/principals.yaml"},
			{Operation: OpDelete, ResourceKind: KindPrincipal, ResourceName: "user2", FilePath: "security/principals.yaml"},
		},
	}
	var buf bytes.Buffer
	FormatText(&buf, plan, true)
	output := buf.String()
	assert.Contains(t, output, "+")
	assert.Contains(t, output, "-")
	assert.Contains(t, output, "user1")
	assert.Contains(t, output, "user2")
	assert.Contains(t, output, "1 to create")
	assert.Contains(t, output, "1 to delete")
}

func TestFormatText_UpdateShowsFieldDiffs(t *testing.T) {
	plan := &Plan{
		Actions: []Action{
			{
				Operation:    OpUpdate,
				ResourceKind: KindPrincipal,
				ResourceName: "user1",
				Changes: []FieldDiff{
					{Field: "is_admin", OldValue: "false", NewValue: "true"},
				},
			},
		},
	}
	var buf bytes.Buffer
	FormatText(&buf, plan, true)
	output := buf.String()
	assert.Contains(t, output, "~")
	assert.Contains(t, output, "is_admin")
	assert.Contains(t, output, "0 to create")
	assert.Contains(t, output, "1 to update")
}

func TestFormatText_ErrorSection(t *testing.T) {
	plan := &Plan{
		Errors: []PlanError{
			{ResourceKind: KindCatalogRegistration, ResourceName: "protected", Message: "deletion_protection is enabled"},
		},
	}
	var buf bytes.Buffer
	FormatText(&buf, plan, true)
	output := buf.String()
	assert.Contains(t, output, "deletion_protection")
	assert.Contains(t, output, "1 error(s)")
}

func TestFormatJSON_ValidOutput(t *testing.T) {
	plan := &Plan{
		Actions: []Action{
			{Operation: OpCreate, ResourceKind: KindPrincipal, ResourceName: "user1"},
		},
	}
	var buf bytes.Buffer
	err := FormatJSON(&buf, plan)
	require.NoError(t, err)

	var result map[string]any
	err = json.Unmarshal(buf.Bytes(), &result)
	require.NoError(t, err)
	assert.Contains(t, result, "actions")
	assert.Contains(t, result, "summary")
}

func TestFormatJSON_EmptyPlan(t *testing.T) {
	plan := &Plan{}
	var buf bytes.Buffer
	err := FormatJSON(&buf, plan)
	require.NoError(t, err)

	var result map[string]any
	err = json.Unmarshal(buf.Bytes(), &result)
	require.NoError(t, err)

	actions, ok := result["actions"].([]any)
	require.True(t, ok)
	assert.Empty(t, actions)

	summary, ok := result["summary"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, 0, int(summary["creates"].(float64)))
	assert.Equal(t, 0, int(summary["deletes"].(float64)))
}

func TestFormatJSON_IncludesChanges(t *testing.T) {
	plan := &Plan{
		Actions: []Action{
			{
				Operation:    OpUpdate,
				ResourceKind: KindGroup,
				ResourceName: "g1",
				FilePath:     "security/groups.yaml",
				Changes: []FieldDiff{
					{Field: "description", OldValue: "old", NewValue: "new"},
				},
			},
		},
	}
	var buf bytes.Buffer
	err := FormatJSON(&buf, plan)
	require.NoError(t, err)

	var result map[string]any
	err = json.Unmarshal(buf.Bytes(), &result)
	require.NoError(t, err)

	actions := result["actions"].([]any)
	require.Len(t, actions, 1)
	action := actions[0].(map[string]any)
	assert.Equal(t, "update", action["operation"])
	assert.Equal(t, "group", action["resource_type"])
	assert.Equal(t, "g1", action["resource_name"])

	changes := action["changes"].([]any)
	require.Len(t, changes, 1)
	change := changes[0].(map[string]any)
	assert.Equal(t, "description", change["field"])
	assert.Equal(t, "old", change["old_value"])
	assert.Equal(t, "new", change["new_value"])
}

func TestFormatJSON_IncludesErrors(t *testing.T) {
	plan := &Plan{
		Errors: []PlanError{
			{ResourceKind: KindCatalogRegistration, ResourceName: "cat1", Message: "deletion_protection is enabled"},
		},
	}
	var buf bytes.Buffer
	err := FormatJSON(&buf, plan)
	require.NoError(t, err)

	var result map[string]any
	err = json.Unmarshal(buf.Bytes(), &result)
	require.NoError(t, err)

	errors, ok := result["errors"].([]any)
	require.True(t, ok)
	require.Len(t, errors, 1)

	planErr := errors[0].(map[string]any)
	assert.Equal(t, "cat1", planErr["resource_name"])
	assert.Contains(t, planErr["message"], "deletion_protection")
}
