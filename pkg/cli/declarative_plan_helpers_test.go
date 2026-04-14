package cli

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/Yacobolo/quackstack/internal/declarative"
)

func TestPlanExitCode(t *testing.T) {
	t.Parallel()

	assert.Equal(t, 0, planExitCode(&declarative.Plan{}))
	assert.Equal(t, 2, planExitCode(&declarative.Plan{
		Actions: []declarative.Action{{Operation: declarative.OpCreate}},
	}))
	assert.Equal(t, 1, planExitCode(&declarative.Plan{
		Errors: []declarative.PlanError{{ResourceName: "demo", Message: "blocked"}},
	}))
}

func TestPlanErrorMessages(t *testing.T) {
	t.Parallel()

	msgs := planErrorMessages(&declarative.Plan{
		Errors: []declarative.PlanError{{
			ResourceKind: declarative.KindTable,
			ResourceName: "main.analytics.orders",
			Message:      "cannot delete table: deletion_protection is enabled",
		}},
	})

	assert.Equal(t, []string{
		"table main.analytics.orders: cannot delete table: deletion_protection is enabled",
	}, msgs)
}
