package cli

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRejectUnsupportedSystemSchemaQuery(t *testing.T) {
	t.Parallel()

	err := rejectUnsupportedSystemSchemaQuery(`select * from system.principals`)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "system.* queries")
}

func TestRejectUnsupportedSystemSchemaQuery_NonSystemSchemaAllowed(t *testing.T) {
	t.Parallel()

	err := rejectUnsupportedSystemSchemaQuery(`select * from main.users`)
	require.NoError(t, err)
}
