package cuesqlgen

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLoadCatalog_UsesMigrationsAsSchemaSource(t *testing.T) {
	catalog, err := LoadCatalog("../db/migrations")
	require.NoError(t, err)

	principals, err := catalog.MustTable("principals")
	require.NoError(t, err)
	assert.NotEmpty(t, principals.Columns)
	assert.Equal(t, "id", principals.Columns[0].Name)

	apiKeys, err := catalog.MustTable("api_keys")
	require.NoError(t, err)
	assert.True(t, tableHasColumn(apiKeys, "key_hash"))
	assert.True(t, tableHasColumn(apiKeys, "expires_at"))
}
