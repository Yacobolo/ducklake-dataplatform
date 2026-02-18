package declarative

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDeclarativeSchemaDocumentTypes_UniqueEntries(t *testing.T) {
	types := SchemaDocumentTypes()
	require.NotEmpty(t, types)

	seenKinds := make(map[string]bool, len(types))
	seenFiles := make(map[string]bool, len(types))

	for _, entry := range types {
		require.NotEmpty(t, entry.Kind)
		require.NotEmpty(t, entry.FileName)
		require.NotNil(t, entry.Type)

		assert.False(t, seenKinds[entry.Kind], "duplicate schema kind %q", entry.Kind)
		assert.False(t, seenFiles[entry.FileName], "duplicate schema filename %q", entry.FileName)

		seenKinds[entry.Kind] = true
		seenFiles[entry.FileName] = true
	}

	assert.True(t, seenKinds[KindNameModel])
	assert.True(t, seenKinds[KindNameMacro])
}
