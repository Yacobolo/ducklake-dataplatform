package catalogs

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCatalogColumnTypeInfo(t *testing.T) {
	t.Run("maps common sql families to stable icon groups", func(t *testing.T) {
		cases := []struct {
			name     string
			typeName string
			group    string
			icon     string
			tone     string
		}{
			{name: "timestamp", typeName: "TIMESTAMP", group: "temporal", icon: "clock-3", tone: "blue"},
			{name: "integer", typeName: "BIGINT", group: "integer", icon: "hash", tone: "gray"},
			{name: "float", typeName: "DOUBLE", group: "floating", icon: "binary", tone: "indigo"},
			{name: "decimal", typeName: "DECIMAL(12,2)", group: "decimal", icon: "circle-dollar-sign", tone: "orange"},
			{name: "boolean", typeName: "BOOLEAN", group: "boolean", icon: "toggle-left", tone: "green"},
			{name: "text", typeName: "VARCHAR", group: "text", icon: "file-text", tone: "teal"},
			{name: "nested", typeName: "STRUCT(id INTEGER)", group: "nested", icon: "braces", tone: "plum"},
			{name: "binary", typeName: "BLOB", group: "binary", icon: "binary", tone: "red"},
			{name: "fallback", typeName: "GEOGRAPHY", group: "other", icon: "database", tone: "gray"},
		}

		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				info := catalogColumnTypeInfo(tc.typeName)
				assert.Equal(t, tc.group, info.Group)
				assert.Equal(t, tc.icon, info.Icon)
				assert.Equal(t, tc.tone, info.Tone)
			})
		}
	})
}
