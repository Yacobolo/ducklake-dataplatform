package model

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDepToLineageSource(t *testing.T) {
	tests := []struct {
		name          string
		dep           string
		defaultSchema string
		wantSchema    string
		wantTable     string
	}{
		{name: "model dep in project format", dep: "analytics.stg_orders", defaultSchema: "mart", wantSchema: "mart", wantTable: "stg_orders"},
		{name: "source dep with schema table", dep: "source:raw.orders", defaultSchema: "mart", wantSchema: "raw", wantTable: "orders"},
		{name: "source dep with single part", dep: "source:orders", defaultSchema: "mart", wantSchema: "mart", wantTable: "orders"},
		{name: "single token dep", dep: "stg_orders", defaultSchema: "mart", wantSchema: "mart", wantTable: "stg_orders"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			schema, table := depToLineageSource(tt.dep, tt.defaultSchema)
			assert.Equal(t, tt.wantSchema, schema)
			assert.Equal(t, tt.wantTable, table)
		})
	}
}

func TestMakeLineageTableName(t *testing.T) {
	assert.Equal(t, "analytics.orders", makeLineageTableName("", "analytics", "orders"))
	assert.Equal(t, "memory.analytics.orders", makeLineageTableName("memory", "analytics", "orders"))
}
