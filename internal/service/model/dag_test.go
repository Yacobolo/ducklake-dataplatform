package model

import (
	"testing"

	"duck-demo/internal/domain"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestResolveDAG(t *testing.T) {
	tests := []struct {
		name      string
		models    []domain.Model
		wantTiers int
		wantErr   string
	}{
		{
			name:      "empty",
			models:    nil,
			wantTiers: 0,
		},
		{
			name: "single model no deps",
			models: []domain.Model{
				{ProjectName: "p", Name: "a"},
			},
			wantTiers: 1,
		},
		{
			name: "linear chain",
			models: []domain.Model{
				{ProjectName: "p", Name: "a"},
				{ProjectName: "p", Name: "b", DependsOn: []string{"p.a"}},
				{ProjectName: "p", Name: "c", DependsOn: []string{"p.b"}},
			},
			wantTiers: 3,
		},
		{
			name: "diamond",
			models: []domain.Model{
				{ProjectName: "p", Name: "a"},
				{ProjectName: "p", Name: "b", DependsOn: []string{"p.a"}},
				{ProjectName: "p", Name: "c", DependsOn: []string{"p.a"}},
				{ProjectName: "p", Name: "d", DependsOn: []string{"p.b", "p.c"}},
			},
			wantTiers: 3,
		},
		{
			name: "cross-project",
			models: []domain.Model{
				{ProjectName: "warehouse", Name: "raw"},
				{ProjectName: "sales", Name: "stg", DependsOn: []string{"warehouse.raw"}},
			},
			wantTiers: 2,
		},
		{
			name: "cycle",
			models: []domain.Model{
				{ProjectName: "p", Name: "a", DependsOn: []string{"p.b"}},
				{ProjectName: "p", Name: "b", DependsOn: []string{"p.a"}},
			},
			wantErr: "cycle detected",
		},
		{
			name: "self dependency",
			models: []domain.Model{
				{ProjectName: "p", Name: "a", DependsOn: []string{"p.a"}},
			},
			wantErr: "self dependency",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tiers, err := ResolveDAG(tt.models)
			if tt.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)
				return
			}
			require.NoError(t, err)
			assert.Len(t, tiers, tt.wantTiers)
		})
	}
}
