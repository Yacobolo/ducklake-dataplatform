package model

import (
	"testing"

	"duck-demo/internal/domain"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSelectModels(t *testing.T) {
	models := []domain.Model{
		{ProjectName: "p", Name: "a", Tags: []string{"finance"}},
		{ProjectName: "p", Name: "b", DependsOn: []string{"p.a"}, Tags: []string{"finance", "staging"}},
		{ProjectName: "p", Name: "c", DependsOn: []string{"p.b"}},
		{ProjectName: "q", Name: "d", DependsOn: []string{"p.a"}, Tags: []string{"marketing"}},
	}

	tests := []struct {
		name     string
		selector string
		want     []string // qualified names
		wantErr  bool
	}{
		{name: "all (empty)", selector: "", want: []string{"p.a", "p.b", "p.c", "q.d"}},
		{name: "all (star)", selector: "*", want: []string{"p.a", "p.b", "p.c", "q.d"}},
		{name: "single model", selector: "p.a", want: []string{"p.a"}},
		{name: "single model unqualified", selector: "c", want: []string{"p.c"}},
		{name: "downstream", selector: "a+", want: []string{"p.a", "p.b", "p.c", "q.d"}},
		{name: "upstream", selector: "+c", want: []string{"p.a", "p.b", "p.c"}},
		{name: "both directions", selector: "+b+", want: []string{"p.a", "p.b", "p.c"}},
		{name: "tag", selector: "tag:finance", want: []string{"p.a", "p.b"}},
		{name: "project", selector: "project:q", want: []string{"q.d"}},
		{name: "not found", selector: "nonexistent", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := SelectModels(tt.selector, models)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			var names []string
			for _, m := range result {
				names = append(names, m.QualifiedName())
			}
			assert.Equal(t, tt.want, names)
		})
	}
}
