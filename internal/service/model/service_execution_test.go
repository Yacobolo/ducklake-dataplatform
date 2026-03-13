package model

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"duck-demo/internal/domain"
)

func TestSelectDefaultDevelopmentEnvironment_PrefersNamedDev(t *testing.T) {
	t.Parallel()

	environment, err := selectDefaultDevelopmentEnvironment([]domain.Environment{
		{Name: "alice", Kind: domain.EnvironmentKindDevelopment},
		{Name: "dev", Kind: domain.EnvironmentKindDevelopment},
	})
	require.NoError(t, err)
	require.NotNil(t, environment)
	assert.Equal(t, "dev", environment.Name)
}

func TestSelectDefaultDevelopmentEnvironment_RequiresDisambiguation(t *testing.T) {
	t.Parallel()

	environment, err := selectDefaultDevelopmentEnvironment([]domain.Environment{
		{Name: "alice", Kind: domain.EnvironmentKindDevelopment},
		{Name: "bob", Kind: domain.EnvironmentKindDevelopment},
	})
	require.Error(t, err)
	assert.Nil(t, environment)
	assert.Contains(t, err.Error(), "environment_name is required")
}

func TestSelectDefaultDevelopmentEnvironment_RequiresDevelopmentEnvironment(t *testing.T) {
	t.Parallel()

	environment, err := selectDefaultDevelopmentEnvironment([]domain.Environment{
		{Name: "staging", Kind: domain.EnvironmentKindStaging},
		{Name: "prod", Kind: domain.EnvironmentKindProduction},
	})
	require.Error(t, err)
	assert.Nil(t, environment)
	assert.Contains(t, err.Error(), "no development environment")
}
