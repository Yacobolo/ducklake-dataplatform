package platform

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Yacobolo/quackstack/internal/config"
)

func TestNewProvider_DefaultsToManual(t *testing.T) {
	provider, err := NewProvider(&config.Config{PlatformProvider: "manual", ControlDBDriver: "sqlite"})
	require.NoError(t, err)
	assert.Equal(t, ProviderManual, provider.Name())
	assert.Equal(t, "sqlite", provider.ControlPlaneDatabase().Driver())
}

func TestNewProvider_SelectsAzure(t *testing.T) {
	provider, err := NewProvider(&config.Config{PlatformProvider: "azure", ControlDBDriver: "postgres"})
	require.NoError(t, err)
	assert.Equal(t, ProviderAzure, provider.Name())
	assert.Equal(t, "postgres", provider.ControlPlaneDatabase().Driver())
	assert.Equal(t, "aks-managed-compute", provider.ComputeLifecycle().Name())
}

func TestNewProvider_RejectsUnknownProvider(t *testing.T) {
	_, err := NewProvider(&config.Config{PlatformProvider: "unknown"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unsupported PLATFORM_PROVIDER")
}
