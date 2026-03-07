package app

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"duck-demo/internal/config"
	"duck-demo/internal/service/orchestration"
)

func TestNewOrchestrationIOManager_DefaultMemory(t *testing.T) {
	mgr, err := newOrchestrationIOManager(&config.Config{})
	require.NoError(t, err)
	_, ok := mgr.(*orchestration.InMemoryIOManager)
	assert.True(t, ok)
}

func TestNewOrchestrationIOManager_Filesystem(t *testing.T) {
	mgr, err := newOrchestrationIOManager(&config.Config{
		OrchestrationIOManager: "filesystem",
		OrchestrationIOFSRoot:  t.TempDir(),
	})
	require.NoError(t, err)
	_, ok := mgr.(*orchestration.FilesystemIOManager)
	assert.True(t, ok)
}

func TestNewOrchestrationIOManager_UnknownType(t *testing.T) {
	mgr, err := newOrchestrationIOManager(&config.Config{OrchestrationIOManager: "bad"})
	assert.Nil(t, mgr)
	require.Error(t, err)
}
