package app

import (
	"fmt"
	"path/filepath"
	"strings"

	"duck-demo/internal/config"
	"duck-demo/internal/service/orchestration"
)

func newOrchestrationIOManager(cfg *config.Config) (orchestration.IOManager, error) {
	managerType := strings.ToLower(strings.TrimSpace(cfg.OrchestrationIOManager))
	if managerType == "" {
		managerType = "memory"
	}

	switch managerType {
	case "memory", "inmemory":
		return orchestration.NewInMemoryIOManager(), nil
	case "filesystem", "fs":
		root := strings.TrimSpace(cfg.OrchestrationIOFSRoot)
		if root == "" {
			root = filepath.Join(filepath.Dir(cfg.MetaDBPath), "orchestration_io")
		}
		manager, err := orchestration.NewFilesystemIOManager(root)
		if err != nil {
			return nil, fmt.Errorf("create filesystem io manager: %w", err)
		}
		return manager, nil
	default:
		return nil, fmt.Errorf("unsupported ORCHESTRATION_IO_MANAGER %q", cfg.OrchestrationIOManager)
	}
}
