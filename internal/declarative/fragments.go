package declarative

import (
	"fmt"
	"os"
	"path/filepath"
)

// EnsureModuleRoot ensures a declarative config directory contains a CUE module.
func EnsureModuleRoot(dir string) error {
	moduleDir := filepath.Join(dir, "cue.mod")
	if err := os.MkdirAll(moduleDir, 0o750); err != nil {
		return fmt.Errorf("create module dir %s: %w", moduleDir, err)
	}

	moduleFile := filepath.Join(moduleDir, "module.cue")
	if _, err := os.Stat(moduleFile); err == nil {
		return nil
	} else if !os.IsNotExist(err) {
		return fmt.Errorf("stat module file %s: %w", moduleFile, err)
	}

	if err := os.WriteFile(moduleFile, []byte(cueModuleFile), 0o600); err != nil {
		return fmt.Errorf("write module file %s: %w", moduleFile, err)
	}
	return nil
}

// WriteFragmentFile writes a single declarative fragment CUE file.
func WriteFragmentFile(path string, fragment any) error {
	return writeCueFile(path, fragment)
}
