//nolint:revive // orchestration components are exported for app wiring and tests.
package orchestration

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

var _ IOManager = (*FilesystemIOManager)(nil)

type FilesystemIOManager struct {
	root string
}

func NewFilesystemIOManager(root string) (*FilesystemIOManager, error) {
	trimmedRoot := strings.TrimSpace(root)
	if trimmedRoot == "" {
		return nil, fmt.Errorf("filesystem io manager root is required")
	}
	if err := os.MkdirAll(trimmedRoot, 0o750); err != nil {
		return nil, fmt.Errorf("create io manager root directory: %w", err)
	}
	return &FilesystemIOManager{root: trimmedRoot}, nil
}

func (m *FilesystemIOManager) LoadInput(_ context.Context, key string) (map[string]any, error) {
	path := m.pathForKey(key)
	// #nosec G304 -- path is derived from a SHA-256 digest under configured root.
	payload, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return map[string]any{}, nil
		}
		return nil, fmt.Errorf("read output for key %q: %w", key, err)
	}

	var out map[string]any
	if err := json.Unmarshal(payload, &out); err != nil {
		return nil, fmt.Errorf("decode output for key %q: %w", key, err)
	}
	if out == nil {
		return map[string]any{}, nil
	}
	return out, nil
}

func (m *FilesystemIOManager) StoreOutput(_ context.Context, key string, value map[string]any) error {
	payload, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("encode output for key %q: %w", key, err)
	}

	path := m.pathForKey(key)
	tmpPath := path + ".tmp"
	if err := os.WriteFile(tmpPath, payload, 0o600); err != nil {
		return fmt.Errorf("write output temp file for key %q: %w", key, err)
	}
	if err := os.Rename(tmpPath, path); err != nil {
		return fmt.Errorf("move output file into place for key %q: %w", key, err)
	}
	return nil
}

func (m *FilesystemIOManager) pathForKey(key string) string {
	digest := sha256.Sum256([]byte(key))
	filename := hex.EncodeToString(digest[:]) + ".json"
	return filepath.Join(m.root, filename)
}
