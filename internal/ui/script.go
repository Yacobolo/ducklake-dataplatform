package ui

import (
	"encoding/json"
	"io/fs"
	"path"
	"strings"
	"sync"

	"duck-demo/internal/ui/assets"
)

const defaultScriptPrefix = "/ui/static/js/"

var (
	scriptManifestOnce sync.Once
	scriptManifest     map[string]string
)

func uiScriptHref(name string) string {
	name = strings.TrimSpace(name)
	if name == "" || path.Base(name) != name || path.Ext(name) != ".js" {
		return defaultScriptPrefix + "notebook.js"
	}

	scriptManifestOnce.Do(func() {
		scriptManifest = map[string]string{}
		manifestBytes, err := fs.ReadFile(assets.StaticFS(), "static/js/manifest.json")
		if err != nil {
			return
		}

		if err := json.Unmarshal(manifestBytes, &scriptManifest); err != nil {
			scriptManifest = map[string]string{}
		}
	})

	target := name
	if hashed := strings.TrimSpace(scriptManifest[name]); hashed != "" && path.Base(hashed) == hashed && path.Ext(hashed) == ".js" {
		target = hashed
	}

	return defaultScriptPrefix + target
}
