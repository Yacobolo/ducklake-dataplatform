package ui

import (
	"encoding/json"
	"io/fs"
	"path"
	"strings"
	"sync"

	"duck-demo/internal/ui/assets"
)

const defaultStylesheetPath = "/ui/static/css/app.css"

var (
	stylesheetPathOnce sync.Once
	stylesheetPath     = defaultStylesheetPath
)

func uiStylesheetHref() string {
	stylesheetPathOnce.Do(func() {
		manifestBytes, err := fs.ReadFile(assets.StaticFS(), "static/css/manifest.json")
		if err != nil {
			return
		}

		manifest := map[string]string{}
		if err := json.Unmarshal(manifestBytes, &manifest); err != nil {
			return
		}

		name := strings.TrimSpace(manifest["app.css"])
		if name == "" {
			return
		}

		if path.Base(name) != name || path.Ext(name) != ".css" {
			return
		}

		stylesheetPath = "/ui/static/css/" + name
	})

	return stylesheetPath
}
