package legacy

import (
	"fmt"
	"strings"

	"duck-demo/internal/ui/core"
)

func classNames(parts ...string) string {
	filtered := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		filtered = append(filtered, part)
	}
	return strings.Join(filtered, " ")
}

func titleizeWords(value string) string {
	return core.TitleizeWords(value)
}

func sqlCellString(value interface{}) string {
	if value == nil {
		return "NULL"
	}
	return fmt.Sprintf("%v", value)
}
