package core

import (
	"strings"
	"time"
)

func TitleizeWords(value string) string {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return ""
	}

	parts := strings.Fields(strings.NewReplacer("_", " ", "-", " ", ".", " ").Replace(strings.ToLower(trimmed)))
	for i := range parts {
		runes := []rune(parts[i])
		if len(runes) == 0 {
			continue
		}
		runes[0] = []rune(strings.ToUpper(string(runes[0])))[0]
		parts[i] = string(runes)
	}
	return strings.Join(parts, " ")
}

func BoolLabel(value bool) string {
	if value {
		return "true"
	}
	return "false"
}

func FormatTimeUTC(ts time.Time) string {
	if ts.IsZero() {
		return "-"
	}
	return ts.UTC().Format("2006-01-02 15:04 UTC")
}
