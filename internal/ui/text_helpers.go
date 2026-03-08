package ui

import "strings"

func titleizeWords(value string) string {
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
