package ui

import (
	"strings"

	. "maragu.dev/gomponents"
	. "maragu.dev/gomponents/html"
)

func avatar(cfg avatarConfig) Node {
	initials := avatarInitials(cfg.Label)
	size := strings.TrimSpace(cfg.Size)
	if size == "" {
		size = "medium"
	}

	tone := strings.TrimSpace(cfg.Tone)
	if tone == "" {
		tone = "neutral"
	}

	return Span(
		Class("Avatar Avatar-"+size+" Avatar-"+tone),
		Attr("aria-label", cfg.Label),
		Text(initials),
	)
}

func avatarInitials(label string) string {
	parts := strings.Fields(strings.TrimSpace(label))
	if len(parts) == 0 {
		return "?"
	}
	if len(parts) == 1 {
		r := []rune(parts[0])
		if len(r) == 0 {
			return "?"
		}
		return strings.ToUpper(string(r[0]))
	}
	r1 := []rune(parts[0])
	r2 := []rune(parts[1])
	if len(r1) == 0 || len(r2) == 0 {
		return "?"
	}
	return strings.ToUpper(string(r1[0]) + string(r2[0]))
}
