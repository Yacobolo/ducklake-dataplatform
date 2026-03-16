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
		Class(classNames("inline-flex items-center justify-center rounded-full font-semibold uppercase tracking-wide", avatarSizeClass(size), avatarToneClass(tone))),
		Attr("aria-label", cfg.Label),
		Text(initials),
	)
}

func avatarSizeClass(size string) string {
	switch size {
	case "small":
		return "h-8 w-8 text-xs"
	case "large":
		return "h-12 w-12 text-base"
	default:
		return "h-10 w-10 text-sm"
	}
}

func avatarToneClass(tone string) string {
	switch tone {
	case "accent":
		return "bg-[var(--bgColor-accent-muted)] text-[var(--fgColor-accent)]"
	case "success":
		return "bg-[var(--bgColor-success-muted)] text-[var(--fgColor-success)]"
	case "danger":
		return "bg-[var(--bgColor-danger-muted)] text-[var(--fgColor-danger)]"
	default:
		return "bg-[var(--bgColor-neutral-muted)] text-[var(--fgColor-default)]"
	}
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
