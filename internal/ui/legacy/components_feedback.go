package legacy

import (
	"strings"

	. "maragu.dev/gomponents"
	. "maragu.dev/gomponents/html"
)

func banner(level, title, message string) Node {
	className := "flex items-start gap-3 rounded-xl border border-[var(--borderColor-accent-muted)] bg-[var(--bgColor-accent-muted)] px-4 py-3 text-sm text-[var(--fgColor-default)]"
	icon := "info"
	switch strings.ToLower(strings.TrimSpace(level)) {
	case "success":
		className = "flex items-start gap-3 rounded-xl border border-[var(--borderColor-success-muted)] bg-[var(--bgColor-success-muted)] px-4 py-3 text-sm text-[var(--fgColor-default)]"
		icon = "check-circle-2"
	case "attention", "warning":
		className = "flex items-start gap-3 rounded-xl border border-[var(--borderColor-attention-muted)] bg-[var(--bgColor-attention-muted)] px-4 py-3 text-sm text-[var(--fgColor-default)]"
		icon = "triangle-alert"
	case "danger", "error":
		className = "flex items-start gap-3 rounded-xl border border-[var(--borderColor-danger-muted)] bg-[var(--bgColor-danger-muted)] px-4 py-3 text-sm text-[var(--fgColor-default)]"
		icon = "circle-x"
	}

	return Div(
		Class(className),
		I(Class(navIconClass("mt-0.5")), Attr("data-lucide", icon), Attr("aria-hidden", "true")),
		Div(Class("flex min-w-0 flex-col gap-1"), Strong(Class("font-semibold"), Text(title)), P(Class("m-0 text-sm text-[var(--fgColor-muted)]"), Text(message))),
	)
}

func spinner() Node {
	return Span(Class("inline-block h-4 w-4 animate-spin rounded-full border-2 border-[var(--borderColor-muted)] border-t-[var(--fgColor-accent)]"), Attr("aria-hidden", "true"))
}

func progressBar(value, max int) Node {
	if max <= 0 {
		max = 100
	}
	if value < 0 {
		value = 0
	}
	if value > max {
		value = max
	}

	return Div(
		Class("h-2 w-full overflow-hidden rounded-full bg-[var(--bgColor-muted)]"),
		Attr("role", "progressbar"),
		Attr("aria-valuemin", "0"),
		Attr("aria-valuemax", intToString(max)),
		Attr("aria-valuenow", intToString(value)),
		Div(Class("h-full rounded-full bg-[var(--bgColor-accent-emphasis)] transition-[width] duration-200 ease-out"), Style("width: "+intToString((value*100)/max)+"%;")),
	)
}
