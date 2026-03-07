package ui

import (
	"strings"

	. "maragu.dev/gomponents"
	. "maragu.dev/gomponents/html"
)

func banner(level, title, message string) Node {
	className := "Banner Banner-info"
	icon := "info"
	switch strings.ToLower(strings.TrimSpace(level)) {
	case "success":
		className = "Banner Banner-success"
		icon = "check-circle-2"
	case "attention", "warning":
		className = "Banner Banner-attention"
		icon = "triangle-alert"
	case "danger", "error":
		className = "Banner Banner-danger"
		icon = "circle-x"
	}

	return Div(
		Class(className),
		I(Class("nav-icon"), Attr("data-lucide", icon), Attr("aria-hidden", "true")),
		Div(Strong(Text(title)), P(Text(message))),
	)
}

func spinner() Node {
	return Span(Class("Spinner"), Attr("aria-hidden", "true"))
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
		Class("ProgressBar"),
		Attr("role", "progressbar"),
		Attr("aria-valuemin", "0"),
		Attr("aria-valuemax", intToString(max)),
		Attr("aria-valuenow", intToString(value)),
		Div(Class("ProgressBar-indicator"), Style("width: "+intToString((value*100)/max)+"%;")),
	)
}
