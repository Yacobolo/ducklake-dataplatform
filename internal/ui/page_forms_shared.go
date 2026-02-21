package ui

import (
	"strings"

	"duck-demo/internal/domain"

	. "maragu.dev/gomponents"
	. "maragu.dev/gomponents/html"
)

func formPage(principal domain.ContextPrincipal, title, active, action string, csrfFieldProvider func() Node, fields ...Node) Node {
	nodes := []Node{csrfFieldProvider()}
	nodes = append(nodes, fields...)
	return appPage(
		title,
		active,
		principal,
		Div(
			Class(cardClass()),
			Form(
				Class("stack-form"),
				Method("post"),
				Action(action),
				Group(nodes),
				Div(Class("form-actions"), Button(Type("submit"), Class(primaryButtonClass()), Text("Save"))),
			),
		),
	)
}

func optionSelected(value, selected string) Node {
	if value == selected {
		return Option(Value(value), Selected(), Text(value))
	}
	return Option(Value(value), Text(value))
}

func optionalStringValue(v *string) string {
	if v == nil {
		return ""
	}
	return *v
}

func csvValues(values []string) string {
	if len(values) == 0 {
		return ""
	}
	return strings.Join(values, ", ")
}
