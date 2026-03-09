package ui

import (
	"strings"

	. "maragu.dev/gomponents"
	. "maragu.dev/gomponents/html"
)

func formField(cfg formFieldConfig) Node {
	inputType := strings.TrimSpace(cfg.Type)
	if inputType == "" {
		inputType = "text"
	}

	fieldClass := "FormControl"
	if cfg.Invalid {
		fieldClass += " is-invalid"
	}

	inputClass := "form-control"
	if cfg.Invalid {
		inputClass += " FormControl-input-invalid"
	}

	labelNodes := []Node{Text(cfg.Label)}
	if cfg.Required {
		labelNodes = append(labelNodes, Span(Class("FormControl-required"), Text("*")))
	}

	inputNodes := []Node{
		Type(inputType),
		Class(inputClass),
		Name(cfg.Name),
		Value(cfg.Value),
	}
	if strings.TrimSpace(cfg.Placeholder) != "" {
		inputNodes = append(inputNodes, Placeholder(cfg.Placeholder))
	}
	if cfg.Required {
		inputNodes = append(inputNodes, Required())
	}
	if cfg.Invalid {
		inputNodes = append(inputNodes, Attr("aria-invalid", "true"))
	}

	helpNode := Node(nil)
	if strings.TrimSpace(cfg.HelpText) != "" {
		helpNode = P(Class("FormControl-help"), Text(cfg.HelpText))
	}

	errorNode := Node(nil)
	if strings.TrimSpace(cfg.ErrorMessage) != "" {
		errorNode = P(Class("FormControl-error"), Text(cfg.ErrorMessage))
	}

	return Div(
		Class(fieldClass),
		Label(Group(labelNodes)),
		Input(inputNodes...),
		helpNode,
		errorNode,
	)
}

func checkboxOption(id, name, label string, checked bool) Node {
	checkedNode := Node(nil)
	if checked {
		checkedNode = Checked()
	}

	return Label(
		Class("SelectionControl"),
		Input(Type("checkbox"), ID(id), Name(name), Value(label), checkedNode),
		Span(Text(label)),
	)
}

func radioOption(id, name, label string, checked bool) Node {
	checkedNode := Node(nil)
	if checked {
		checkedNode = Checked()
	}

	return Label(
		Class("SelectionControl"),
		Input(Type("radio"), ID(id), Name(name), Value(label), checkedNode),
		Span(Text(label)),
	)
}

func toggleSwitch(id, name, label string, checked bool) Node {
	checkedNode := Node(nil)
	stateLabel := "Off"
	if checked {
		checkedNode = Checked()
		stateLabel = "On"
	}

	return Label(
		Class("ToggleSwitch"),
		Span(Class("ToggleSwitch-label"), Text(label)),
		Span(Class("ToggleSwitch-state"), Text(stateLabel)),
		Input(Type("checkbox"), ID(id), Name(name), checkedNode),
		Span(Class("ToggleSwitch-track"), Span(Class("ToggleSwitch-knob"))),
	)
}
