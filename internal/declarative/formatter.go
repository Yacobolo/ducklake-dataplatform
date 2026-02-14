package declarative

import (
	"encoding/json"
	"fmt"
	"io"
)

// ANSI color codes.
const (
	colorReset  = "\033[0m"
	colorGreen  = "\033[32m"
	colorYellow = "\033[33m"
	colorRed    = "\033[31m"
	colorCyan   = "\033[36m"
	colorDim    = "\033[2m"
)

// FormatText writes a human-readable plan to w.
// If noColor is true, ANSI codes are suppressed.
func FormatText(w io.Writer, plan *Plan, noColor bool) {
	c := func(code string) string {
		if noColor {
			return ""
		}
		return code
	}

	if !plan.HasChanges() {
		fmt.Fprintln(w, "No changes. Infrastructure is up-to-date.")
		return
	}

	// Group actions by file path for section headers.
	type group struct {
		path    string
		actions []Action
	}
	var groups []group
	seen := map[string]int{}
	for _, a := range plan.Actions {
		p := a.FilePath
		if idx, ok := seen[p]; ok {
			groups[idx].actions = append(groups[idx].actions, a)
		} else {
			seen[p] = len(groups)
			groups = append(groups, group{path: p, actions: []Action{a}})
		}
	}

	for _, g := range groups {
		if g.path != "" {
			fmt.Fprintf(w, "\n%s# %s%s\n", c(colorCyan), g.path, c(colorReset))
		} else {
			fmt.Fprintf(w, "\n%s# (server-only)%s\n", c(colorCyan), c(colorReset))
		}

		for _, a := range g.actions {
			switch a.Operation {
			case OpCreate:
				fmt.Fprintf(w, "  %s+%s %s %q will be created\n",
					c(colorGreen), c(colorReset), a.ResourceKind, a.ResourceName)
				formatDesired(w, a.Desired, c)

			case OpUpdate:
				fmt.Fprintf(w, "  %s~%s %s %q will be updated\n",
					c(colorYellow), c(colorReset), a.ResourceKind, a.ResourceName)
				for _, d := range a.Changes {
					fmt.Fprintf(w, "      %s: %q → %q\n", d.Field, d.OldValue, d.NewValue)
				}

			case OpDelete:
				fmt.Fprintf(w, "  %s-%s %s %q will be deleted\n",
					c(colorRed), c(colorReset), a.ResourceKind, a.ResourceName)
			}
		}
	}

	// Errors section.
	for _, e := range plan.Errors {
		fmt.Fprintf(w, "  %s✗%s %s %q: %s\n",
			c(colorRed), c(colorReset), e.ResourceKind, e.ResourceName, e.Message)
	}

	// Summary.
	s := plan.Summary()
	fmt.Fprintf(w, "\n%sPlan:%s %d to create, %d to update, %d to delete.",
		c(colorDim), c(colorReset), s.Creates, s.Updates, s.Deletes)
	if s.Errors > 0 {
		fmt.Fprintf(w, " %s%d error(s).%s", c(colorRed), s.Errors, c(colorReset))
	}
	fmt.Fprintln(w)
}

// formatDesired writes indented key-value pairs for a created resource's desired state.
func formatDesired(w io.Writer, desired any, c func(string) string) {
	if desired == nil {
		return
	}
	m, ok := desired.(map[string]any)
	if !ok {
		return
	}
	for k, v := range m {
		fmt.Fprintf(w, "      %s%s%s: %v\n", c(colorDim), k, c(colorReset), v)
	}
}

// FormatJSON writes the plan as JSON to w.
func FormatJSON(w io.Writer, plan *Plan) error {
	type jsonAction struct {
		Operation    string      `json:"operation"`
		ResourceType string      `json:"resource_type"`
		ResourceName string      `json:"resource_name"`
		Path         string      `json:"path,omitempty"`
		Changes      []FieldDiff `json:"changes,omitempty"`
	}
	type jsonPlan struct {
		Actions []jsonAction `json:"actions"`
		Errors  []PlanError  `json:"errors,omitempty"`
		Summary PlanSummary  `json:"summary"`
	}

	jp := jsonPlan{
		Actions: make([]jsonAction, 0, len(plan.Actions)),
		Summary: plan.Summary(),
	}
	if len(plan.Errors) > 0 {
		jp.Errors = plan.Errors
	}

	for _, a := range plan.Actions {
		ja := jsonAction{
			Operation:    a.Operation.String(),
			ResourceType: a.ResourceKind.String(),
			ResourceName: a.ResourceName,
			Path:         a.FilePath,
		}
		if len(a.Changes) > 0 {
			ja.Changes = a.Changes
		}
		jp.Actions = append(jp.Actions, ja)
	}

	data, err := json.MarshalIndent(jp, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal plan: %w", err)
	}
	_, err = w.Write(data)
	if err != nil {
		return fmt.Errorf("write plan: %w", err)
	}
	_, err = fmt.Fprintln(w)
	return err
}
