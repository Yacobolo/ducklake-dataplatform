package declarative

import "sort"

// Action represents a single planned change.
type Action struct {
	Operation    Operation
	ResourceKind ResourceKind
	ResourceName string // human-readable identifier e.g. "analyst1" or "main.analytics.orders"
	FilePath     string // source YAML file path (empty for deletes of server-only resources)
	Desired      any    // the spec from YAML (nil for Delete)
	Actual       any    // the current server state (nil for Create)
	Changes      []FieldDiff
}

// FieldDiff describes a single field change within an Update action.
type FieldDiff struct {
	Field    string `json:"field"`
	OldValue string `json:"old_value"`
	NewValue string `json:"new_value"`
}

// Plan is an ordered list of actions grouped by dependency layer.
type Plan struct {
	Actions []Action
	Errors  []PlanError // e.g. deletion-protected resources that are missing from YAML
}

// PlanError represents a non-actionable issue found during planning.
type PlanError struct {
	ResourceKind ResourceKind `json:"resource_type"`
	ResourceName string       `json:"resource_name"`
	Message      string       `json:"message"`
}

// Summary returns counts of creates, updates, deletes, and errors.
func (p *Plan) Summary() PlanSummary {
	var s PlanSummary
	for _, a := range p.Actions {
		switch a.Operation {
		case OpCreate:
			s.Creates++
		case OpUpdate:
			s.Updates++
		case OpDelete:
			s.Deletes++
		}
	}
	s.Errors = len(p.Errors)
	return s
}

// HasChanges returns true if the plan has any actions or errors.
func (p *Plan) HasChanges() bool {
	return len(p.Actions) > 0 || len(p.Errors) > 0
}

// PlanSummary holds counts of planned operations.
type PlanSummary struct {
	Creates int `json:"creates"`
	Updates int `json:"updates"`
	Deletes int `json:"deletes"`
	Errors  int `json:"errors"`
}

// SortActions sorts actions by dependency layer (creates ascending, deletes descending).
// Creates/updates are ordered layer 0->7; deletes are ordered layer 7->0.
// Deletes come AFTER all creates/updates.
// Within the same layer and operation, actions are sorted alphabetically by ResourceName.
func (p *Plan) SortActions() {
	sort.SliceStable(p.Actions, func(i, j int) bool {
		ai, aj := p.Actions[i], p.Actions[j]

		iIsDelete := ai.Operation == OpDelete
		jIsDelete := aj.Operation == OpDelete

		// Deletes come after creates/updates.
		if iIsDelete != jIsDelete {
			return !iIsDelete
		}

		li := ai.ResourceKind.Layer()
		lj := aj.ResourceKind.Layer()

		if iIsDelete {
			// Deletes: descending layer order (high layers first).
			if li != lj {
				return li > lj
			}
		} else {
			// Creates/updates: ascending layer order (low layers first).
			if li != lj {
				return li < lj
			}
		}

		// Within same layer and operation group, sort alphabetically.
		return ai.ResourceName < aj.ResourceName
	})
}
