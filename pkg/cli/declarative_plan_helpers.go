package cli

import "duck-demo/internal/declarative"

func planExitCode(plan *declarative.Plan) int {
	if len(plan.Errors) > 0 {
		return 1
	}
	if len(plan.Actions) > 0 {
		return 2
	}
	return 0
}

func planErrorMessages(plan *declarative.Plan) []string {
	if len(plan.Errors) == 0 {
		return nil
	}

	msgs := make([]string, 0, len(plan.Errors))
	for _, planErr := range plan.Errors {
		msgs = append(msgs, planErr.ResourceKind.String()+" "+planErr.ResourceName+": "+planErr.Message)
	}
	return msgs
}
