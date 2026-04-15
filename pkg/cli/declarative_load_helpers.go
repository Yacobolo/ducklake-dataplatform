package cli

import (
	"fmt"
	"strings"

	"github.com/spf13/cobra"

	"github.com/Yacobolo/quackstack/internal/declarative"
)

type declarativeLoadFlags struct {
	target string
	vars   []string
}

func addDeclarativeLoadFlags(cmd *cobra.Command, flags *declarativeLoadFlags) {
	cmd.Flags().StringVar(&flags.target, "target", "", "Named target environment to resolve (for example: dev or workspace/project/dev)")
	cmd.Flags().StringArrayVar(&flags.vars, "var", nil, "Variable override in key=value form (repeatable)")
}

func (f declarativeLoadFlags) loadOptions(allowUnknownFields bool) (declarative.LoadOptions, error) {
	parsedVars, err := parseDeclarativeVars(f.vars)
	if err != nil {
		return declarative.LoadOptions{}, err
	}
	return declarative.LoadOptions{
		AllowUnknownFields: allowUnknownFields,
		Target:             f.target,
		Vars:               parsedVars,
	}, nil
}

func parseDeclarativeVars(items []string) (map[string]string, error) {
	if len(items) == 0 {
		return nil, nil
	}
	vars := make(map[string]string, len(items))
	for _, item := range items {
		parts := strings.SplitN(item, "=", 2)
		if len(parts) != 2 || strings.TrimSpace(parts[0]) == "" {
			return nil, fmt.Errorf("invalid --var %q: expected key=value", item)
		}
		vars[strings.TrimSpace(parts[0])] = parts[1]
	}
	return vars, nil
}
