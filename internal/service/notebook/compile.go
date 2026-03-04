package notebook

import (
	"fmt"
	"sort"
	"strings"

	"duck-demo/internal/domain"
	"duck-demo/internal/duckdbsql"
)

// CompileNotebookCellSQL compiles notebook SQL for a target cell by resolving
// dependencies across named SQL cells, tree-shaking to reachable nodes only.
func CompileNotebookCellSQL(cells []domain.Cell, targetCellID string, requireOutput bool) (string, error) {
	byID := make(map[string]domain.Cell, len(cells))
	named := make(map[string]domain.Cell)
	for _, cell := range cells {
		byID[cell.ID] = cell
		if cell.CellType != domain.CellTypeSQL || cell.Disabled || cell.Name == nil || *cell.Name == "" {
			continue
		}
		if cell.Role == domain.CellRoleTest {
			continue
		}
		named[*cell.Name] = cell
	}

	target, ok := byID[targetCellID]
	if !ok {
		return "", domain.ErrNotFound("cell %s not found", targetCellID)
	}
	if target.CellType != domain.CellTypeSQL {
		return "", domain.ErrValidation("cell %s is not a SQL cell", targetCellID)
	}
	if target.Disabled {
		return "", domain.ErrValidation("cell %s is disabled", targetCellID)
	}
	if requireOutput {
		if target.Role != "" && target.Role != domain.CellRoleOutput {
			return "", domain.ErrValidation("cell %s is not an output cell", targetCellID)
		}
	}
	if isEmptyOrCommentOnlySQL(target.Content) {
		return "", domain.ErrValidation("selected cell has empty SQL")
	}

	depsByName := make(map[string][]string, len(named))
	for name, cell := range named {
		deps, err := collectNotebookDeps(cell.Content, name, named)
		if err != nil {
			return "", err
		}
		depsByName[name] = deps
	}

	roots, err := rootsForTarget(target, named)
	if err != nil {
		return "", err
	}

	visitState := map[string]int{}
	stack := []string{}
	ordered := []string{}

	var dfs func(string) error
	dfs = func(name string) error {
		switch visitState[name] {
		case 1:
			cycle := append(append([]string{}, stack...), name)
			return domain.ErrValidation("notebook cell dependency cycle detected: %s", strings.Join(cycle, " -> "))
		case 2:
			return nil
		}

		visitState[name] = 1
		stack = append(stack, name)
		for _, dep := range depsByName[name] {
			if err := dfs(dep); err != nil {
				return err
			}
		}
		stack = stack[:len(stack)-1]
		visitState[name] = 2
		ordered = append(ordered, name)
		return nil
	}

	sort.Strings(roots)
	for _, root := range roots {
		if err := dfs(root); err != nil {
			return "", err
		}
	}

	targetName := ""
	if target.Name != nil {
		targetName = *target.Name
	}
	ctes := make([]string, 0, len(ordered))
	for _, name := range ordered {
		if name == targetName {
			continue
		}
		cell := named[name]
		ctes = append(ctes, fmt.Sprintf("%s AS (\n%s\n)", quoteCTEName(name), cell.Content))
	}
	if len(ctes) == 0 {
		return target.Content, nil
	}

	return "WITH\n" + strings.Join(ctes, ",\n") + "\n" + target.Content, nil
}

func rootsForTarget(target domain.Cell, named map[string]domain.Cell) ([]string, error) {
	if target.Name != nil && *target.Name != "" {
		if _, ok := named[*target.Name]; ok {
			return []string{*target.Name}, nil
		}
	}
	return collectNotebookDeps(target.Content, "", named)
}

func collectNotebookDeps(sqlText, ownerName string, named map[string]domain.Cell) ([]string, error) {
	stmt, err := duckdbsql.Parse(sqlText)
	if err != nil {
		if ownerName == "" {
			return nil, fmt.Errorf("parse notebook SQL: %w", err)
		}
		return nil, fmt.Errorf("parse notebook SQL for cell %q: %w", ownerName, err)
	}

	refs := duckdbsql.CollectTableNames(stmt)
	depsSet := map[string]struct{}{}
	for _, ref := range refs {
		if strings.HasPrefix(ref, "__func__") {
			continue
		}
		parts := strings.Split(ref, ".")
		unqualified := parts[len(parts)-1]
		if _, ok := named[unqualified]; !ok {
			continue
		}
		if len(parts) > 1 {
			return nil, domain.ErrValidation("ambiguous reference %q: notebook cell references must be unqualified", ref)
		}
		if unqualified == ownerName {
			return nil, domain.ErrValidation("cell %q cannot reference itself", ownerName)
		}
		depsSet[unqualified] = struct{}{}
	}

	deps := make([]string, 0, len(depsSet))
	for dep := range depsSet {
		deps = append(deps, dep)
	}
	sort.Strings(deps)
	return deps, nil
}

func quoteCTEName(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}

func isEmptyOrCommentOnlySQL(sql string) bool {
	sanitized := strings.TrimSpace(sql)
	if sanitized == "" {
		return true
	}
	for {
		if strings.HasPrefix(sanitized, "--") {
			if idx := strings.IndexByte(sanitized, '\n'); idx >= 0 {
				sanitized = strings.TrimSpace(sanitized[idx+1:])
				continue
			}
			return true
		}
		if strings.HasPrefix(sanitized, "/*") {
			if idx := strings.Index(sanitized, "*/"); idx >= 0 {
				sanitized = strings.TrimSpace(sanitized[idx+2:])
				continue
			}
			return true
		}
		break
	}
	return strings.TrimSpace(sanitized) == ""
}
