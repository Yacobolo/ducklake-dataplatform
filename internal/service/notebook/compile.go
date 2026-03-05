package notebook

import (
	"fmt"
	"sort"
	"strings"

	"duck-demo/internal/domain"
	"duck-demo/internal/duckdbsql"
)

// CompileContext controls template rendering while compiling notebook SQL.
type CompileContext struct {
	Vars          map[string]string
	Sources       map[string]string
	StrictSources bool
}

// CompileNotebookCellSQL compiles notebook SQL for a target cell by resolving
// dependencies across named SQL cells, tree-shaking to reachable nodes only.
func CompileNotebookCellSQL(cells []domain.Cell, targetCellID string, requireOutput bool) (string, error) {
	return CompileNotebookCellSQLWithContext(cells, targetCellID, requireOutput, CompileContext{})
}

// CompileNotebookCellSQLWithContext compiles notebook SQL with template context.
func CompileNotebookCellSQLWithContext(cells []domain.Cell, targetCellID string, requireOutput bool, ctx CompileContext) (string, error) {
	byID := make(map[string]domain.Cell, len(cells))
	named := make(map[string]domain.Cell)
	renderedByID := make(map[string]string, len(cells))
	explicitDepsByID := make(map[string][]string, len(cells))
	for _, cell := range cells {
		byID[cell.ID] = cell
		if cell.CellType != domain.CellTypeSQL || cell.Disabled || cell.Name == nil || *cell.Name == "" {
			// Still render raw SQL cells without names for target support.
			if cell.CellType == domain.CellTypeSQL && !cell.Disabled {
				rendered, explicitDeps, err := renderNotebookTemplate(cell.Content, "", namedCellNameSet(cells), ctx)
				if err != nil {
					return "", err
				}
				renderedByID[cell.ID] = rendered
				explicitDepsByID[cell.ID] = explicitDeps
			}
			continue
		}
		named[*cell.Name] = cell
	}
	namedSet := make(map[string]struct{}, len(named))
	for name := range named {
		namedSet[name] = struct{}{}
	}
	for _, cell := range cells {
		if cell.CellType != domain.CellTypeSQL || cell.Disabled {
			continue
		}
		owner := ""
		if cell.Name != nil {
			owner = *cell.Name
		}
		rendered, explicitDeps, err := renderNotebookTemplate(cell.Content, owner, namedSet, ctx)
		if err != nil {
			return "", err
		}
		renderedByID[cell.ID] = rendered
		explicitDepsByID[cell.ID] = explicitDeps
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
	targetSQL := renderedByID[target.ID]
	if targetSQL == "" {
		targetSQL = target.Content
	}
	if isEmptyOrCommentOnlySQL(targetSQL) {
		return "", domain.ErrValidation("selected cell has empty SQL")
	}

	depsByName := make(map[string][]string, len(named))
	for name, cell := range named {
		rendered := renderedByID[cell.ID]
		if rendered == "" {
			rendered = cell.Content
		}
		explicitDeps := explicitDepsByID[cell.ID]
		deps, err := collectNotebookDeps(rendered, name, named, explicitDeps)
		if err != nil {
			return "", err
		}
		depsByName[name] = deps
	}

	roots, err := rootsForTarget(target, targetSQL, explicitDepsByID[target.ID], named)
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
		cellSQL := renderedByID[cell.ID]
		if cellSQL == "" {
			cellSQL = cell.Content
		}
		ctes = append(ctes, fmt.Sprintf("%s AS (\n%s\n)", quoteCTEName(name), cellSQL))
	}
	if len(ctes) == 0 {
		return targetSQL, nil
	}

	return "WITH\n" + strings.Join(ctes, ",\n") + "\n" + targetSQL, nil
}

func rootsForTarget(target domain.Cell, targetSQL string, explicitDeps []string, named map[string]domain.Cell) ([]string, error) {
	if target.Name != nil && *target.Name != "" {
		if _, ok := named[*target.Name]; ok {
			return []string{*target.Name}, nil
		}
	}
	return collectNotebookDeps(targetSQL, "", named, explicitDeps)
}

func collectNotebookDeps(sqlText, ownerName string, named map[string]domain.Cell, explicitDeps []string) ([]string, error) {
	stmt, err := duckdbsql.Parse(sqlText)
	if err != nil {
		if ownerName == "" {
			return nil, fmt.Errorf("parse notebook SQL: %w", err)
		}
		return nil, fmt.Errorf("parse notebook SQL for cell %q: %w", ownerName, err)
	}

	refs := duckdbsql.CollectTableNames(stmt)
	depsSet := map[string]struct{}{}
	for _, dep := range explicitDeps {
		if dep == ownerName {
			return nil, domain.ErrValidation("cell %q cannot reference itself", ownerName)
		}
		depsSet[dep] = struct{}{}
	}
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

func namedCellNameSet(cells []domain.Cell) map[string]struct{} {
	out := make(map[string]struct{})
	for _, cell := range cells {
		if cell.CellType != domain.CellTypeSQL || cell.Disabled || cell.Name == nil || *cell.Name == "" {
			continue
		}
		out[*cell.Name] = struct{}{}
	}
	return out
}

func renderNotebookTemplate(sqlText, ownerName string, named map[string]struct{}, ctx CompileContext) (string, []string, error) {
	deps := make(map[string]struct{})
	evalExpr := func(expr string) (string, error) {
		expr = strings.TrimSpace(expr)
		if expr == "this" {
			if ownerName == "" {
				return "this", nil
			}
			return quoteCTEName(ownerName), nil
		}

		fnName, args, ok, err := parseFunctionCall(expr)
		if err != nil {
			return "", err
		}
		if !ok {
			return "", domain.ErrValidation("unsupported template expression %q", expr)
		}

		switch fnName {
		case "ref":
			if len(args) != 1 {
				return "", domain.ErrValidation("ref() expects exactly one string argument")
			}
			name, err := unquoteString(args[0])
			if err != nil {
				return "", err
			}
			if strings.Contains(name, ".") {
				parts := strings.Split(name, ".")
				name = parts[len(parts)-1]
			}
			if _, ok := named[name]; !ok {
				return "", domain.ErrValidation("unknown ref(%q)", name)
			}
			if name == ownerName {
				return "", domain.ErrValidation("cell %q cannot reference itself", ownerName)
			}
			deps[name] = struct{}{}
			return quoteCTEName(name), nil
		case "source":
			if len(args) != 2 {
				return "", domain.ErrValidation("source() expects exactly two string arguments")
			}
			sourceName, err := unquoteString(args[0])
			if err != nil {
				return "", err
			}
			tableName, err := unquoteString(args[1])
			if err != nil {
				return "", err
			}
			key := sourceName + "." + tableName
			if rel, ok := ctx.Sources[key]; ok {
				return rel, nil
			}
			if ctx.StrictSources || len(ctx.Sources) > 0 {
				return "", domain.ErrValidation("unknown source(%q,%q)", sourceName, tableName)
			}
			return quoteRelationParts(sourceName, tableName), nil
		case "var":
			if len(args) != 1 && len(args) != 2 {
				return "", domain.ErrValidation("var() expects one required argument and optional default")
			}
			name, err := unquoteString(args[0])
			if err != nil {
				return "", err
			}
			if v, ok := ctx.Vars[name]; ok {
				return v, nil
			}
			if len(args) == 2 {
				def, err := unquoteString(args[1])
				if err != nil {
					return "", err
				}
				return def, nil
			}
			return "", domain.ErrValidation("required var %q not provided", name)
		default:
			return "", domain.ErrValidation("unsupported template expression %q", expr)
		}
	}

	evalCondition := func(expr string) (bool, error) {
		expr = strings.TrimSpace(expr)
		switch expr {
		case "is_incremental()":
			return false, nil
		case "not is_incremental()":
			return true, nil
		case "true":
			return true, nil
		case "false":
			return false, nil
		default:
			return false, domain.ErrValidation("unsupported if condition %q", expr)
		}
	}

	out, err := renderJinjaSubset(sqlText, evalExpr, evalCondition)
	if err != nil {
		return "", nil, err
	}

	depsList := make([]string, 0, len(deps))
	for dep := range deps {
		depsList = append(depsList, dep)
	}
	sort.Strings(depsList)
	return out, depsList, nil
}

func quoteRelationParts(parts ...string) string {
	quoted := make([]string, 0, len(parts))
	for _, p := range parts {
		for _, part := range strings.Split(p, ".") {
			part = strings.TrimSpace(part)
			if part == "" {
				continue
			}
			quoted = append(quoted, quoteCTEName(part))
		}
	}
	return strings.Join(quoted, ".")
}

func renderJinjaSubset(input string, evalExpr func(string) (string, error), evalIf func(string) (bool, error)) (string, error) {
	type frame struct {
		cond     bool
		parentOn bool
		inElse   bool
	}

	var out strings.Builder
	frames := make([]frame, 0)
	active := true

	updateActive := func() {
		active = true
		for _, f := range frames {
			if !f.parentOn {
				active = false
				return
			}
			if !f.inElse && !f.cond {
				active = false
				return
			}
			if f.inElse && f.cond {
				active = false
				return
			}
		}
	}

	i := 0
	for i < len(input) {
		if strings.HasPrefix(input[i:], "{{") {
			end := strings.Index(input[i+2:], "}}")
			if end < 0 {
				return "", domain.ErrValidation("unterminated expression tag")
			}
			if active {
				repl, err := evalExpr(input[i+2 : i+2+end])
				if err != nil {
					return "", err
				}
				out.WriteString(repl)
			}
			i += end + 4
			continue
		}

		if strings.HasPrefix(input[i:], "{%") {
			end := strings.Index(input[i+2:], "%}")
			if end < 0 {
				return "", domain.ErrValidation("unterminated control tag")
			}
			directive := strings.TrimSpace(input[i+2 : i+2+end])

			switch {
			case strings.HasPrefix(directive, "if "):
				condExpr := strings.TrimSpace(strings.TrimPrefix(directive, "if "))
				parentActive := active
				cond := false
				if parentActive {
					v, err := evalIf(condExpr)
					if err != nil {
						return "", err
					}
					cond = v
				}
				frames = append(frames, frame{cond: cond, parentOn: parentActive})
				updateActive()
			case directive == "else":
				if len(frames) == 0 {
					return "", domain.ErrValidation("unexpected else without matching if")
				}
				if frames[len(frames)-1].inElse {
					return "", domain.ErrValidation("duplicate else in same if block")
				}
				frames[len(frames)-1].inElse = true
				updateActive()
			case directive == "endif":
				if len(frames) == 0 {
					return "", domain.ErrValidation("unexpected endif without matching if")
				}
				frames = frames[:len(frames)-1]
				updateActive()
			default:
				return "", domain.ErrValidation("unsupported control tag %q", directive)
			}

			i += end + 4
			continue
		}

		if active {
			out.WriteByte(input[i])
		}
		i++
	}

	if len(frames) > 0 {
		return "", domain.ErrValidation("unterminated if block")
	}

	return strings.TrimSpace(out.String()), nil
}

func parseFunctionCall(expr string) (string, []string, bool, error) {
	open := strings.IndexByte(expr, '(')
	closeIdx := strings.LastIndexByte(expr, ')')
	if open <= 0 || closeIdx < open {
		return "", nil, false, nil
	}
	name := strings.TrimSpace(expr[:open])
	if name == "" || strings.Contains(name, " ") {
		return "", nil, false, nil
	}
	argsStr := strings.TrimSpace(expr[open+1 : closeIdx])
	if strings.TrimSpace(expr[closeIdx+1:]) != "" {
		return "", nil, false, domain.ErrValidation("invalid expression %q", expr)
	}
	args, err := splitArgs(argsStr)
	if err != nil {
		return "", nil, false, err
	}
	return name, args, true, nil
}

func splitArgs(s string) ([]string, error) {
	if strings.TrimSpace(s) == "" {
		return nil, nil
	}

	var args []string
	start := 0
	inSingle := false
	inDouble := false
	depth := 0
	for i := 0; i < len(s); i++ {
		switch s[i] {
		case '\'':
			if !inDouble {
				if inSingle && i+1 < len(s) && s[i+1] == '\'' {
					i++
					continue
				}
				inSingle = !inSingle
			}
		case '"':
			if !inSingle {
				inDouble = !inDouble
			}
		case '(', '[', '{':
			if !inSingle && !inDouble {
				depth++
			}
		case ')', ']', '}':
			if !inSingle && !inDouble {
				if depth > 0 {
					depth--
				}
			}
		case ',':
			if !inSingle && !inDouble && depth == 0 {
				args = append(args, strings.TrimSpace(s[start:i]))
				start = i + 1
			}
		}
	}
	if inSingle || inDouble {
		return nil, domain.ErrValidation("unterminated string literal in function arguments")
	}
	args = append(args, strings.TrimSpace(s[start:]))
	for _, a := range args {
		if a == "" {
			return nil, domain.ErrValidation("empty function argument")
		}
	}
	return args, nil
}

func unquoteString(v string) (string, error) {
	v = strings.TrimSpace(v)
	if len(v) < 2 {
		return "", domain.ErrValidation("expected quoted string argument, got %q", v)
	}
	if (v[0] == '\'' && v[len(v)-1] == '\'') || (v[0] == '"' && v[len(v)-1] == '"') {
		u := v[1 : len(v)-1]
		if v[0] == '\'' {
			u = strings.ReplaceAll(u, "''", "'")
		}
		if v[0] == '"' {
			u = strings.ReplaceAll(u, `""`, `"`)
		}
		return u, nil
	}
	return "", domain.ErrValidation("expected quoted string argument, got %q", v)
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
