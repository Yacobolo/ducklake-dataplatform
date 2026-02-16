package model

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"duck-demo/internal/domain"
)

type compileContext struct {
	targetCatalog string
	targetSchema  string
	vars          map[string]string
	fullRefresh   bool
	projectName   string
	modelName     string
	materialize   string
	models        map[string]domain.Model
	byName        map[string][]domain.Model
}

type compileResult struct {
	sql          string
	dependsOn    []string
	varsUsed     []string
	macrosUsed   []string
	compiledHash string
}

func compileModelSQL(sqlText string, ctx compileContext) (*compileResult, error) {
	rendered, refs, sources, varsUsed, macrosUsed, err := renderTemplate(sqlText, ctx)
	if err != nil {
		return nil, err
	}

	depsSet := make(map[string]struct{})
	for _, ref := range refs {
		depsSet[ref] = struct{}{}
	}
	for _, src := range sources {
		depsSet["source:"+src] = struct{}{}
	}

	deps, err := ExtractDependencies(rendered, ctx.projectName, mapValues(ctx.models))
	if err != nil {
		return nil, fmt.Errorf("parse compiled SQL: %w", err)
	}
	for _, dep := range deps {
		depsSet[dep] = struct{}{}
	}

	allDeps := mapKeys(depsSet)
	sort.Strings(allDeps)
	sort.Strings(varsUsed)
	sort.Strings(macrosUsed)

	hash, err := computeCompiledHash(rendered, ctx)
	if err != nil {
		return nil, fmt.Errorf("compiled hash: %w", err)
	}
	return &compileResult{
		sql:          rendered,
		dependsOn:    allDeps,
		varsUsed:     dedupeSorted(varsUsed),
		macrosUsed:   dedupeSorted(macrosUsed),
		compiledHash: hash,
	}, nil
}

func computeCompiledHash(sqlText string, ctx compileContext) (string, error) {
	ctxDigest, err := compileContextDigest(ctx)
	if err != nil {
		return "", err
	}
	h := sha256.Sum256([]byte(sqlText + "\n" + ctxDigest))
	return "sha256:" + hex.EncodeToString(h[:]), nil
}

func renderTemplate(sqlText string, ctx compileContext) (string, []string, []string, []string, []string, error) {
	refsSet := make(map[string]struct{})
	sourcesSet := make(map[string]struct{})
	varsSet := make(map[string]struct{})
	macrosSet := make(map[string]struct{})

	evalExpr := func(expr string) (string, error) {
		expr = strings.TrimSpace(expr)
		if expr == "this" {
			return relationFQN(ctx.targetCatalog, ctx.targetSchema, ctx.modelName), nil
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
			qualified := qualifyRef(ctx.projectName, name)
			m, ok := ctx.models[qualified]
			if !ok {
				if strings.Contains(name, ".") {
					return "", domain.ErrValidation("unknown ref(%q)", name)
				}
				if candidates := ctx.byName[name]; len(candidates) == 1 {
					m = candidates[0]
					qualified = m.QualifiedName()
				} else {
					return "", domain.ErrValidation("unknown ref(%q)", name)
				}
			}
			refsSet[qualified] = struct{}{}
			return relationFQN(ctx.targetCatalog, ctx.targetSchema, m.Name), nil
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
			sourcesSet[key] = struct{}{}
			return renderRelationParts(sourceName, tableName), nil
		case "var":
			if len(args) != 1 && len(args) != 2 {
				return "", domain.ErrValidation("var() expects one required argument and optional default")
			}
			name, err := unquoteString(args[0])
			if err != nil {
				return "", err
			}
			varsSet[name] = struct{}{}
			if v, ok := ctx.vars[name]; ok {
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
		case "is_incremental":
			if len(args) != 0 {
				return "", domain.ErrValidation("is_incremental() does not accept arguments")
			}
			if ctx.materialize == domain.MaterializationIncremental && !ctx.fullRefresh {
				return "true", nil
			}
			return "false", nil
		default:
			macrosSet[fnName] = struct{}{}
			return expr, nil
		}
	}

	evalCondition := func(expr string) (bool, error) {
		expr = strings.TrimSpace(expr)
		switch expr {
		case "is_incremental()":
			return ctx.materialize == domain.MaterializationIncremental && !ctx.fullRefresh, nil
		case "not is_incremental()":
			return ctx.materialize != domain.MaterializationIncremental || ctx.fullRefresh, nil
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
		return "", nil, nil, nil, nil, err
	}

	return out, mapKeys(refsSet), mapKeys(sourcesSet), mapKeys(varsSet), mapKeys(macrosSet), nil
}

func renderJinjaSubset(input string, evalExpr func(string) (string, error), evalIf func(string) (bool, error)) (string, error) {
	type frame struct {
		cond      bool
		parentOn  bool
		inElse    bool
		directive string
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
				frames = append(frames, frame{cond: cond, parentOn: parentActive, directive: directive})
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
	for _, r := range name {
		if r == '.' || r == '_' || (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') {
			continue
		}
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
		case ',':
			if !inSingle && !inDouble {
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

func qualifyRef(projectName, refName string) string {
	if strings.Contains(refName, ".") {
		return refName
	}
	return projectName + "." + refName
}

func renderRelationParts(parts ...string) string {
	quoted := make([]string, 0, len(parts))
	for _, p := range parts {
		for _, part := range strings.Split(p, ".") {
			part = strings.TrimSpace(part)
			if part == "" {
				continue
			}
			quoted = append(quoted, quoteIdent(part))
		}
	}
	return strings.Join(quoted, ".")
}

func mapValues[V any](m map[string]V) []V {
	out := make([]V, 0, len(m))
	for _, v := range m {
		out = append(out, v)
	}
	return out
}

func mapKeys[T any](m map[string]T) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

func dedupeSorted(values []string) []string {
	if len(values) == 0 {
		return nil
	}
	clone := append([]string(nil), values...)
	sort.Strings(clone)
	out := make([]string, 0, len(clone))
	var prev string
	for i, v := range clone {
		if i == 0 || v != prev {
			out = append(out, v)
			prev = v
		}
	}
	return out
}

func compileContextDigest(ctx compileContext) (string, error) {
	p := struct {
		TargetCatalog string            `json:"target_catalog"`
		TargetSchema  string            `json:"target_schema"`
		ProjectName   string            `json:"project_name"`
		ModelName     string            `json:"model_name"`
		Materialize   string            `json:"materialization"`
		FullRefresh   bool              `json:"full_refresh"`
		Vars          map[string]string `json:"vars"`
	}{
		TargetCatalog: ctx.targetCatalog,
		TargetSchema:  ctx.targetSchema,
		ProjectName:   ctx.projectName,
		ModelName:     ctx.modelName,
		Materialize:   ctx.materialize,
		FullRefresh:   ctx.fullRefresh,
		Vars:          ctx.vars,
	}
	b, err := json.Marshal(p)
	if err != nil {
		return "", err
	}
	return string(b), nil
}
