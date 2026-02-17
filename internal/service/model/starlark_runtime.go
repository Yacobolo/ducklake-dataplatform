package model

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"duck-demo/internal/domain"

	"go.starlark.net/starlark"
	"go.starlark.net/syntax"
)

const (
	defaultStarlarkMaxSteps = uint64(50_000)
	defaultStarlarkTimeout  = 2 * time.Second
	maxStarlarkOutputBytes  = 256 * 1024
	maxStarlarkModuleBytes  = 512 * 1024
	maxStarlarkArgsBytes    = 64 * 1024
)

type starlarkMacroRuntime struct {
	modules     map[string]starlark.StringDict
	maxSteps    uint64
	evalTimeout time.Duration
	maxArgBytes int
	maxModBytes int
}

func newStarlarkMacroRuntime(defs map[string]compileMacroDefinition) (*starlarkMacroRuntime, error) {
	byModule := map[string][]compileMacroDefinition{}
	for _, def := range defs {
		if !def.starlark {
			continue
		}
		module, _, ok := splitMacroQualifiedName(def.name)
		if !ok {
			return nil, domain.ErrValidation("invalid starlark macro name %q", def.name)
		}
		byModule[module] = append(byModule[module], def)
	}

	moduleSources := make(map[string]string, len(byModule))
	for module, moduleDefs := range byModule {
		src, err := renderModuleSource(moduleDefs)
		if err != nil {
			return nil, fmt.Errorf("render starlark module %q: %w", module, err)
		}
		moduleSources[module] = src
	}

	return newStarlarkMacroRuntimeFromModules(moduleSources)
}

func newStarlarkMacroRuntimeFromModules(moduleSources map[string]string) (*starlarkMacroRuntime, error) {
	runtime := &starlarkMacroRuntime{
		modules:     map[string]starlark.StringDict{},
		maxSteps:    defaultStarlarkMaxSteps,
		evalTimeout: defaultStarlarkTimeout,
		maxArgBytes: maxStarlarkArgsBytes,
		maxModBytes: maxStarlarkModuleBytes,
	}

	moduleNames := make([]string, 0, len(moduleSources))
	for module := range moduleSources {
		moduleNames = append(moduleNames, module)
	}
	sort.Strings(moduleNames)

	for _, module := range moduleNames {
		src := moduleSources[module]
		if len(src) > runtime.maxModBytes {
			return nil, domain.ErrValidation("starlark module %q exceeds %d bytes", module, runtime.maxModBytes)
		}
		thread := &starlark.Thread{Name: "compile-macro-module"}
		thread.SetMaxExecutionSteps(runtime.maxSteps)
		var globals starlark.StringDict
		if err := runStarlarkWithTimeout(thread, runtime.evalTimeout, func() error {
			loaded, err := starlark.ExecFileOptions(&syntax.FileOptions{}, thread, module+".star", src, nil)
			if err != nil {
				return err
			}
			globals = loaded
			return nil
		}); err != nil {
			return nil, fmt.Errorf("load starlark module %q: %w", module, err)
		}
		runtime.modules[module] = globals
	}

	return runtime, nil
}

func (r *starlarkMacroRuntime) EvalMacro(def compileMacroDefinition, rawArgs []string) (string, error) {
	if !def.starlark {
		return "", domain.ErrValidation("macro %q is not a starlark macro", def.name)
	}

	module, fnName, ok := splitMacroQualifiedName(def.name)
	if !ok {
		return "", domain.ErrValidation("invalid macro name %q", def.name)
	}

	globals, ok := r.modules[module]
	if !ok {
		return "", domain.ErrValidation("starlark module %q not loaded", module)
	}

	callable, ok := globals[fnName]
	if !ok {
		return "", domain.ErrValidation("macro function %q not found in module %q", fnName, module)
	}

	thread := &starlark.Thread{Name: "compile-macro-eval"}
	thread.SetMaxExecutionSteps(r.maxSteps)
	argBytes := 0
	for _, raw := range rawArgs {
		argBytes += len(raw)
		if argBytes > r.maxArgBytes {
			return "", domain.ErrValidation("macro %q arguments exceed %d bytes", def.name, r.maxArgBytes)
		}
	}

	args, kwargs, err := parseStarlarkCallArgs(thread, rawArgs)
	if err != nil {
		return "", err
	}

	var result starlark.Value
	if err := runStarlarkWithTimeout(thread, r.evalTimeout, func() error {
		callResult, err := starlark.Call(thread, callable, args, kwargs)
		if err != nil {
			return err
		}
		result = callResult
		return nil
	}); err != nil {
		return "", err
	}

	text, ok := starlark.AsString(result)
	if !ok {
		return "", domain.ErrValidation("macro %q must return string SQL", def.name)
	}
	if len(text) > maxStarlarkOutputBytes {
		return "", domain.ErrValidation("macro %q output exceeds %d bytes", def.name, maxStarlarkOutputBytes)
	}
	return text, nil
}

func renderModuleSource(defs []compileMacroDefinition) (string, error) {
	if len(defs) == 0 {
		return "", nil
	}

	sorted := append([]compileMacroDefinition(nil), defs...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].name < sorted[j].name })

	var b strings.Builder
	for i, def := range sorted {
		_, fnName, ok := splitMacroQualifiedName(def.name)
		if !ok {
			return "", domain.ErrValidation("invalid macro name %q", def.name)
		}
		if !isValidStarlarkIdent(fnName) {
			return "", domain.ErrValidation("invalid starlark function name %q", fnName)
		}
		for _, p := range def.parameters {
			if !isValidStarlarkIdent(p) {
				return "", domain.ErrValidation("invalid starlark parameter name %q for macro %q", p, def.name)
			}
		}

		b.WriteString("def ")
		b.WriteString(fnName)
		b.WriteByte('(')
		b.WriteString(strings.Join(def.parameters, ", "))
		b.WriteString("):\n")

		body := strings.TrimSpace(def.body)
		if body == "" {
			return "", domain.ErrValidation("starlark macro %q body cannot be empty", def.name)
		}

		lines := strings.Split(body, "\n")
		if len(lines) == 1 && !looksLikeStatement(lines[0]) {
			b.WriteString("    return ")
			b.WriteString(strings.TrimSpace(lines[0]))
			b.WriteByte('\n')
		} else {
			for _, line := range lines {
				trimmed := strings.TrimRight(line, " \t")
				if strings.TrimSpace(trimmed) == "" {
					b.WriteString("    \n")
					continue
				}
				b.WriteString("    ")
				b.WriteString(trimmed)
				b.WriteByte('\n')
			}
		}

		if i < len(sorted)-1 {
			b.WriteByte('\n')
		}
	}

	return b.String(), nil
}

func parseStarlarkCallArgs(thread *starlark.Thread, rawArgs []string) (starlark.Tuple, []starlark.Tuple, error) {
	args := make(starlark.Tuple, 0, len(rawArgs))
	kwargs := make([]starlark.Tuple, 0)

	for _, raw := range rawArgs {
		name, valueExpr, isKw := splitKeywordArg(raw)
		val, err := starlark.EvalOptions(&syntax.FileOptions{}, thread, "<macro-arg>", valueExpr, nil)
		if err != nil {
			return nil, nil, domain.ErrValidation("invalid macro argument %q: %v", raw, err)
		}

		if isKw {
			kwargs = append(kwargs, starlark.Tuple{starlark.String(name), val})
			continue
		}
		args = append(args, val)
	}

	return args, kwargs, nil
}

func splitKeywordArg(raw string) (name, value string, isKw bool) {
	raw = strings.TrimSpace(raw)
	idx := findTopLevelEquals(raw)
	if idx <= 0 {
		return "", raw, false
	}
	name = strings.TrimSpace(raw[:idx])
	value = strings.TrimSpace(raw[idx+1:])
	if !isValidStarlarkIdent(name) || value == "" {
		return "", raw, false
	}
	return name, value, true
}

func runStarlarkWithTimeout(thread *starlark.Thread, timeout time.Duration, fn func() error) error {
	if timeout <= 0 {
		return fn()
	}

	done := make(chan error, 1)
	go func() {
		done <- fn()
	}()

	timer := time.NewTimer(timeout)
	defer timer.Stop()

	select {
	case err := <-done:
		return err
	case <-timer.C:
		thread.Cancel("starlark execution timed out")
		err := <-done
		if err != nil {
			return domain.ErrValidation("starlark execution timed out after %s: %v", timeout, err)
		}
		return domain.ErrValidation("starlark execution timed out after %s", timeout)
	}
}

func findTopLevelEquals(s string) int {
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
			if !inSingle && !inDouble && depth > 0 {
				depth--
			}
		case '=':
			if !inSingle && !inDouble && depth == 0 {
				return i
			}
		}
	}
	return -1
}

func splitMacroQualifiedName(name string) (module string, fn string, ok bool) {
	idx := strings.LastIndex(name, ".")
	if idx <= 0 || idx == len(name)-1 {
		return "", "", false
	}
	return name[:idx], name[idx+1:], true
}

func isValidStarlarkIdent(name string) bool {
	if name == "" {
		return false
	}
	for i, r := range name {
		if i == 0 {
			if r != '_' && (r < 'a' || r > 'z') && (r < 'A' || r > 'Z') {
				return false
			}
			continue
		}
		if r != '_' && (r < 'a' || r > 'z') && (r < 'A' || r > 'Z') && (r < '0' || r > '9') {
			return false
		}
	}
	return true
}

func looksLikeStatement(line string) bool {
	trimmed := strings.TrimSpace(line)
	if trimmed == "" {
		return false
	}
	for _, prefix := range []string{"return ", "if ", "for ", "while ", "def ", "pass", "break", "continue", "load("} {
		if strings.HasPrefix(trimmed, prefix) {
			return true
		}
	}
	return false
}
