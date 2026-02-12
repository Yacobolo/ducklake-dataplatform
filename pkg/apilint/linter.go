// Package apilint provides an OpenAPI 3.x linter that enforces project
// conventions. It uses gopkg.in/yaml.v3 raw nodes to preserve line numbers.
package apilint

import (
	"fmt"
	"os"
	"regexp"
	"sort"
	"strings"

	"gopkg.in/yaml.v3"
)

// Severity levels for lint violations.
type Severity string

const (
	SeverityError   Severity = "error"
	SeverityWarning Severity = "warning"
	SeverityInfo    Severity = "info"
)

// Violation represents a single lint finding.
type Violation struct {
	File     string
	Line     int
	RuleID   string
	Severity Severity
	Message  string
}

// String formats a violation in golangci-lint style.
func (v Violation) String() string {
	return fmt.Sprintf("%s:%d: %s %s: %s", v.File, v.Line, v.RuleID, v.Severity, v.Message)
}

// Linter holds the parsed OpenAPI document and runs rules against it.
type Linter struct {
	file string
	root *yaml.Node
}

// New parses the given YAML file and returns a Linter.
func New(path string) (*Linter, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", path, err)
	}
	var doc yaml.Node
	if err := yaml.Unmarshal(data, &doc); err != nil {
		return nil, fmt.Errorf("parse %s: %w", path, err)
	}
	if doc.Kind != yaml.DocumentNode || len(doc.Content) == 0 {
		return nil, fmt.Errorf("%s: empty or invalid YAML document", path)
	}
	return &Linter{file: path, root: doc.Content[0]}, nil
}

// Run executes all lint rules and returns violations sorted by line number.
func (l *Linter) Run() []Violation {
	var vs []Violation
	vs = append(vs, l.checkOperationTags()...)
	vs = append(vs, l.checkOperationIDs()...)
	vs = append(vs, l.checkDeleteRequestBody()...)
	vs = append(vs, l.checkInlineResponseSchemas()...)
	vs = append(vs, l.checkPaginatedEndpointParams()...)
	vs = append(vs, l.checkCollectionMethodOrder()...)
	vs = append(vs, l.checkPathParamCamelCase()...)
	vs = append(vs, l.checkQueryParamSnakeCase()...)
	vs = append(vs, l.checkMissing401()...)
	vs = append(vs, l.checkPaginatedSchemaShape()...)
	vs = append(vs, l.checkRefsResolve()...)
	vs = append(vs, l.checkPostCreate201()...)

	sort.Slice(vs, func(i, j int) bool { return vs[i].Line < vs[j].Line })
	return vs
}

// HasErrors returns true if any violation has error severity.
func HasErrors(vs []Violation) bool {
	for _, v := range vs {
		if v.Severity == SeverityError {
			return true
		}
	}
	return false
}

// Filter returns violations at or above the given severity.
func Filter(vs []Violation, minSev Severity) []Violation {
	rank := map[Severity]int{SeverityInfo: 0, SeverityWarning: 1, SeverityError: 2}
	min := rank[minSev]
	var out []Violation
	for _, v := range vs {
		if rank[v.Severity] >= min {
			out = append(out, v)
		}
	}
	return out
}

// === YAML helpers ===

// mapGet looks up a key in a YAML mapping node and returns the value node.
func mapGet(m *yaml.Node, key string) *yaml.Node {
	if m.Kind != yaml.MappingNode {
		return nil
	}
	for i := 0; i < len(m.Content)-1; i += 2 {
		if m.Content[i].Value == key {
			return m.Content[i+1]
		}
	}
	return nil
}

// mapGetKey returns the key node for a given key name.
func mapGetKey(m *yaml.Node, key string) *yaml.Node {
	if m.Kind != yaml.MappingNode {
		return nil
	}
	for i := 0; i < len(m.Content)-1; i += 2 {
		if m.Content[i].Value == key {
			return m.Content[i]
		}
	}
	return nil
}

// mapKeys returns all key names from a mapping node.
func mapKeys(m *yaml.Node) []string {
	if m.Kind != yaml.MappingNode {
		return nil
	}
	var keys []string
	for i := 0; i < len(m.Content)-1; i += 2 {
		keys = append(keys, m.Content[i].Value)
	}
	return keys
}

// httpMethods is the set of HTTP methods in OpenAPI.
var httpMethods = map[string]bool{
	"get": true, "put": true, "post": true, "delete": true,
	"options": true, "head": true, "patch": true, "trace": true,
}

// forEachOperation calls fn for every (path, method, operationNode) in the spec.
func (l *Linter) forEachOperation(fn func(path, method string, op *yaml.Node)) {
	paths := mapGet(l.root, "paths")
	if paths == nil {
		return
	}
	for i := 0; i < len(paths.Content)-1; i += 2 {
		pathKey := paths.Content[i].Value
		pathItem := paths.Content[i+1]
		for j := 0; j < len(pathItem.Content)-1; j += 2 {
			method := pathItem.Content[j].Value
			if httpMethods[method] {
				fn(pathKey, method, pathItem.Content[j+1])
			}
		}
	}
}

// operationID extracts the operationId from an operation node.
func operationID(op *yaml.Node) string {
	n := mapGet(op, "operationId")
	if n != nil {
		return n.Value
	}
	return ""
}

// camelCaseRe matches lowerCamelCase identifiers.
var camelCaseRe = regexp.MustCompile(`^[a-z][a-zA-Z0-9]*$`)

// snakeCaseRe matches snake_case identifiers.
var snakeCaseRe = regexp.MustCompile(`^[a-z][a-z0-9]*(_[a-z0-9]+)*$`)

// hasGlobalSecurity checks if the spec has top-level security requirements.
func (l *Linter) hasGlobalSecurity() bool {
	sec := mapGet(l.root, "security")
	return sec != nil && len(sec.Content) > 0
}

// resolveRef checks if a $ref string resolves to a component.
func (l *Linter) resolveRef(ref string) bool {
	// Only handle local refs: #/components/schemas/Foo
	if !strings.HasPrefix(ref, "#/") {
		return true // external refs not checked
	}
	parts := strings.Split(strings.TrimPrefix(ref, "#/"), "/")
	node := l.root
	for _, p := range parts {
		node = mapGet(node, p)
		if node == nil {
			return false
		}
	}
	return true
}

func (l *Linter) violation(line int, ruleID string, sev Severity, msg string) Violation {
	return Violation{File: l.file, Line: line, RuleID: ruleID, Severity: sev, Message: msg}
}
