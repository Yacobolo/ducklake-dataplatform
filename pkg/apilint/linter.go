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

// Severity constants.
const (
	SeverityError   Severity = "error"
	SeverityWarning Severity = "warning"
	SeverityInfo    Severity = "info"
)

// sevRank maps severity to a numeric rank for comparison.
var sevRank = map[Severity]int{SeverityInfo: 0, SeverityWarning: 1, SeverityError: 2}

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

// === Rule interface & registry ===

// Rule is the interface that every lint rule must implement.
type Rule interface {
	ID() string
	Description() string
	DefaultSeverity() Severity
	Check(ctx *LintContext) []Violation
}

// registry holds all registered rules in registration order.
var registry []Rule

// Register adds a rule to the global registry. Called from init() in rules.go.
func Register(r Rule) { registry = append(registry, r) }

// RegisteredRules returns a copy of the registry for introspection (e.g. --list-rules).
func RegisteredRules() []Rule {
	out := make([]Rule, len(registry))
	copy(out, registry)
	return out
}

// === LintContext ===

// LintContext provides read access to the parsed spec and helper methods.
// It is passed to each Rule.Check call.
type LintContext struct {
	File string
	Root *yaml.Node
}

// MapGet looks up a key in a YAML mapping node and returns the value node.
func (ctx *LintContext) MapGet(m *yaml.Node, key string) *yaml.Node { return mapGet(m, key) }

// MapGetKey returns the key node for a given key name.
func (ctx *LintContext) MapGetKey(m *yaml.Node, key string) *yaml.Node { return mapGetKey(m, key) }

// MapKeys returns all key names from a mapping node.
func (ctx *LintContext) MapKeys(m *yaml.Node) []string { return mapKeys(m) }

// OperationID extracts the operationId from an operation node.
func (ctx *LintContext) OperationID(op *yaml.Node) string { return operationID(op) }

// HasGlobalSecurity checks if the spec has top-level security requirements.
func (ctx *LintContext) HasGlobalSecurity() bool {
	sec := mapGet(ctx.Root, "security")
	return sec != nil && len(sec.Content) > 0
}

// ResolveRef checks if a $ref string resolves to a component.
func (ctx *LintContext) ResolveRef(ref string) bool {
	if !strings.HasPrefix(ref, "#/") {
		return true // external refs not checked
	}
	parts := strings.Split(strings.TrimPrefix(ref, "#/"), "/")
	node := ctx.Root
	for _, p := range parts {
		node = mapGet(node, p)
		if node == nil {
			return false
		}
	}
	return true
}

// ForEachOperation calls fn for every (path, method, operationNode) in the spec.
func (ctx *LintContext) ForEachOperation(fn func(path, method string, op *yaml.Node)) {
	paths := mapGet(ctx.Root, "paths")
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

// Violation creates a Violation with the context's file path.
func (ctx *LintContext) Violation(line int, ruleID string, sev Severity, msg string) Violation {
	return Violation{File: ctx.File, Line: line, RuleID: ruleID, Severity: sev, Message: msg}
}

// === Linter ===

// Linter holds the parsed OpenAPI document and runs rules against it.
type Linter struct {
	file string
	root *yaml.Node
}

// New parses the given YAML file and returns a Linter.
func New(path string) (*Linter, error) {
	data, err := os.ReadFile(path) //nolint:gosec // path is provided by the caller
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

// Run executes all lint rules with default severity and returns violations sorted by line number.
func (l *Linter) Run() []Violation {
	return l.RunWithConfig(nil)
}

// RunWithConfig executes all lint rules using the given configuration (may be nil for defaults).
// Rules with severity overridden to "off" are skipped. Inline suppression comments are honoured.
func (l *Linter) RunWithConfig(cfg *Config) []Violation {
	ctx := &LintContext{File: l.file, Root: l.root}
	var vs []Violation
	for _, rule := range registry {
		sev := effectiveSeverity(cfg, rule)
		if sev == "" { // "off"
			continue
		}
		for _, v := range rule.Check(ctx) {
			v.Severity = sev // apply configured override
			if !isSuppressed(l.root, v.Line, rule.ID()) {
				vs = append(vs, v)
			}
		}
	}
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
	minRank := sevRank[minSev]
	var out []Violation
	for _, v := range vs {
		if sevRank[v.Severity] >= minRank {
			out = append(out, v)
		}
	}
	return out
}

// === Inline suppression ===

// suppressRe matches YAML comments like "apilint:ignore OAL001 OAL003".
var suppressRe = regexp.MustCompile(`apilint:ignore\s+(OAL\d+(?:\s+OAL\d+)*)`)

// isSuppressed returns true if the given rule is suppressed at the given line
// via a YAML comment containing "apilint:ignore OALXXX".
// It checks the node at the violation line, the node one line above (for parent
// key comments), and walks up through parent nodes that contain the violation line.
func isSuppressed(root *yaml.Node, line int, ruleID string) bool {
	// Check the exact line.
	if node := findNodeAtLine(root, line); node != nil {
		if commentSuppresses(node.LineComment, ruleID) ||
			commentSuppresses(node.HeadComment, ruleID) {
			return true
		}
	}
	// Check the line above (parent key nodes often have the comment).
	if node := findNodeAtLine(root, line-1); node != nil {
		if commentSuppresses(node.LineComment, ruleID) ||
			commentSuppresses(node.HeadComment, ruleID) {
			return true
		}
	}
	// Walk up the tree: any ancestor mapping key that contains this line range.
	return ancestorSuppresses(root, line, ruleID)
}

// ancestorSuppresses checks if any ancestor node has a suppression comment for the given rule.
func ancestorSuppresses(n *yaml.Node, line int, ruleID string) bool {
	if n == nil {
		return false
	}
	if n.Kind == yaml.MappingNode {
		for i := 0; i < len(n.Content)-1; i += 2 {
			keyNode := n.Content[i]
			valNode := n.Content[i+1]
			// Check if this key-value pair "contains" the target line.
			if keyNode.Line <= line && containsLine(valNode, line) {
				if commentSuppresses(keyNode.LineComment, ruleID) ||
					commentSuppresses(keyNode.HeadComment, ruleID) {
					return true
				}
				// Recurse into the value.
				return ancestorSuppresses(valNode, line, ruleID)
			}
		}
	}
	for _, c := range n.Content {
		if containsLine(c, line) {
			if ancestorSuppresses(c, line, ruleID) {
				return true
			}
		}
	}
	return false
}

// containsLine returns true if the node or any descendant is on the given line.
func containsLine(n *yaml.Node, line int) bool {
	if n == nil {
		return false
	}
	if n.Line == line {
		return true
	}
	for _, c := range n.Content {
		if containsLine(c, line) {
			return true
		}
	}
	return false
}

// findNodeAtLine walks the YAML tree and returns the first node at the given line.
func findNodeAtLine(n *yaml.Node, line int) *yaml.Node {
	if n == nil {
		return nil
	}
	if n.Line == line {
		return n
	}
	for _, c := range n.Content {
		if found := findNodeAtLine(c, line); found != nil {
			return found
		}
	}
	return nil
}

// commentSuppresses checks if a YAML comment suppresses the given rule ID.
func commentSuppresses(comment, ruleID string) bool {
	if comment == "" {
		return false
	}
	ms := suppressRe.FindAllStringSubmatch(comment, -1)
	for _, m := range ms {
		for _, id := range strings.Fields(m[1]) {
			if id == ruleID {
				return true
			}
		}
	}
	return false
}

// === YAML helpers (unexported) ===

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

// kebabCaseRe matches kebab-case path segments.
var kebabCaseRe = regexp.MustCompile(`^[a-z][a-z0-9]*(-[a-z0-9]+)*$`)
