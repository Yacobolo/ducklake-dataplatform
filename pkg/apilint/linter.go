// Package apilint provides an OpenAPI 3.x linter powered by vacuum with
// project-specific custom rules. It uses vacuum as a Go library for built-in
// rules (Spectral-compatible) and registers custom Go functions for
// domain-specific conventions.
package apilint

import (
	_ "embed"
	"fmt"
	"os"
	"sort"

	"github.com/daveshanley/vacuum/model"
	"github.com/daveshanley/vacuum/motor"
	"github.com/daveshanley/vacuum/rulesets"
)

//go:embed ruleset.yaml
var embeddedRuleset []byte

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

// RuleInfo describes a registered rule for introspection (e.g. --list-rules).
type RuleInfo struct {
	ID          string
	Severity    string
	Description string
}

// Linter holds the spec bytes and runs vacuum + custom rules against it.
type Linter struct {
	file     string
	specData []byte
}

// New parses the given YAML file and returns a Linter.
func New(path string) (*Linter, error) {
	data, err := os.ReadFile(path) //nolint:gosec // path is provided by the caller
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", path, err)
	}
	return &Linter{file: path, specData: data}, nil
}

// Run executes all lint rules with default severity and returns violations sorted by line number.
func (l *Linter) Run() []Violation {
	return l.RunWithConfig(nil)
}

// RunWithConfig executes all vacuum + custom rules using the given configuration (may be nil).
// Rules with severity overridden to "off" are skipped.
func (l *Linter) RunWithConfig(cfg *Config) []Violation {
	// Build the ruleset from embedded YAML.
	rs := buildRuleSet(cfg)
	if rs == nil {
		return nil
	}

	// Execute vacuum with custom functions.
	result := motor.ApplyRulesToRuleSet(&motor.RuleSetExecution{
		RuleSet:         rs,
		Spec:            l.specData,
		SpecFileName:    l.file,
		CustomFunctions: customFunctions(),
		SilenceLogs:     true,
	})

	// Convert results to our Violation type.
	var vs []Violation
	for _, r := range result.Results {
		sev := mapVacuumSeverity(r.RuleSeverity)
		ruleID := r.RuleId

		// Apply config overrides.
		if cfg != nil {
			if override, ok := cfg.Rules[ruleID]; ok {
				if override == "off" {
					continue
				}
				sev = Severity(override)
			}
		}

		line := 0
		if r.StartNode != nil {
			line = int(r.StartNode.Line)
		}

		vs = append(vs, Violation{
			File:     l.file,
			Line:     line,
			RuleID:   ruleID,
			Severity: sev,
			Message:  r.Message,
		})
	}

	sort.Slice(vs, func(i, j int) bool { return vs[i].Line < vs[j].Line })
	return vs
}

// RegisteredRules returns rule info for all active rules in the embedded ruleset.
func RegisteredRules() []RuleInfo {
	rs := buildRuleSet(nil)
	if rs == nil {
		return nil
	}

	var rules []RuleInfo
	for id, r := range rs.Rules {
		desc := ""
		if r != nil {
			desc = r.Description
		}
		sev := "warn"
		if r != nil && r.Severity != "" {
			sev = r.Severity
		}
		rules = append(rules, RuleInfo{
			ID:          id,
			Severity:    sev,
			Description: desc,
		})
	}

	sort.Slice(rules, func(i, j int) bool { return rules[i].ID < rules[j].ID })
	return rules
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

// buildRuleSet creates a vacuum RuleSet from the embedded YAML, merging with
// built-in defaults. Config overrides are applied at the result mapping stage.
func buildRuleSet(cfg *Config) *rulesets.RuleSet {
	// Parse the embedded ruleset.
	parsed, err := rulesets.CreateRuleSetFromData(embeddedRuleset)
	if err != nil {
		// This should never fail with a valid embedded ruleset.
		fmt.Fprintf(os.Stderr, "apilint: failed to parse embedded ruleset: %v\n", err)
		return nil
	}

	// Merge with defaults (resolves extends, etc.).
	defaultRS := rulesets.BuildDefaultRuleSets()
	rs := defaultRS.GenerateRuleSetFromSuppliedRuleSet(parsed)

	// Apply "off" overrides from config by removing rules from the set.
	if cfg != nil {
		for ruleID, override := range cfg.Rules {
			if override == "off" {
				delete(rs.Rules, ruleID)
			}
		}
	}

	return rs
}

// mapVacuumSeverity maps vacuum severity strings to our Severity type.
func mapVacuumSeverity(s string) Severity {
	switch s {
	case model.SeverityError:
		return SeverityError
	case model.SeverityWarn:
		return SeverityWarning
	case model.SeverityInfo, model.SeverityHint:
		return SeverityInfo
	default:
		return SeverityWarning
	}
}
