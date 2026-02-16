package architecture_test

import (
	"os"
	"regexp"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGovernanceRules_ParityAcrossTestsLintAndDocs(t *testing.T) {
	t.Helper()

	fromTests := architectureRuleMap()
	fromDepguard := parseDepguardGovernanceRules(t, repoRootDir()+"/.golangci.yml")
	fromDocs := parseDocsGovernanceRules(t, repoRootDir()+"/docs/architecture-governance.md")

	require.Equal(t, fromTests, fromDepguard, "governance: architecture test rules and depguard rules must match")
	require.Equal(t, fromTests, fromDocs, "governance: architecture test rules and docs rules must match")
}

func parseDepguardGovernanceRules(t *testing.T, configPath string) map[string][]string {
	t.Helper()

	b, err := os.ReadFile(configPath)
	require.NoError(t, err)

	lines := strings.Split(string(b), "\n")

	ruleToSource := map[string]string{
		"governance_domain":     modulePath + "/internal/domain",
		"governance_service":    modulePath + "/internal/service",
		"governance_api":        modulePath + "/internal/api",
		"governance_db":         modulePath + "/internal/db",
		"governance_engine":     modulePath + "/internal/engine",
		"governance_middleware": modulePath + "/internal/middleware",
	}

	blockPattern := regexp.MustCompile(`^\s{8}(governance_[a-z]+):\s*$`)
	otherBlockPattern := regexp.MustCompile(`^\s{8}([a-z_]+):\s*$`)
	pkgPattern := regexp.MustCompile(`^\s{12}- pkg: "([^"]+)"\s*$`)

	current := ""
	parsed := make(map[string][]string)
	for _, line := range lines {
		if m := blockPattern.FindStringSubmatch(line); len(m) == 2 {
			current = m[1]
			continue
		}
		if m := otherBlockPattern.FindStringSubmatch(line); len(m) == 2 && !strings.HasPrefix(m[1], "governance_") {
			current = ""
			continue
		}
		if current == "" {
			continue
		}
		if m := pkgPattern.FindStringSubmatch(line); len(m) == 2 {
			source := ruleToSource[current]
			parsed[source] = append(parsed[source], m[1])
		}
	}

	for source, forbidden := range parsed {
		sort.Strings(forbidden)
		parsed[source] = forbidden
	}

	return parsed
}

func parseDocsGovernanceRules(t *testing.T, docsPath string) map[string][]string {
	t.Helper()

	b, err := os.ReadFile(docsPath)
	require.NoError(t, err)

	lines := strings.Split(string(b), "\n")
	layerHeader := regexp.MustCompile(`^- ` + "`" + `(internal/[a-z]+/\*\*)` + "`" + `$`)
	inlineCode := regexp.MustCompile("`([^`]+)`")

	parsed := make(map[string][]string)
	currentSource := ""
	for _, rawLine := range lines {
		line := strings.TrimSpace(rawLine)
		if m := layerHeader.FindStringSubmatch(line); len(m) == 2 {
			currentSource = modulePath + "/" + strings.TrimSuffix(m[1], "/**")
			continue
		}
		if currentSource == "" || !strings.Contains(line, "must not import:") {
			continue
		}

		matches := inlineCode.FindAllStringSubmatch(line, -1)
		for _, m := range matches {
			raw := strings.TrimSuffix(m[1], "/**")
			if raw == "" {
				continue
			}
			parsed[currentSource] = append(parsed[currentSource], modulePath+"/"+raw)
		}
	}

	for source, forbidden := range parsed {
		sort.Strings(forbidden)
		parsed[source] = forbidden
	}

	return parsed
}
