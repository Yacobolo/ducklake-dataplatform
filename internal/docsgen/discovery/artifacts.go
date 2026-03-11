package discovery

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"
)

type declarativeManifest struct {
	Files map[string]string `json:"files"`
}

type coverageReport struct {
	APITags            []string `json:"api_tags"`
	CoveredAPITags     []string `json:"covered_api_tags"`
	MissingAPITags     []string `json:"missing_api_tags"`
	DeclarativeKinds   []string `json:"declarative_kinds"`
	CoveredDeclarative []string `json:"covered_declarative_kinds"`
	MissingDeclarative []string `json:"missing_declarative_kinds"`
	DocIDs             []string `json:"doc_ids"`
}

type agentIndex struct {
	Docs       []referenceDoc       `json:"docs"`
	Operations []referenceOperation `json:"operations"`
	Links      []referenceLink      `json:"links"`
	Coverage   coverageReport       `json:"coverage"`
}

func loadDeclarativeKinds(indexPath string) ([]string, error) {
	if strings.TrimSpace(indexPath) == "" {
		return nil, nil
	}

	bytes, err := os.ReadFile(filepath.Clean(indexPath)) // #nosec G304 -- trusted local build input
	if err != nil {
		return nil, fmt.Errorf("read declarative index: %w", err)
	}

	var manifest declarativeManifest
	if err := json.Unmarshal(bytes, &manifest); err != nil {
		return nil, fmt.Errorf("decode declarative index: %w", err)
	}

	kinds := make([]string, 0, len(manifest.Files))
	for name := range manifest.Files {
		if !strings.HasPrefix(name, "kinds/") || !strings.HasSuffix(name, ".schema.json") {
			continue
		}
		kind := strings.TrimSuffix(strings.TrimPrefix(name, "kinds/"), ".schema.json")
		kinds = append(kinds, kind)
	}
	slices.Sort(kinds)
	return kinds, nil
}

func validateDocs(docs []referenceDoc, operations []referenceOperation, declarativeKinds []string) error {
	docByID := make(map[string]referenceDoc, len(docs))
	opByID := make(map[string]struct{}, len(operations))
	apiTags := make(map[string]struct{})

	for _, doc := range docs {
		docByID[doc.ID] = doc
	}
	for _, op := range operations {
		opByID[op.OperationID] = struct{}{}
		for _, tag := range op.Tags {
			apiTags[tag] = struct{}{}
		}
	}

	declarativeKindSet := make(map[string]struct{}, len(declarativeKinds))
	for _, kind := range declarativeKinds {
		declarativeKindSet[kind] = struct{}{}
	}

	for _, doc := range docs {
		if strings.TrimSpace(doc.Title) == "" {
			return fmt.Errorf("doc %q: title is required", doc.ID)
		}
		if strings.TrimSpace(doc.Description) == "" {
			return fmt.Errorf("doc %q: description is required", doc.ID)
		}
		if strings.TrimSpace(doc.DocKind) == "" {
			return fmt.Errorf("doc %q: doc_kind is required", doc.ID)
		}
		for _, required := range []struct {
			name   string
			values []string
		}{
			{name: "audiences", values: doc.Audiences},
			{name: "product_areas", values: doc.ProductAreas},
			{name: "surfaces", values: doc.Surfaces},
			{name: "tasks", values: doc.Tasks},
			{name: "prerequisites", values: doc.Prerequisites},
			{name: "permissions", values: doc.Permissions},
			{name: "related_docs", values: doc.RelatedDocs},
			{name: "source_of_truth", values: doc.SourceOfTruth},
		} {
			if len(required.values) == 0 {
				return fmt.Errorf("doc %q: %s is required", doc.ID, required.name)
			}
		}

		if _, err := time.Parse("2006-01-02", doc.LastVerified); err != nil {
			return fmt.Errorf("doc %q: last_verified must be YYYY-MM-DD: %w", doc.ID, err)
		}

		for _, relatedDoc := range doc.RelatedDocs {
			if _, ok := docByID[relatedDoc]; !ok {
				return fmt.Errorf("doc %q: related_docs references unknown doc %q", doc.ID, relatedDoc)
			}
		}
		for _, opID := range doc.OperationIDs {
			if _, ok := opByID[opID]; !ok {
				return fmt.Errorf("doc %q: operation_ids references unknown operation %q", doc.ID, opID)
			}
		}
		for _, tag := range doc.APITags {
			if tag == "*" {
				continue
			}
			if _, ok := apiTags[tag]; !ok {
				return fmt.Errorf("doc %q: api_tags references unknown tag %q", doc.ID, tag)
			}
		}
		for _, kind := range doc.DeclarativeKinds {
			if kind == "*" {
				continue
			}
			if _, ok := declarativeKindSet[kind]; !ok {
				return fmt.Errorf("doc %q: declarative_kinds references unknown kind %q", doc.ID, kind)
			}
		}
	}

	coverage := buildCoverage(docs, operations, declarativeKinds)
	if len(coverage.MissingAPITags) > 0 {
		return fmt.Errorf("docs coverage missing API tags: %s", strings.Join(coverage.MissingAPITags, ", "))
	}
	if len(coverage.MissingDeclarative) > 0 {
		return fmt.Errorf("docs coverage missing declarative kinds: %s", strings.Join(coverage.MissingDeclarative, ", "))
	}

	return nil
}

func buildCoverage(docs []referenceDoc, operations []referenceOperation, declarativeKinds []string) coverageReport {
	apiTagSet := make(map[string]struct{})
	for _, op := range operations {
		for _, tag := range op.Tags {
			apiTagSet[tag] = struct{}{}
		}
	}

	coveredAPITags := make(map[string]struct{})
	for _, doc := range docs {
		for _, tag := range doc.APITags {
			if tag == "*" {
				for candidate := range apiTagSet {
					coveredAPITags[candidate] = struct{}{}
				}
				continue
			}
			coveredAPITags[tag] = struct{}{}
		}
	}

	coveredKinds := make(map[string]struct{})
	for _, doc := range docs {
		for _, kind := range doc.DeclarativeKinds {
			if kind == "*" {
				for _, candidate := range declarativeKinds {
					coveredKinds[candidate] = struct{}{}
				}
				continue
			}
			coveredKinds[kind] = struct{}{}
		}
	}

	apiTags := sortedKeys(apiTagSet)
	missingAPITags := make([]string, 0)
	for _, tag := range apiTags {
		if _, ok := coveredAPITags[tag]; !ok {
			missingAPITags = append(missingAPITags, tag)
		}
	}

	missingKinds := make([]string, 0)
	for _, kind := range declarativeKinds {
		if _, ok := coveredKinds[kind]; !ok {
			missingKinds = append(missingKinds, kind)
		}
	}

	docIDs := make([]string, 0, len(docs))
	for _, doc := range docs {
		docIDs = append(docIDs, doc.ID)
	}
	slices.Sort(docIDs)

	return coverageReport{
		APITags:            apiTags,
		CoveredAPITags:     sortedKeys(coveredAPITags),
		MissingAPITags:     missingAPITags,
		DeclarativeKinds:   append([]string(nil), declarativeKinds...),
		CoveredDeclarative: sortedKeys(coveredKinds),
		MissingDeclarative: missingKinds,
		DocIDs:             docIDs,
	}
}

func writeAgentArtifacts(outDir string, docs []referenceDoc, operations []referenceOperation, links []referenceLink, coverage coverageReport) error {
	if err := os.MkdirAll(outDir, 0o750); err != nil {
		return fmt.Errorf("create docs public dir: %w", err)
	}

	indexPayload, err := json.MarshalIndent(agentIndex{
		Docs:       docs,
		Operations: operations,
		Links:      links,
		Coverage:   coverage,
	}, "", "  ")
	if err != nil {
		return fmt.Errorf("encode agent index: %w", err)
	}
	if err := os.WriteFile(filepath.Join(outDir, "agent-index.json"), append(indexPayload, '\n'), 0o600); err != nil {
		return fmt.Errorf("write agent index: %w", err)
	}

	if err := os.WriteFile(filepath.Join(outDir, "llms.txt"), []byte(renderLLMSIndex(docs, coverage)), 0o600); err != nil {
		return fmt.Errorf("write llms.txt: %w", err)
	}
	if err := os.WriteFile(filepath.Join(outDir, "llms-full.txt"), []byte(renderLLMSFull(docs)), 0o600); err != nil {
		return fmt.Errorf("write llms-full.txt: %w", err)
	}

	return nil
}

func renderLLMSIndex(docs []referenceDoc, coverage coverageReport) string {
	var b strings.Builder
	b.WriteString("# Duck Data Platform Docs\n\n")
	b.WriteString("> AI-first product documentation for users, admins, builders, and agents.\n\n")
	b.WriteString("## Core docs\n\n")
	for _, doc := range docs {
		b.WriteString(fmt.Sprintf("- [%s](/%s): %s\n", doc.Title, strings.TrimSuffix(doc.Path, ".md"), doc.Description))
	}
	b.WriteString("\n## Machine-readable artifacts\n\n")
	b.WriteString("- [/agent-index.json](/agent-index.json): structured documentation graph for agent retrieval.\n")
	b.WriteString("- [/llms-full.txt](/llms-full.txt): concatenated markdown corpus for long-context ingestion.\n")
	b.WriteString("\n## Coverage summary\n\n")
	b.WriteString(fmt.Sprintf("- API tags covered: %d/%d\n", len(coverage.CoveredAPITags), len(coverage.APITags)))
	b.WriteString(fmt.Sprintf("- Declarative kinds covered: %d/%d\n", len(coverage.CoveredDeclarative), len(coverage.DeclarativeKinds)))
	return b.String()
}

func renderLLMSFull(docs []referenceDoc) string {
	var b strings.Builder
	b.WriteString("# Duck Data Platform Full Docs Export\n\n")
	for _, doc := range docs {
		b.WriteString(fmt.Sprintf("## %s\n\n", doc.Title))
		b.WriteString(fmt.Sprintf("- Path: /%s\n", strings.TrimSuffix(doc.Path, ".md")))
		b.WriteString(fmt.Sprintf("- Doc kind: %s\n", doc.DocKind))
		b.WriteString(fmt.Sprintf("- Audiences: %s\n", strings.Join(doc.Audiences, ", ")))
		b.WriteString(fmt.Sprintf("- Product areas: %s\n", strings.Join(doc.ProductAreas, ", ")))
		b.WriteString(fmt.Sprintf("- Surfaces: %s\n", strings.Join(doc.Surfaces, ", ")))
		b.WriteString(fmt.Sprintf("- Tasks: %s\n", strings.Join(doc.Tasks, ", ")))
		b.WriteString(fmt.Sprintf("- Last verified: %s\n\n", doc.LastVerified))
		b.WriteString(doc.Body)
		b.WriteString("\n\n")
	}
	return b.String()
}
