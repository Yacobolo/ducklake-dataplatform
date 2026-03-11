// Package discovery provides a merged offline search corpus for CLI commands,
// generated docs metadata, and API operation metadata.
package discovery

import (
	"sort"
	"strings"
	"unicode"

	"github.com/sahilm/fuzzy"

	"duck-demo/pkg/cli/gen"
)

// CommandInfo is the subset of CLI command metadata used by the discovery
// corpus.
type CommandInfo struct {
	Path    string   `json:"path"`
	Group   string   `json:"group"`
	Short   string   `json:"short"`
	Long    string   `json:"long,omitempty"`
	Example string   `json:"example,omitempty"`
	Args    string   `json:"args,omitempty"`
	Flags   []string `json:"flags,omitempty"`
}

// SearchOptions controls corpus search behavior.
type SearchOptions struct {
	Kind  string
	Limit int
}

// SearchResult is a single ranked discovery result.
type SearchResult struct {
	Kind              string   `json:"kind"`
	ID                string   `json:"id"`
	Title             string   `json:"title"`
	Summary           string   `json:"summary"`
	Path              string   `json:"path,omitempty"`
	Score             int      `json:"score"`
	MatchedFields     []string `json:"matched_fields,omitempty"`
	RelatedCommands   []string `json:"related_commands,omitempty"`
	RelatedOperations []string `json:"related_operations,omitempty"`
	RelatedDocs       []string `json:"related_docs,omitempty"`
}

// Corpus is the merged search corpus used by discovery-oriented CLI commands.
type Corpus struct {
	index           gen.ReferenceIndex
	commands        map[string]CommandInfo
	docs            map[string]gen.ReferenceDoc
	operations      map[string]gen.ReferenceOperation
	commandToOps    map[string][]string
	commandToDocs   map[string][]string
	docToCommands   map[string][]string
	docToOps        map[string][]string
	opToCommands    map[string][]string
	opToDocs        map[string][]string
	fuzzyCandidates []fuzzyCandidate
}

type fuzzyCandidate struct {
	Kind  string
	ID    string
	Label string
}

// NewCorpus constructs a merged discovery corpus from live Cobra command
// metadata and generated docs/API metadata.
func NewCorpus(commands []CommandInfo, index gen.ReferenceIndex) Corpus {
	corpus := Corpus{
		index:           index,
		commands:        make(map[string]CommandInfo, len(commands)),
		docs:            make(map[string]gen.ReferenceDoc, len(index.Docs)),
		operations:      make(map[string]gen.ReferenceOperation, len(index.Operations)),
		commandToOps:    map[string][]string{},
		commandToDocs:   map[string][]string{},
		docToCommands:   map[string][]string{},
		docToOps:        map[string][]string{},
		opToCommands:    map[string][]string{},
		opToDocs:        map[string][]string{},
		fuzzyCandidates: make([]fuzzyCandidate, 0, len(commands)+len(index.Docs)+len(index.Operations)),
	}

	for _, command := range commands {
		key := normalizeID(command.Path)
		corpus.commands[key] = command
		corpus.fuzzyCandidates = append(corpus.fuzzyCandidates, fuzzyCandidate{Kind: "command", ID: key, Label: command.Path})
	}
	for _, doc := range index.Docs {
		corpus.docs[doc.ID] = doc
		corpus.fuzzyCandidates = append(corpus.fuzzyCandidates, fuzzyCandidate{Kind: "doc", ID: doc.ID, Label: doc.Title + " " + doc.Path})
	}
	for _, op := range index.Operations {
		corpus.operations[op.OperationID] = op
		corpus.fuzzyCandidates = append(corpus.fuzzyCandidates, fuzzyCandidate{Kind: "operation", ID: op.OperationID, Label: op.OperationID + " " + op.Path})
		if op.CLICommand != "" {
			commandKey := normalizeID(op.CLICommand)
			if _, ok := corpus.commands[commandKey]; ok {
				corpus.commandToOps[commandKey] = appendUnique(corpus.commandToOps[commandKey], op.OperationID)
				corpus.opToCommands[op.OperationID] = appendUnique(corpus.opToCommands[op.OperationID], commandKey)
			}
		}
	}

	for _, link := range index.Links {
		switch {
		case link.SourceKind == "doc" && link.TargetKind == "operation":
			if _, ok := corpus.docs[link.SourceID]; ok {
				if _, ok := corpus.operations[link.TargetID]; ok {
					corpus.docToOps[link.SourceID] = appendUnique(corpus.docToOps[link.SourceID], link.TargetID)
					corpus.opToDocs[link.TargetID] = appendUnique(corpus.opToDocs[link.TargetID], link.SourceID)
				}
			}
		case link.SourceKind == "doc" && link.TargetKind == "command":
			commandKey := normalizeID(link.TargetID)
			if _, ok := corpus.docs[link.SourceID]; ok {
				if _, ok := corpus.commands[commandKey]; ok {
					corpus.docToCommands[link.SourceID] = appendUnique(corpus.docToCommands[link.SourceID], commandKey)
					corpus.commandToDocs[commandKey] = appendUnique(corpus.commandToDocs[commandKey], link.SourceID)
				}
			}
		case link.SourceKind == "operation" && link.TargetKind == "doc":
			if _, ok := corpus.operations[link.SourceID]; ok {
				if _, ok := corpus.docs[link.TargetID]; ok {
					corpus.opToDocs[link.SourceID] = appendUnique(corpus.opToDocs[link.SourceID], link.TargetID)
					corpus.docToOps[link.TargetID] = appendUnique(corpus.docToOps[link.TargetID], link.SourceID)
				}
			}
		case link.SourceKind == "operation" && link.TargetKind == "command":
			commandKey := normalizeID(link.TargetID)
			if _, ok := corpus.operations[link.SourceID]; ok {
				if _, ok := corpus.commands[commandKey]; ok {
					corpus.opToCommands[link.SourceID] = appendUnique(corpus.opToCommands[link.SourceID], commandKey)
					corpus.commandToOps[commandKey] = appendUnique(corpus.commandToOps[commandKey], link.SourceID)
				}
			}
		}
	}

	for commandKey, opIDs := range corpus.commandToOps {
		for _, opID := range opIDs {
			for _, docID := range corpus.opToDocs[opID] {
				corpus.commandToDocs[commandKey] = appendUnique(corpus.commandToDocs[commandKey], docID)
				corpus.docToCommands[docID] = appendUnique(corpus.docToCommands[docID], commandKey)
			}
		}
	}

	return corpus
}

// FindDoc resolves a document by ID, path stem, or unique exact title.
func (c Corpus) FindDoc(idOrPath string) (*gen.ReferenceDoc, bool) {
	key := strings.TrimSpace(idOrPath)
	if key == "" {
		return nil, false
	}
	if doc, ok := c.docs[key]; ok {
		return &doc, true
	}
	key = strings.TrimSuffix(key, ".md")
	for _, doc := range c.docs {
		if doc.Path == key || strings.TrimSuffix(doc.Path, ".md") == key || strings.EqualFold(doc.Title, idOrPath) {
			docCopy := doc
			return &docCopy, true
		}
	}
	return nil, false
}

// FindOperation resolves an API operation by operation ID.
func (c Corpus) FindOperation(opID string) (*gen.ReferenceOperation, bool) {
	op, ok := c.operations[strings.TrimSpace(opID)]
	if !ok {
		return nil, false
	}
	return &op, true
}

// ListDocs returns all docs, optionally filtered by top-level section.
func (c Corpus) ListDocs(section string) []gen.ReferenceDoc {
	section = strings.TrimSpace(section)
	out := make([]gen.ReferenceDoc, 0, len(c.docs))
	for _, doc := range c.docs {
		if section != "" && doc.Section != section {
			continue
		}
		out = append(out, doc)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Section == out[j].Section {
			return out[i].ID < out[j].ID
		}
		return out[i].Section < out[j].Section
	})
	return out
}

// RelatedCommandsForDoc returns command IDs linked to the given document.
func (c Corpus) RelatedCommandsForDoc(docID string) []string {
	return append([]string(nil), c.docToCommands[docID]...)
}

// RelatedOperationsForDoc returns operation IDs linked to the given document.
func (c Corpus) RelatedOperationsForDoc(docID string) []string {
	return append([]string(nil), c.docToOps[docID]...)
}

// RelatedDocsForOperation returns doc IDs linked to the given operation.
func (c Corpus) RelatedDocsForOperation(opID string) []string {
	return append([]string(nil), c.opToDocs[opID]...)
}

// RelatedCommandsForOperation returns command IDs linked to the given operation.
func (c Corpus) RelatedCommandsForOperation(opID string) []string {
	return append([]string(nil), c.opToCommands[opID]...)
}

// Search performs ranked offline search across the merged discovery corpus.
func (c Corpus) Search(query string, opts SearchOptions) []SearchResult {
	query = strings.TrimSpace(query)
	if query == "" {
		return nil
	}
	queryTokens := tokenize(query)
	results := c.exactSearch(query, queryTokens, opts.Kind)
	if len(results) < 5 {
		results = c.mergeFuzzy(results, query, opts.Kind)
	}
	sortResults(results)
	if opts.Limit > 0 && len(results) > opts.Limit {
		results = results[:opts.Limit]
	}
	return results
}

func (c Corpus) exactSearch(query string, queryTokens []string, kind string) []SearchResult {
	results := make([]SearchResult, 0)
	for key, command := range c.commands {
		if !matchKind("command", kind) {
			continue
		}
		if result, ok := scoreCommand(query, queryTokens, key, command, c.commandToOps[key], c.commandToDocs[key]); ok {
			results = append(results, result)
		}
	}
	for id, op := range c.operations {
		if !matchKind("operation", kind) {
			continue
		}
		if result, ok := scoreOperation(query, queryTokens, op, c.opToCommands[id], c.opToDocs[id]); ok {
			results = append(results, result)
		}
	}
	for id, doc := range c.docs {
		if !matchKind("doc", kind) {
			continue
		}
		if result, ok := scoreDoc(query, queryTokens, doc, c.docToCommands[id], c.docToOps[id]); ok {
			results = append(results, result)
		}
	}
	return dedupeResults(results)
}

func (c Corpus) mergeFuzzy(results []SearchResult, query, kind string) []SearchResult {
	existing := make(map[string]struct{}, len(results))
	for _, result := range results {
		existing[result.Kind+":"+result.ID] = struct{}{}
	}
	matches := fuzzy.Find(strings.ToLower(query), lowerCandidateLabels(c.fuzzyCandidates))
	for _, match := range matches {
		candidate := c.fuzzyCandidates[match.Index]
		if !matchKind(candidate.Kind, kind) {
			continue
		}
		key := candidate.Kind + ":" + candidate.ID
		if _, ok := existing[key]; ok {
			continue
		}
		switch candidate.Kind {
		case "command":
			command := c.commands[candidate.ID]
			results = append(results, SearchResult{
				Kind:              "command",
				ID:                candidate.ID,
				Title:             command.Path,
				Summary:           command.Short,
				Path:              command.Path,
				Score:             35,
				MatchedFields:     []string{"fuzzy"},
				RelatedOperations: append([]string(nil), c.commandToOps[candidate.ID]...),
				RelatedDocs:       append([]string(nil), c.commandToDocs[candidate.ID]...),
			})
		case "operation":
			op := c.operations[candidate.ID]
			results = append(results, SearchResult{
				Kind:            "operation",
				ID:              candidate.ID,
				Title:           op.OperationID,
				Summary:         op.Summary,
				Path:            op.Path,
				Score:           35,
				MatchedFields:   []string{"fuzzy"},
				RelatedCommands: append([]string(nil), c.opToCommands[candidate.ID]...),
				RelatedDocs:     append([]string(nil), c.opToDocs[candidate.ID]...),
			})
		case "doc":
			doc := c.docs[candidate.ID]
			results = append(results, SearchResult{
				Kind:              "doc",
				ID:                candidate.ID,
				Title:             doc.Title,
				Summary:           firstNonEmpty(doc.Description, doc.Excerpt),
				Path:              doc.Path,
				Score:             35,
				MatchedFields:     []string{"fuzzy"},
				RelatedCommands:   append([]string(nil), c.docToCommands[candidate.ID]...),
				RelatedOperations: append([]string(nil), c.docToOps[candidate.ID]...),
			})
		}
	}
	return dedupeResults(results)
}

func scoreCommand(query string, queryTokens []string, key string, command CommandInfo, relatedOps, relatedDocs []string) (SearchResult, bool) {
	fields := []weightedField{
		{name: "title", value: command.Path, weight: 100},
		{name: "summary", value: command.Short, weight: 50},
		{name: "description", value: command.Long, weight: 10},
	}
	for _, flag := range command.Flags {
		fields = append(fields, weightedField{name: "flag", value: flag, weight: 30})
	}
	score, matched := scoreFields(query, queryTokens, fields)
	if score == 0 {
		return SearchResult{}, false
	}
	score += 200
	score += intentBoost(queryTokens, command.Path, 2200)
	score += linkedBoost(len(relatedOps) + len(relatedDocs))
	return SearchResult{
		Kind:              "command",
		ID:                key,
		Title:             command.Path,
		Summary:           command.Short,
		Path:              command.Path,
		Score:             score,
		MatchedFields:     matched,
		RelatedOperations: append([]string(nil), relatedOps...),
		RelatedDocs:       append([]string(nil), relatedDocs...),
	}, true
}

func scoreOperation(query string, queryTokens []string, op gen.ReferenceOperation, relatedCommands, relatedDocs []string) (SearchResult, bool) {
	fields := []weightedField{
		{name: "title", value: op.OperationID, weight: 100},
		{name: "path", value: op.Path, weight: 100},
		{name: "cli_command", value: op.CLICommand, weight: 100},
		{name: "summary", value: op.Summary, weight: 50},
		{name: "description", value: op.Description, weight: 10},
	}
	for _, tag := range op.Tags {
		fields = append(fields, weightedField{name: "tag", value: tag, weight: 30})
	}
	for _, param := range op.Parameters {
		fields = append(fields, weightedField{name: "parameter", value: param.Name, weight: 30})
	}
	for _, field := range op.BodyFields {
		fields = append(fields, weightedField{name: "parameter", value: field.Name, weight: 30})
	}
	score, matched := scoreFields(query, queryTokens, fields)
	if score == 0 {
		return SearchResult{}, false
	}
	score += intentBoost(queryTokens, op.CLICommand, 700)
	score += intentBoost(queryTokens, op.Path, 400)
	score += intentBoost(queryTokens, op.OperationID+" "+op.Summary, 1200)
	score += linkedBoost(len(relatedCommands) + len(relatedDocs))
	return SearchResult{
		Kind:            "operation",
		ID:              op.OperationID,
		Title:           op.OperationID,
		Summary:         firstNonEmpty(op.Summary, op.Description),
		Path:            op.Path,
		Score:           score,
		MatchedFields:   matched,
		RelatedCommands: append([]string(nil), relatedCommands...),
		RelatedDocs:     append([]string(nil), relatedDocs...),
	}, true
}

func scoreDoc(query string, queryTokens []string, doc gen.ReferenceDoc, relatedCommands, relatedOps []string) (SearchResult, bool) {
	fields := []weightedField{
		{name: "title", value: doc.Title, weight: 100},
		{name: "path", value: doc.Path, weight: 100},
		{name: "summary", value: doc.Description, weight: 50},
		{name: "excerpt", value: doc.Excerpt, weight: 10},
	}
	for _, heading := range doc.Headings {
		fields = append(fields, weightedField{name: "heading", value: heading, weight: 50})
	}
	for _, keyword := range doc.Keywords {
		fields = append(fields, weightedField{name: "keyword", value: keyword, weight: 30})
	}
	for _, example := range doc.CodeExamples {
		fields = append(fields, weightedField{name: "example", value: example, weight: 10})
	}
	score, matched := scoreFields(query, queryTokens, fields)
	if score == 0 {
		return SearchResult{}, false
	}
	score += intentBoost(queryTokens, doc.Title+" "+doc.Path, 250)
	score += linkedBoost(len(relatedCommands) + len(relatedOps))
	return SearchResult{
		Kind:              "doc",
		ID:                doc.ID,
		Title:             doc.Title,
		Summary:           firstNonEmpty(doc.Description, doc.Excerpt),
		Path:              doc.Path,
		Score:             score,
		MatchedFields:     matched,
		RelatedCommands:   append([]string(nil), relatedCommands...),
		RelatedOperations: append([]string(nil), relatedOps...),
	}, true
}

type weightedField struct {
	name   string
	value  string
	weight int
}

func scoreFields(query string, queryTokens []string, fields []weightedField) (int, []string) {
	score := 0
	matched := make([]string, 0, 4)
	queryNorm := normalizeID(query)
	for _, field := range fields {
		valueNorm := normalizeID(field.value)
		if valueNorm == "" {
			continue
		}
		fieldMatched := false
		if valueNorm == queryNorm {
			score += field.weight + 40
			fieldMatched = true
		} else if strings.HasPrefix(valueNorm, queryNorm) {
			score += field.weight + 20
			fieldMatched = true
		}

		valueTokens := tokenize(field.value)
		matches := 0
		for _, token := range queryTokens {
			if containsToken(valueTokens, token) {
				matches++
			}
		}
		if matches > 0 {
			score += field.weight
			if matches > 1 {
				score += (matches - 1) * maxInt(field.weight/2, 1)
			}
			fieldMatched = true
		}
		if fieldMatched {
			matched = appendUnique(matched, field.name)
		}
	}
	return score, matched
}

func linkedBoost(count int) int {
	if count <= 0 {
		return 0
	}
	return count * 15
}

func sortResults(results []SearchResult) {
	sort.Slice(results, func(i, j int) bool {
		if results[i].Score != results[j].Score {
			return results[i].Score > results[j].Score
		}
		if kindRank(results[i].Kind) != kindRank(results[j].Kind) {
			return kindRank(results[i].Kind) < kindRank(results[j].Kind)
		}
		return results[i].Title < results[j].Title
	})
}

func dedupeResults(results []SearchResult) []SearchResult {
	best := make(map[string]SearchResult, len(results))
	for _, result := range results {
		key := result.Kind + ":" + result.ID
		existing, ok := best[key]
		if !ok || result.Score > existing.Score {
			best[key] = result
		}
	}
	out := make([]SearchResult, 0, len(best))
	for _, result := range best {
		out = append(out, result)
	}
	return out
}

func matchKind(candidate, filter string) bool {
	filter = strings.TrimSpace(filter)
	return filter == "" || filter == "all" || filter == candidate
}

func kindRank(kind string) int {
	switch kind {
	case "command":
		return 0
	case "operation":
		return 1
	default:
		return 2
	}
}

func appendUnique(values []string, value string) []string {
	for _, existing := range values {
		if existing == value {
			return values
		}
	}
	return append(values, value)
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}

func normalizeID(value string) string {
	return strings.Join(strings.Fields(strings.ToLower(strings.TrimSpace(value))), " ")
}

func tokenize(value string) []string {
	value = strings.TrimSpace(value)
	if value == "" {
		return nil
	}
	var out []string
	var current []rune
	flush := func() {
		if len(current) == 0 {
			return
		}
		out = append(out, strings.ToLower(string(current)))
		current = current[:0]
	}
	for _, r := range value {
		switch {
		case unicode.IsLetter(r) || unicode.IsDigit(r):
			if len(current) > 0 && unicode.IsUpper(r) && unicode.IsLower(current[len(current)-1]) {
				flush()
			}
			current = append(current, r)
		default:
			flush()
		}
	}
	flush()
	return expandTokenVariants(out)
}

func containsToken(tokens []string, want string) bool {
	for _, token := range tokens {
		if token == want {
			return true
		}
	}
	return false
}

func lowerCandidateLabels(values []fuzzyCandidate) []string {
	out := make([]string, 0, len(values))
	for _, value := range values {
		out = append(out, strings.ToLower(value.Label))
	}
	return out
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func intentBoost(queryTokens []string, candidate string, fullCoverageBoost int) int {
	if len(queryTokens) < 2 {
		return 0
	}
	candidateTokens := tokenize(candidate)
	if len(candidateTokens) == 0 {
		return 0
	}
	matches := 0
	for _, queryToken := range queryTokens {
		if containsToken(candidateTokens, queryToken) {
			matches++
		}
	}
	if matches == 0 {
		return 0
	}
	if matches == len(queryTokens) {
		return fullCoverageBoost
	}
	return matches * maxInt(fullCoverageBoost/5, 1)
}

func expandTokenVariants(tokens []string) []string {
	out := make([]string, 0, len(tokens)*2)
	seen := make(map[string]struct{}, len(tokens)*2)
	for _, token := range tokens {
		for _, candidate := range []string{token, singularize(token)} {
			candidate = strings.TrimSpace(candidate)
			if candidate == "" {
				continue
			}
			if _, ok := seen[candidate]; ok {
				continue
			}
			seen[candidate] = struct{}{}
			out = append(out, candidate)
		}
	}
	return out
}

func singularize(token string) string {
	switch {
	case strings.HasSuffix(token, "ies") && len(token) > 3:
		return token[:len(token)-3] + "y"
	case strings.HasSuffix(token, "ses") && len(token) > 3:
		return token[:len(token)-2]
	case strings.HasSuffix(token, "s") && len(token) > 3:
		return token[:len(token)-1]
	default:
		return token
	}
}
