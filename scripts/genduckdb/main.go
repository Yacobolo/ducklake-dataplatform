// Package main generates Go source files containing DuckDB syntax metadata
// by querying DuckDB's system catalog tables.
//
// Usage:
//
//	go run . -gen=all -outdir=../../internal/duckdbsql/catalog/
//	go run . -gen=keywords -out=../../internal/duckdbsql/catalog/keywords_gen.go
//	go run . -gen=types -out=../../internal/duckdbsql/catalog/types_gen.go
//	go run . -gen=functions -out=../../internal/duckdbsql/catalog/functions_gen.go
package main

import (
	"bytes"
	"context"
	"database/sql"
	"flag"
	"fmt"
	"go/format"
	"log"
	"os"
	"regexp"
	"sort"
	"strings"
	"time"

	_ "github.com/marcboeker/go-duckdb"
)

var (
	genFlag    = flag.String("gen", "all", "what to generate: keywords, types, functions, all")
	outFlag    = flag.String("out", "", "output file path (required for single generation)")
	outDirFlag = flag.String("outdir", "", "output directory (for 'all' generation)")
)

func main() {
	flag.Parse()

	if *genFlag == "all" {
		if *outDirFlag == "" {
			log.Fatal("--outdir flag is required when using -gen=all")
		}
	} else if *outFlag == "" {
		log.Fatal("--out flag is required")
	}

	db, err := sql.Open("duckdb", "")
	if err != nil {
		log.Fatalf("failed to open duckdb: %v", err)
	}

	ctx := context.Background()

	var version string
	if err := db.QueryRowContext(ctx, "SELECT version()").Scan(&version); err != nil {
		_ = db.Close()
		log.Fatalf("failed to get version: %v", err)
	}
	log.Printf("Connected to DuckDB %s", version)

	validGenFlags := map[string]bool{"functions": true, "keywords": true, "types": true, "all": true}
	if !validGenFlags[*genFlag] {
		_ = db.Close()
		log.Fatalf("unknown -gen value: %s (use: functions, keywords, types, all)", *genFlag)
	}

	defer func() { _ = db.Close() }()

	switch *genFlag {
	case "functions":
		generateFunctionsFile(ctx, db, version, *outFlag)
	case "keywords":
		generateKeywordsFile(ctx, db, version, *outFlag)
	case "types":
		generateTypesFile(ctx, db, version, *outFlag)
	case "all":
		generateKeywordsFile(ctx, db, version, *outDirFlag+"/keywords_gen.go")
		generateTypesFile(ctx, db, version, *outDirFlag+"/types_gen.go")
		generateFunctionsFile(ctx, db, version, *outDirFlag+"/functions_gen.go")
	}
}

// === Keywords ===

func generateKeywordsFile(ctx context.Context, db *sql.DB, version, outPath string) {
	reserved, err := extractKeywords(ctx, db, true)
	if err != nil {
		log.Fatalf("failed to extract reserved keywords: %v", err)
	}
	log.Printf("Extracted %d reserved keywords", len(reserved))

	all, err := extractKeywords(ctx, db, false)
	if err != nil {
		log.Fatalf("failed to extract all keywords: %v", err)
	}
	log.Printf("Extracted %d total keywords", len(all))

	code := generateKeywordsCode(version, reserved, all)
	writeFormattedCode(outPath, code)
}

func extractKeywords(ctx context.Context, db *sql.DB, reservedOnly bool) ([]string, error) {
	query := "SELECT keyword_name FROM duckdb_keywords()"
	if reservedOnly {
		query += " WHERE keyword_category = 'reserved'"
	}
	query += " ORDER BY keyword_name"

	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("query keywords: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var keywords []string
	for rows.Next() {
		var kw string
		if err := rows.Scan(&kw); err != nil {
			return nil, fmt.Errorf("scan keyword: %w", err)
		}
		keywords = append(keywords, strings.ToLower(kw))
	}
	return keywords, rows.Err()
}

func generateKeywordsCode(version string, reserved, all []string) string {
	var buf bytes.Buffer
	writeHeader(&buf, version)

	buf.WriteString("// ReservedKeywords contains keywords marked as 'reserved' by DuckDB.\n")
	buf.WriteString("// These cannot be used as unquoted identifiers.\n")
	buf.WriteString("// Source: SELECT keyword_name FROM duckdb_keywords() WHERE keyword_category = 'reserved'\n")
	buf.WriteString("var ReservedKeywords = map[string]bool{\n")
	for _, kw := range reserved {
		fmt.Fprintf(&buf, "\t%q: true,\n", kw)
	}
	buf.WriteString("}\n\n")

	buf.WriteString("// AllKeywords contains every keyword DuckDB recognizes.\n")
	buf.WriteString("// Identifiers matching these should be quoted.\n")
	buf.WriteString("// Source: SELECT keyword_name FROM duckdb_keywords()\n")
	buf.WriteString("var AllKeywords = map[string]bool{\n")
	for _, kw := range all {
		fmt.Fprintf(&buf, "\t%q: true,\n", kw)
	}
	buf.WriteString("}\n")

	return buf.String()
}

// === Types ===

func generateTypesFile(ctx context.Context, db *sql.DB, version, outPath string) {
	types, err := extractTypes(ctx, db)
	if err != nil {
		log.Fatalf("failed to extract types: %v", err)
	}
	log.Printf("Extracted %d data types", len(types))

	code := generateTypesCode(version, types)
	writeFormattedCode(outPath, code)
}

func extractTypes(ctx context.Context, db *sql.DB) ([]string, error) {
	query := `SELECT DISTINCT type_name FROM duckdb_types() WHERE type_category NOT IN ('INVALID') ORDER BY type_name`

	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("query types: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var types []string
	for rows.Next() {
		var t string
		if err := rows.Scan(&t); err != nil {
			return nil, fmt.Errorf("scan type: %w", err)
		}
		types = append(types, strings.ToUpper(t))
	}
	return types, rows.Err()
}

func generateTypesCode(version string, types []string) string {
	var buf bytes.Buffer
	writeHeader(&buf, version)

	buf.WriteString("// TypeNames contains all recognized DuckDB type names (UPPERCASE).\n")
	buf.WriteString("// Source: SELECT DISTINCT type_name FROM duckdb_types()\n")
	buf.WriteString("var TypeNames = map[string]bool{\n")
	for _, t := range types {
		fmt.Fprintf(&buf, "\t%q: true,\n", t)
	}
	buf.WriteString("}\n")

	return buf.String()
}

// === Functions ===

type functionInfo struct {
	Name           string
	FunctionType   string
	Parameters     []string
	ParameterTypes []string
	ReturnType     string
	Description    string
}

type functionDoc struct {
	Description string
	Signatures  []string
	ReturnType  string
}

func generateFunctionsFile(ctx context.Context, db *sql.DB, version, outPath string) {
	functions, err := extractFunctions(ctx, db)
	if err != nil {
		log.Fatalf("failed to extract functions: %v", err)
	}
	log.Printf("Extracted %d functions", len(functions))

	functions = filterFunctions(functions)
	log.Printf("After filtering: %d functions", len(functions))

	aggregates, tableFuncs, docs := classifyFunctions(functions)
	log.Printf("Classification: %d aggregates, %d table functions, %d docs", len(aggregates), len(tableFuncs), len(docs))

	code := generateFunctionsCode(version, aggregates, tableFuncs, docs)
	writeFormattedCode(outPath, code)
}

func extractFunctions(ctx context.Context, db *sql.DB) ([]functionInfo, error) {
	query := `
		SELECT
			function_name,
			function_type,
			COALESCE(list_transform(parameters, x -> COALESCE(x, ''))::VARCHAR, ''),
			COALESCE(list_transform(parameter_types, x -> COALESCE(x, ''))::VARCHAR, ''),
			COALESCE(return_type, ''),
			COALESCE(description, '')
		FROM duckdb_functions()
		WHERE schema_name = 'main' OR schema_name IS NULL
		ORDER BY function_name, function_type
	`

	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("query functions: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var functions []functionInfo
	seen := make(map[string]bool)

	for rows.Next() {
		var fi functionInfo
		var params, paramTypes, returnType, desc string

		if err := rows.Scan(&fi.Name, &fi.FunctionType, &params, &paramTypes, &returnType, &desc); err != nil {
			return nil, fmt.Errorf("scan row: %w", err)
		}

		key := fi.Name + "|" + fi.FunctionType
		if seen[key] {
			continue
		}
		seen[key] = true

		fi.Parameters = parseArrayString(params)
		fi.ParameterTypes = parseArrayString(paramTypes)
		fi.ReturnType = returnType
		fi.Description = desc

		functions = append(functions, fi)
	}

	return functions, rows.Err()
}

func filterFunctions(functions []functionInfo) []functionInfo {
	symbolicOp := regexp.MustCompile(`^[!@#$%^&*+\-=<>|/~]+$`)

	result := make([]functionInfo, 0, len(functions))
	for _, fi := range functions {
		if shouldSkipFunction(fi.Name, symbolicOp) {
			continue
		}
		result = append(result, fi)
	}
	return result
}

func shouldSkipFunction(name string, symbolicOp *regexp.Regexp) bool {
	if strings.HasPrefix(name, "__internal_") {
		return true
	}
	if strings.HasSuffix(name, "__postfix") {
		return true
	}
	if name == "~~" || name == "~~*" || name == "!~~" || name == "!~~*" || name == "~~~" {
		return true
	}
	return symbolicOp.MatchString(name)
}

func parseArrayString(s string) []string {
	s = strings.TrimSpace(s)
	s = strings.TrimPrefix(s, "[")
	s = strings.TrimSuffix(s, "]")

	if s == "" {
		return nil
	}

	parts := strings.Split(s, ", ")
	result := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			result = append(result, p)
		}
	}
	return result
}

func classifyFunctions(functions []functionInfo) (aggregates, tableFuncs []string, docs map[string]functionDoc) {
	docs = make(map[string]functionDoc)
	aggregateSet := make(map[string]bool)
	tableSet := make(map[string]bool)

	for _, fi := range functions {
		name := strings.ToLower(fi.Name)
		sig := buildSignature(fi)

		if existing, ok := docs[name]; ok {
			hasSig := false
			for _, s := range existing.Signatures {
				if s == sig {
					hasSig = true
					break
				}
			}
			if !hasSig {
				existing.Signatures = append(existing.Signatures, sig)
				docs[name] = existing
			}
		} else {
			docs[name] = functionDoc{
				Description: fi.Description,
				Signatures:  []string{sig},
				ReturnType:  strings.ToUpper(fi.ReturnType),
			}
		}

		switch fi.FunctionType {
		case "aggregate":
			aggregateSet[name] = true
		case "table":
			tableSet[name] = true
		}
	}

	aggregates = mapToSortedSlice(aggregateSet)
	tableFuncs = mapToSortedSlice(tableSet)
	return
}

func buildSignature(fi functionInfo) string {
	var buf bytes.Buffer
	buf.WriteString(strings.ToLower(fi.Name))
	buf.WriteByte('(')

	for i, param := range fi.Parameters {
		if i > 0 {
			buf.WriteString(", ")
		}
		buf.WriteString(param)
		if i < len(fi.ParameterTypes) {
			buf.WriteByte(' ')
			buf.WriteString(strings.ToUpper(fi.ParameterTypes[i]))
		}
	}

	buf.WriteString(") -> ")
	buf.WriteString(strings.ToUpper(fi.ReturnType))
	return buf.String()
}

func mapToSortedSlice(m map[string]bool) []string {
	result := make([]string, 0, len(m))
	for k := range m {
		result = append(result, k)
	}
	sort.Strings(result)
	return result
}

func generateFunctionsCode(version string, aggregates, tableFuncs []string, docs map[string]functionDoc) string {
	var buf bytes.Buffer
	writeHeader(&buf, version)

	// FunctionDoc type
	buf.WriteString("// FunctionDoc holds documentation for a DuckDB function.\n")
	buf.WriteString("type FunctionDoc struct {\n")
	buf.WriteString("\tDescription string\n")
	buf.WriteString("\tSignatures  []string\n")
	buf.WriteString("\tReturnType  string\n")
	buf.WriteString("}\n\n")

	// Aggregates
	buf.WriteString("// AggregateFunctions contains all aggregate function names (lowercase).\n")
	buf.WriteString("var AggregateFunctions = map[string]bool{\n")
	for _, name := range aggregates {
		fmt.Fprintf(&buf, "\t%q: true,\n", name)
	}
	buf.WriteString("}\n\n")

	// Table functions
	buf.WriteString("// TableFunctions contains all table-valued function names (lowercase).\n")
	buf.WriteString("var TableFunctions = map[string]bool{\n")
	for _, name := range tableFuncs {
		fmt.Fprintf(&buf, "\t%q: true,\n", name)
	}
	buf.WriteString("}\n\n")

	// Window functions (manually maintained — DuckDB doesn't expose these via duckdb_functions())
	buf.WriteString("// WindowFunctions contains window function names (lowercase).\n")
	buf.WriteString("// DuckDB doesn't expose these via duckdb_functions(), so maintained manually.\n")
	buf.WriteString("var WindowFunctions = map[string]bool{\n")
	windowFuncs := []string{
		"cume_dist", "dense_rank", "first_value", "lag", "last_value",
		"lead", "nth_value", "ntile", "percent_rank", "rank", "row_number",
	}
	for _, name := range windowFuncs {
		fmt.Fprintf(&buf, "\t%q: true,\n", name)
	}
	buf.WriteString("}\n\n")

	// Function docs
	docNames := make([]string, 0, len(docs))
	for name := range docs {
		docNames = append(docNames, name)
	}
	sort.Strings(docNames)

	buf.WriteString("// FunctionDocs maps function name (lowercase) to its documentation.\n")
	buf.WriteString("var FunctionDocs = map[string]FunctionDoc{\n")
	for _, name := range docNames {
		doc := docs[name]
		fmt.Fprintf(&buf, "\t%q: {\n", name)
		if doc.Description != "" {
			fmt.Fprintf(&buf, "\t\tDescription: %q,\n", doc.Description)
		}
		if len(doc.Signatures) > 0 {
			buf.WriteString("\t\tSignatures: []string{\n")
			for _, sig := range doc.Signatures {
				fmt.Fprintf(&buf, "\t\t\t%q,\n", sig)
			}
			buf.WriteString("\t\t},\n")
		}
		if doc.ReturnType != "" {
			fmt.Fprintf(&buf, "\t\tReturnType: %q,\n", doc.ReturnType)
		}
		buf.WriteString("\t},\n")
	}
	buf.WriteString("}\n")

	return buf.String()
}

// === Helpers ===

func writeHeader(buf *bytes.Buffer, version string) {
	buf.WriteString("// Code generated by scripts/genduckdb. DO NOT EDIT.\n")
	fmt.Fprintf(buf, "// Source: DuckDB %s\n", version)
	fmt.Fprintf(buf, "// Generated: %s\n\n", time.Now().Format("2006-01-02"))
	buf.WriteString("package catalog\n\n")
}

func writeFormattedCode(outPath, code string) {
	formatted, err := format.Source([]byte(code))
	if err != nil {
		log.Printf("Warning: failed to format generated code: %v", err)
		formatted = []byte(code)
	}

	if err := os.WriteFile(outPath, formatted, 0o644); err != nil {
		log.Fatalf("failed to write output: %v", err)
	}
	log.Printf("Generated %s (%d bytes)", outPath, len(formatted))
}
