package architecture_test

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"duck-demo/pkg/astdb"

	_ "github.com/duckdb/duckdb-go/v2"
	"github.com/stretchr/testify/require"
)

type importEdge struct {
	FilePath   string
	ImportPath string
}

type syntheticRule struct {
	SourcePrefix string
	ImportPrefix string
}

func TestQuerySessionAmortization(t *testing.T) {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "query-amortization.duckdb")

	opts := astdb.DefaultOptions()
	opts.RepoRoot = repoRootDir()
	opts.Subdir = "internal"
	opts.MaxFiles = 250
	opts.Mode = "build"
	opts.QueryBench = false
	opts.DuckDBPath = dbPath
	opts.KeepOutputFiles = true
	opts.Reuse = true
	opts.ForceRebuild = true

	_, err := astdb.Run(t.Context(), opts)
	require.NoError(t, err)

	queries := []string{
		`SELECT COUNT(*) FROM nodes WHERE kind = '*ast.ImportSpec'`,
		`SELECT kind, COUNT(*) AS n FROM nodes GROUP BY kind ORDER BY n DESC LIMIT 20`,
		`SELECT f.path, COUNT(*) AS n FROM nodes n JOIN files f ON f.file_id = n.file_id WHERE n.kind = '*ast.FuncDecl' GROUP BY f.path ORDER BY n DESC LIMIT 50`,
		`SELECT f.path, COUNT(*) AS n FROM nodes n JOIN files f ON f.file_id = n.file_id GROUP BY f.path ORDER BY n DESC LIMIT 50`,
	}

	const iters = 20

	perQueryStart := time.Now()
	perQueryTotals, err := runQueriesOpenPerQuery(dbPath, queries, iters)
	require.NoError(t, err)
	perQueryElapsed := time.Since(perQueryStart)

	sessionStart := time.Now()
	sessionTotals, err := runQueriesSingleSession(dbPath, queries, iters)
	require.NoError(t, err)
	sessionElapsed := time.Since(sessionStart)

	require.Equal(t, perQueryTotals, sessionTotals, "query totals must match across execution modes")

	t.Logf("query session amortization: per_query_open=%s single_session=%s speedup=%.2fx iters=%d queries=%d", perQueryElapsed, sessionElapsed, float64(perQueryElapsed)/float64(sessionElapsed), iters, len(queries))
}

func TestScalingMockup_ParserVsDuckDB(t *testing.T) {
	t.Helper()

	const maxFiles = 250
	parserEdges := collectParserImportEdges(t, maxFiles)
	rules := syntheticRulesForScaling()
	const repeats = 20

	parserSingleStart := time.Now()
	parserSingle := countParserRule(parserEdges, rules[0])
	parserSingleElapsed := time.Since(parserSingleStart)

	parserMultiStart := time.Now()
	parserMulti := runParserSyntheticRules(parserEdges, rules, repeats)
	parserMultiElapsed := time.Since(parserMultiStart)

	dbPath := filepath.Join(t.TempDir(), "scaling-mockup.duckdb")
	opts := astdb.DefaultOptions()
	opts.RepoRoot = repoRootDir()
	opts.Subdir = "internal"
	opts.MaxFiles = maxFiles
	opts.Mode = "build"
	opts.QueryBench = false
	opts.DuckDBPath = dbPath
	opts.KeepOutputFiles = true
	opts.Reuse = true
	opts.ForceRebuild = true

	buildStart := time.Now()
	_, err := astdb.Run(t.Context(), opts)
	require.NoError(t, err)
	buildElapsed := time.Since(buildStart)

	db, err := sql.Open("duckdb", dbPath)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	_, err = db.ExecContext(context.Background(), `
CREATE TEMP TABLE import_edges AS
SELECT f.path AS file_path,
       replace(coalesce(n.node_text, ''), '"', '') AS import_path
FROM nodes n
JOIN files f ON f.file_id = n.file_id
WHERE n.kind = '*ast.ImportSpec'
`)
	require.NoError(t, err)

	duckSingleStart := time.Now()
	duckSingle, err := countDuckRule(db, rules[0])
	require.NoError(t, err)
	duckSingleElapsed := time.Since(duckSingleStart)

	duckMultiStart := time.Now()
	duckMulti, err := runDuckSyntheticRules(db, rules, repeats)
	require.NoError(t, err)
	duckMultiElapsed := time.Since(duckMultiStart)

	require.Equal(t, parserSingle, duckSingle, "single rule count mismatch")
	require.Equal(t, parserMulti, duckMulti, "multi-rule count mismatch")

	parserScale := float64(parserMultiElapsed) / float64(parserSingleElapsed)
	duckScale := float64(duckMultiElapsed) / float64(duckSingleElapsed)

	t.Logf(
		"scaling mockup: rules=%d repeats=%d edges=%d build=%s parser_single=%s parser_n=%s parser_scale=%.2fx duck_single=%s duck_n=%s duck_scale=%.2fx",
		len(rules),
		repeats,
		len(parserEdges),
		buildElapsed,
		parserSingleElapsed,
		parserMultiElapsed,
		parserScale,
		duckSingleElapsed,
		duckMultiElapsed,
		duckScale,
	)
}

func TestImportBoundaries_QueryParityAndTiming(t *testing.T) {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "import-boundaries.duckdb")

	parserStart := time.Now()
	parserViolations := collectImportBoundaryViolationsFromParser(t)
	parserElapsed := time.Since(parserStart)

	buildAndQueryStart := time.Now()
	queryViolations := collectImportBoundaryViolationsFromQuery(t, dbPath, true)
	buildAndQueryElapsed := time.Since(buildAndQueryStart)

	queryOnlyStart := time.Now()
	reusedQueryViolations := collectImportBoundaryViolationsFromQuery(t, dbPath, false)
	queryOnlyElapsed := time.Since(queryOnlyStart)

	sort.Strings(parserViolations)
	sort.Strings(queryViolations)
	sort.Strings(reusedQueryViolations)

	require.Equal(t, parserViolations, queryViolations,
		"query-based import boundary check must match parser-based check")
	require.Equal(t, parserViolations, reusedQueryViolations,
		"reused query-based import boundary check must match parser-based check")

	t.Logf("import-boundary timing: parser=%s build_plus_query=%s query_reuse=%s parser_violations=%d", parserElapsed, buildAndQueryElapsed, queryOnlyElapsed, len(parserViolations))
}

func collectImportBoundaryViolationsFromParser(t *testing.T) []string {
	t.Helper()

	files, err := collectGoFiles(internalRootDir())
	require.NoError(t, err)

	violations := make([]string, 0)
	for _, file := range files {
		if shouldSkipProductionGovernanceFile(file) {
			continue
		}

		sourcePkg := packageImportPath(file)
		rule, ok := findRule(sourcePkg)
		if !ok {
			continue
		}

		for _, importPath := range parseImports(t, file) {
			if !strings.HasPrefix(importPath, modulePath+"/") {
				continue
			}
			if !violatesRule(importPath, rule.forbidden) {
				continue
			}

			violations = append(violations,
				fmt.Sprintf("governance: %s imports %s via %s; allowed direction: %s", sourcePkg, importPath, file, rule.hint),
			)
		}
	}

	return violations
}

func collectImportBoundaryViolationsFromQuery(t *testing.T, duckPath string, forceRebuild bool) []string {
	t.Helper()

	opts := astdb.DefaultOptions()
	opts.RepoRoot = repoRootDir()
	opts.Subdir = "internal"
	opts.Mode = "build"
	opts.QueryBench = false
	opts.DuckDBPath = duckPath
	opts.KeepOutputFiles = true
	opts.Reuse = true
	opts.ForceRebuild = forceRebuild

	_, err := astdb.Run(t.Context(), opts)
	require.NoError(t, err)

	db, err := sql.Open("duckdb", duckPath)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	rows, err := db.QueryContext(context.Background(), `
SELECT f.path, n.node_text
FROM nodes n
JOIN files f ON f.file_id = n.file_id
WHERE n.kind = '*ast.ImportSpec'
`)
	require.NoError(t, err)
	defer func() { _ = rows.Close() }()

	violations := make([]string, 0)
	for rows.Next() {
		var relPath string
		var nodeText sql.NullString
		require.NoError(t, rows.Scan(&relPath, &nodeText))

		absPath := filepath.Join(repoRootDir(), filepath.FromSlash(relPath))
		if shouldSkipProductionGovernanceFile(absPath) {
			continue
		}

		importPath := strings.Trim(nodeText.String, "\"")
		if !strings.HasPrefix(importPath, modulePath+"/") {
			continue
		}

		sourcePkg := packageImportPath(absPath)
		rule, ok := findRule(sourcePkg)
		if !ok {
			continue
		}
		if !violatesRule(importPath, rule.forbidden) {
			continue
		}

		violations = append(violations,
			fmt.Sprintf("governance: %s imports %s via %s; allowed direction: %s", sourcePkg, importPath, absPath, rule.hint),
		)
	}
	require.NoError(t, rows.Err())

	return violations
}

func runQueriesOpenPerQuery(duckPath string, queries []string, iters int) ([]int64, error) {
	totals := make([]int64, len(queries))
	for i := 0; i < iters; i++ {
		for qi, q := range queries {
			db, err := sql.Open("duckdb", duckPath)
			if err != nil {
				return nil, err
			}

			count, err := executeAndCountRows(db, q)
			_ = db.Close()
			if err != nil {
				return nil, err
			}
			totals[qi] += int64(count)
		}
	}
	return totals, nil
}

func runQueriesSingleSession(duckPath string, queries []string, iters int) ([]int64, error) {
	db, err := sql.Open("duckdb", duckPath)
	if err != nil {
		return nil, err
	}
	defer func() { _ = db.Close() }()

	totals := make([]int64, len(queries))
	for i := 0; i < iters; i++ {
		for qi, q := range queries {
			count, err := executeAndCountRows(db, q)
			if err != nil {
				return nil, err
			}
			totals[qi] += int64(count)
		}
	}
	return totals, nil
}

func executeAndCountRows(db *sql.DB, query string) (int, error) {
	rows, err := db.QueryContext(context.Background(), query)
	if err != nil {
		return 0, err
	}
	defer func() { _ = rows.Close() }()

	cols, err := rows.Columns()
	if err != nil {
		return 0, err
	}
	vals := make([]any, len(cols))
	ptrs := make([]any, len(cols))
	for i := range vals {
		ptrs[i] = &vals[i]
	}

	count := 0
	for rows.Next() {
		if err := rows.Scan(ptrs...); err != nil {
			return 0, err
		}
		count++
	}

	return count, rows.Err()
}

func collectParserImportEdges(t *testing.T, maxFiles int) []importEdge {
	t.Helper()

	files, err := collectGoFiles(internalRootDir())
	require.NoError(t, err)

	sort.Slice(files, func(i, j int) bool {
		return relToRepoRoot(files[i]) < relToRepoRoot(files[j])
	})

	if maxFiles > 0 && len(files) > maxFiles {
		files = files[:maxFiles]
	}

	edges := make([]importEdge, 0, 4096)
	for _, file := range files {
		rel := relToRepoRoot(file)
		for _, imp := range parseImports(t, file) {
			edges = append(edges, importEdge{FilePath: rel, ImportPath: imp})
		}
	}

	return edges
}

func syntheticRulesForScaling() []syntheticRule {
	sourcePrefixes := []string{
		"internal/domain/",
		"internal/service/",
		"internal/api/",
		"internal/db/",
		"internal/engine/",
		"internal/middleware/",
		"internal/declarative/",
		"internal/",
	}

	importPrefixes := []string{
		"duck-demo/internal/domain",
		"duck-demo/internal/service",
		"duck-demo/internal/api",
		"duck-demo/internal/db",
		"duck-demo/internal/engine",
		"duck-demo/internal/middleware",
		"duck-demo/internal/declarative",
		"duck-demo/pkg/",
		"context",
		"database/sql",
		"fmt",
		"strings",
		"github.com/",
	}

	rules := make([]syntheticRule, 0, len(sourcePrefixes)*len(importPrefixes))
	for _, src := range sourcePrefixes {
		for _, imp := range importPrefixes {
			rules = append(rules, syntheticRule{SourcePrefix: src, ImportPrefix: imp})
		}
	}

	return rules
}

func countParserRule(edges []importEdge, rule syntheticRule) int64 {
	var total int64
	for _, e := range edges {
		if strings.HasPrefix(e.FilePath, rule.SourcePrefix) && strings.HasPrefix(e.ImportPath, rule.ImportPrefix) {
			total++
		}
	}
	return total
}

func runParserSyntheticRules(edges []importEdge, rules []syntheticRule, repeats int) int64 {
	var total int64
	for i := 0; i < repeats; i++ {
		for _, rule := range rules {
			total += countParserRule(edges, rule)
		}
	}
	return total
}

func countDuckRule(db *sql.DB, rule syntheticRule) (int64, error) {
	var count int64
	err := db.QueryRowContext(context.Background(),
		`SELECT COUNT(*) FROM import_edges WHERE file_path LIKE ? AND import_path LIKE ?`,
		rule.SourcePrefix+"%",
		rule.ImportPrefix+"%",
	).Scan(&count)
	if err != nil {
		return 0, err
	}
	return count, nil
}

func runDuckSyntheticRules(db *sql.DB, rules []syntheticRule, repeats int) (int64, error) {
	stmt, err := db.PrepareContext(context.Background(), `SELECT COUNT(*) FROM import_edges WHERE file_path LIKE ? AND import_path LIKE ?`)
	if err != nil {
		return 0, err
	}
	defer func() { _ = stmt.Close() }()

	var total int64
	for i := 0; i < repeats; i++ {
		for _, rule := range rules {
			var count int64
			if err := stmt.QueryRowContext(context.Background(), rule.SourcePrefix+"%", rule.ImportPrefix+"%").Scan(&count); err != nil {
				return 0, err
			}
			total += count
		}
	}

	return total, nil
}
