package astdb

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"hash/fnv"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	duckdb "github.com/duckdb/duckdb-go/v2"
)

const schemaVersion = "2"

type Options struct {
	RepoRoot        string
	Subdir          string
	MaxFiles        int
	Workers         int
	DuckDBPath      string
	Mode            string
	Reuse           bool
	ForceRebuild    bool
	CreateIndexes   bool
	QueryBench      bool
	QueryWarmup     int
	QueryIters      int
	KeepOutputFiles bool
}

func DefaultOptions() Options {
	return Options{
		RepoRoot:        ".",
		MaxFiles:        0,
		Workers:         runtime.NumCPU(),
		DuckDBPath:      "./.tmp/astbench/ast.duckdb",
		Mode:            "both",
		Reuse:           true,
		ForceRebuild:    false,
		CreateIndexes:   false,
		QueryBench:      true,
		QueryWarmup:     3,
		QueryIters:      15,
		KeepOutputFiles: true,
	}
}

type Result struct {
	ScanFiles    int
	ScanElapsed  time.Duration
	Subdir       string
	MaxFiles     int
	Sync         SyncStats
	QueryWarmup  int
	QueryIters   int
	QueryResults []QueryResult
}

type SyncStats struct {
	Action       string
	Reason       string
	ParseElapsed time.Duration
	LoadElapsed  time.Duration
	Changed      int
	Deleted      int
	Unchanged    int
	ParseErrors  int
	FilesCount   int64
	NodesCount   int64
	Indexes      bool
}

type QueryResult struct {
	Name    string
	Elapsed time.Duration
}

type fileMeta struct {
	RelPath     string
	Size        int64
	ModUnixNano int64
}

type trackedFile struct {
	FileID      int64
	Size        int64
	ModUnixNano int64
}

type fileRow struct {
	ID         int64
	Path       string
	PkgName    string
	ParseError string
	Bytes      int64
}

type nodeRow struct {
	FileID        int64
	Ordinal       int
	ParentOrdinal int
	HasParent     bool
	Kind          string
	Pos           int
	End           int
	StartLine     int
	StartCol      int
	EndLine       int
	EndCol        int
	StartOffset   int
	EndOffset     int
}

type parseResult struct {
	File fileRow
	Meta fileMeta
	Rows []nodeRow
}

type dbState struct {
	Exists            bool
	SchemaVersion     string
	IndexesEnabled    bool
	FilesCount        int64
	NodesCount        int64
	TrackedFiles      map[string]trackedFile
	SourceFingerprint string
}

type syncInput struct {
	repoRoot      string
	dbPath        string
	mode          string
	reuse         bool
	forceRebuild  bool
	createIndexes bool
	workers       int
	metas         []fileMeta
	state         dbState
}

type trackedDeletion struct {
	Path   string
	FileID int64
}

func Run(ctx context.Context, opts Options) (Result, error) {
	_ = ctx

	if opts.Workers <= 0 {
		opts.Workers = 1
	}
	mode := strings.ToLower(strings.TrimSpace(opts.Mode))
	if mode == "" {
		mode = "both"
	}
	if mode != "both" && mode != "build" && mode != "query" {
		return Result{}, fmt.Errorf("invalid mode %q (expected both|build|query)", opts.Mode)
	}

	repoRoot, err := filepath.Abs(opts.RepoRoot)
	if err != nil {
		return Result{}, fmt.Errorf("resolve repo root: %w", err)
	}
	dbPath, err := filepath.Abs(opts.DuckDBPath)
	if err != nil {
		return Result{}, fmt.Errorf("resolve duckdb path: %w", err)
	}
	if err := os.MkdirAll(filepath.Dir(dbPath), 0o755); err != nil {
		return Result{}, fmt.Errorf("create duckdb parent directory: %w", err)
	}

	scanStart := time.Now()
	metas, err := collectGoFiles(repoRoot, opts.Subdir, opts.MaxFiles)
	if err != nil {
		return Result{}, fmt.Errorf("collect go files: %w", err)
	}
	if len(metas) == 0 {
		return Result{}, errors.New("no .go files found")
	}
	scanElapsed := time.Since(scanStart)

	state, err := inspectDuckDB(dbPath)
	if err != nil {
		return Result{}, fmt.Errorf("inspect duckdb: %w", err)
	}

	syncStats, err := syncDatabase(syncInput{
		repoRoot:      repoRoot,
		dbPath:        dbPath,
		mode:          mode,
		reuse:         opts.Reuse,
		forceRebuild:  opts.ForceRebuild,
		createIndexes: opts.CreateIndexes,
		workers:       opts.Workers,
		metas:         metas,
		state:         state,
	})
	if err != nil {
		return Result{}, err
	}

	res := Result{
		ScanFiles:   len(metas),
		ScanElapsed: scanElapsed,
		Subdir:      opts.Subdir,
		MaxFiles:    opts.MaxFiles,
		Sync:        syncStats,
	}

	if opts.QueryBench && (mode == "both" || mode == "query") {
		queries := defaultQuerySet()
		qResults, err := benchmarkQueries(dbPath, queries, opts.QueryWarmup, opts.QueryIters)
		if err != nil {
			return Result{}, fmt.Errorf("benchmark queries: %w", err)
		}
		res.QueryWarmup = opts.QueryWarmup
		res.QueryIters = max(1, opts.QueryIters)
		res.QueryResults = qResults
	}

	if !opts.KeepOutputFiles {
		cleanupDuckDB(dbPath)
	}

	return res, nil
}

func syncDatabase(in syncInput) (SyncStats, error) {
	stats := SyncStats{}

	fullRebuild, reason := shouldFullRebuild(in.mode, in.reuse, in.forceRebuild, in.state)
	stats.Reason = reason

	currentFingerprint := sourceFingerprint(in.metas)

	changed := make([]fileMeta, 0, len(in.metas))
	deleted := make([]trackedDeletion, 0)
	unchanged := 0

	if fullRebuild {
		changed = append(changed, in.metas...)
		if in.state.Exists {
			for path, t := range in.state.TrackedFiles {
				deleted = append(deleted, trackedDeletion{Path: path, FileID: t.FileID})
			}
		}
	} else {
		seen := make(map[string]struct{}, len(in.metas))
		for _, meta := range in.metas {
			seen[meta.RelPath] = struct{}{}
			prev, ok := in.state.TrackedFiles[meta.RelPath]
			if !ok || prev.Size != meta.Size || prev.ModUnixNano != meta.ModUnixNano {
				changed = append(changed, meta)
				continue
			}
			unchanged++
		}
		for path, prev := range in.state.TrackedFiles {
			if _, ok := seen[path]; !ok {
				deleted = append(deleted, trackedDeletion{Path: path, FileID: prev.FileID})
			}
		}
	}

	stats.Changed = len(changed)
	stats.Deleted = len(deleted)
	stats.Unchanged = unchanged

	if !fullRebuild && len(changed) == 0 && len(deleted) == 0 {
		if in.createIndexes && !in.state.IndexesEnabled {
			start := time.Now()
			if err := addIndexesInPlace(in.dbPath, currentFingerprint); err != nil {
				return SyncStats{}, err
			}
			stats.LoadElapsed = time.Since(start)
			stats.Action = "index-only"
			stats.Reason = "database up-to-date, added indexes"
			stats.Indexes = true
		} else {
			stats.Action = "reuse"
			stats.Reason = "database up-to-date"
			stats.Indexes = in.state.IndexesEnabled
		}
		stats.FilesCount = in.state.FilesCount
		stats.NodesCount = in.state.NodesCount
		return stats, nil
	}

	if fullRebuild {
		cleanupDuckDB(in.dbPath)
		stats.Action = "rebuild"
	} else {
		stats.Action = "incremental"
	}

	startLoad := time.Now()
	db, err := sql.Open("duckdb", in.dbPath)
	if err != nil {
		return SyncStats{}, fmt.Errorf("open duckdb: %w", err)
	}
	defer func() { _ = db.Close() }()

	ctx := context.Background()
	conn, err := db.Conn(ctx)
	if err != nil {
		return SyncStats{}, fmt.Errorf("open duckdb conn: %w", err)
	}
	defer func() { _ = conn.Close() }()

	if _, err := conn.ExecContext(ctx, fmt.Sprintf("PRAGMA threads=%d", runtime.NumCPU())); err != nil {
		return SyncStats{}, fmt.Errorf("set duckdb threads: %w", err)
	}

	if err := createSchema(ctx, conn); err != nil {
		return SyncStats{}, fmt.Errorf("create schema: %w", err)
	}

	parseStart := time.Now()
	parseErrors, err := applyIncremental(ctx, conn, in.repoRoot, changed, deleted, in.workers)
	if err != nil {
		return SyncStats{}, err
	}
	stats.ParseElapsed = time.Since(parseStart)
	stats.ParseErrors = parseErrors

	indexesEnabled := in.state.IndexesEnabled
	if in.createIndexes && !indexesEnabled {
		if err := createDBIndexes(ctx, conn); err != nil {
			return SyncStats{}, fmt.Errorf("create indexes: %w", err)
		}
		indexesEnabled = true
	}
	stats.Indexes = indexesEnabled

	if err := writeRunMeta(ctx, conn, currentFingerprint, indexesEnabled); err != nil {
		return SyncStats{}, fmt.Errorf("write run meta: %w", err)
	}

	filesCount, nodesCount, err := fetchCounts(db)
	if err != nil {
		return SyncStats{}, fmt.Errorf("count rows: %w", err)
	}
	stats.FilesCount = filesCount
	stats.NodesCount = nodesCount
	stats.LoadElapsed = time.Since(startLoad)

	return stats, nil
}

func shouldFullRebuild(mode string, reuse, force bool, state dbState) (bool, string) {
	if force {
		return true, "force rebuild enabled"
	}
	if !state.Exists {
		if mode == "query" {
			return true, "query mode but database missing"
		}
		return true, "database missing"
	}
	if state.SchemaVersion != schemaVersion {
		return true, "schema version changed"
	}
	if !reuse {
		return true, "reuse disabled"
	}
	return false, "incremental check"
}

func applyIncremental(ctx context.Context, conn *sql.Conn, repoRoot string, changed []fileMeta, deleted []trackedDeletion, workers int) (int, error) {
	if _, err := conn.ExecContext(ctx, `BEGIN TRANSACTION`); err != nil {
		return 0, fmt.Errorf("begin transaction: %w", err)
	}

	rollback := func(cause error) (int, error) {
		_, _ = conn.ExecContext(ctx, `ROLLBACK`)
		return 0, cause
	}

	deleteNodesStmt, err := conn.PrepareContext(ctx, `DELETE FROM nodes WHERE file_id = ?`)
	if err != nil {
		return rollback(fmt.Errorf("prepare delete nodes: %w", err))
	}
	defer func() { _ = deleteNodesStmt.Close() }()

	deleteFilesStmt, err := conn.PrepareContext(ctx, `DELETE FROM files WHERE file_id = ?`)
	if err != nil {
		return rollback(fmt.Errorf("prepare delete files: %w", err))
	}
	defer func() { _ = deleteFilesStmt.Close() }()

	deleteStateStmt, err := conn.PrepareContext(ctx, `DELETE FROM file_state WHERE path = ?`)
	if err != nil {
		return rollback(fmt.Errorf("prepare delete file_state: %w", err))
	}
	defer func() { _ = deleteStateStmt.Close() }()

	for _, d := range deleted {
		if _, err := deleteNodesStmt.ExecContext(ctx, d.FileID); err != nil {
			return rollback(fmt.Errorf("delete nodes for %q: %w", d.Path, err))
		}
		if _, err := deleteFilesStmt.ExecContext(ctx, d.FileID); err != nil {
			return rollback(fmt.Errorf("delete file for %q: %w", d.Path, err))
		}
		if _, err := deleteStateStmt.ExecContext(ctx, d.Path); err != nil {
			return rollback(fmt.Errorf("delete state for %q: %w", d.Path, err))
		}
	}

	for _, ch := range changed {
		fileID := fileIDForPath(ch.RelPath)
		if _, err := deleteNodesStmt.ExecContext(ctx, fileID); err != nil {
			return rollback(fmt.Errorf("clear old nodes for %q: %w", ch.RelPath, err))
		}
		if _, err := deleteFilesStmt.ExecContext(ctx, fileID); err != nil {
			return rollback(fmt.Errorf("clear old file row for %q: %w", ch.RelPath, err))
		}
	}

	parseErrCount := 0
	err = conn.Raw(func(raw any) error {
		driverConn, ok := raw.(driver.Conn)
		if !ok {
			return fmt.Errorf("unexpected raw conn type %T", raw)
		}

		filesAppender, err := duckdb.NewAppenderFromConn(driverConn, "", "files")
		if err != nil {
			return fmt.Errorf("create files appender: %w", err)
		}
		defer func() { _ = filesAppender.Close() }()

		nodesAppender, err := duckdb.NewAppenderFromConn(driverConn, "", "nodes")
		if err != nil {
			return fmt.Errorf("create nodes appender: %w", err)
		}
		defer func() { _ = nodesAppender.Close() }()

		stateAppender, err := duckdb.NewAppenderFromConn(driverConn, "", "file_state")
		if err != nil {
			return fmt.Errorf("create file_state appender: %w", err)
		}
		defer func() { _ = stateAppender.Close() }()

		results := startParseWorkers(repoRoot, changed, workers)
		nowUnix := time.Now().Unix()

		for res := range results {
			if res.File.ParseError != "" {
				parseErrCount++
			}

			var parseErr any
			if res.File.ParseError != "" {
				parseErr = res.File.ParseError
			}

			if err := filesAppender.AppendRow(res.File.ID, res.File.Path, res.File.PkgName, parseErr, res.File.Bytes); err != nil {
				return fmt.Errorf("append file row %q: %w", res.File.Path, err)
			}

			for _, row := range res.Rows {
				var parent any
				if row.HasParent {
					parent = row.ParentOrdinal
				}
				if err := nodesAppender.AppendRow(
					row.FileID,
					row.Ordinal,
					parent,
					row.Kind,
					row.Pos,
					row.End,
					row.StartLine,
					row.StartCol,
					row.EndLine,
					row.EndCol,
					row.StartOffset,
					row.EndOffset,
				); err != nil {
					return fmt.Errorf("append node row file=%d ordinal=%d: %w", row.FileID, row.Ordinal, err)
				}
			}

			if err := stateAppender.AppendRow(res.Meta.RelPath, res.File.ID, res.Meta.Size, res.Meta.ModUnixNano, nowUnix); err != nil {
				return fmt.Errorf("append file_state row for %q: %w", res.Meta.RelPath, err)
			}
		}

		return nil
	})
	if err != nil {
		return rollback(err)
	}

	if _, err := conn.ExecContext(ctx, `COMMIT`); err != nil {
		return rollback(fmt.Errorf("commit transaction: %w", err))
	}

	return parseErrCount, nil
}

func startParseWorkers(repoRoot string, files []fileMeta, workers int) <-chan parseResult {
	out := make(chan parseResult, workers)
	jobs := make(chan fileMeta)

	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for meta := range jobs {
				out <- parseFile(repoRoot, meta)
			}
		}()
	}

	go func() {
		for _, meta := range files {
			jobs <- meta
		}
		close(jobs)
		wg.Wait()
		close(out)
	}()

	return out
}

func parseFile(repoRoot string, meta fileMeta) parseResult {
	fileID := fileIDForPath(meta.RelPath)
	absPath := filepath.Join(repoRoot, filepath.FromSlash(meta.RelPath))

	content, readErr := os.ReadFile(absPath)
	if readErr != nil {
		return parseResult{File: fileRow{ID: fileID, Path: meta.RelPath, ParseError: readErr.Error()}, Meta: meta}
	}

	fset := token.NewFileSet()
	parsed, parseErr := parser.ParseFile(fset, absPath, content, parser.ParseComments|parser.AllErrors)

	result := parseResult{
		File: fileRow{ID: fileID, Path: meta.RelPath, Bytes: int64(len(content))},
		Meta: meta,
	}
	if parseErr != nil {
		result.File.ParseError = parseErr.Error()
	}
	if parsed != nil && parsed.Name != nil {
		result.File.PkgName = parsed.Name.Name
	}
	if parsed == nil {
		return result
	}

	result.Rows = walkNodes(fset, fileID, parsed)
	return result
}

func walkNodes(fset *token.FileSet, fileID int64, file *ast.File) []nodeRow {
	rows := make([]nodeRow, 0, 1024)
	stack := make([]int, 0, 256)
	ordinal := 0

	ast.Inspect(file, func(n ast.Node) bool {
		if n == nil {
			if len(stack) > 0 {
				stack = stack[:len(stack)-1]
			}
			return true
		}

		ordinal++
		parentOrdinal := 0
		hasParent := false
		if len(stack) > 0 {
			parentOrdinal = stack[len(stack)-1]
			hasParent = true
		}

		startPos := fset.PositionFor(n.Pos(), false)
		endPos := fset.PositionFor(n.End(), false)

		startOffset := -1
		endOffset := -1
		if n.Pos().IsValid() {
			if tokenFile := fset.File(n.Pos()); tokenFile != nil {
				startOffset = tokenFile.Offset(n.Pos())
				if n.End().IsValid() {
					endOffset = tokenFile.Offset(n.End())
				}
			}
		}

		rows = append(rows, nodeRow{
			FileID:        fileID,
			Ordinal:       ordinal,
			ParentOrdinal: parentOrdinal,
			HasParent:     hasParent,
			Kind:          fmt.Sprintf("%T", n),
			Pos:           int(n.Pos()),
			End:           int(n.End()),
			StartLine:     startPos.Line,
			StartCol:      startPos.Column,
			EndLine:       endPos.Line,
			EndCol:        endPos.Column,
			StartOffset:   startOffset,
			EndOffset:     endOffset,
		})

		stack = append(stack, ordinal)
		return true
	})

	return rows
}

func createSchema(ctx context.Context, conn *sql.Conn) error {
	stmts := []string{
		`CREATE TABLE IF NOT EXISTS files (
			file_id BIGINT PRIMARY KEY,
			path TEXT NOT NULL UNIQUE,
			pkg_name TEXT,
			parse_error TEXT,
			bytes BIGINT
		)`,
		`CREATE TABLE IF NOT EXISTS nodes (
			file_id BIGINT NOT NULL,
			ordinal INTEGER NOT NULL,
			parent_ordinal INTEGER,
			kind TEXT NOT NULL,
			pos INTEGER,
			"end" INTEGER,
			start_line INTEGER,
			start_col INTEGER,
			end_line INTEGER,
			end_col INTEGER,
			start_offset INTEGER,
			end_offset INTEGER,
			PRIMARY KEY(file_id, ordinal)
		)`,
		`CREATE TABLE IF NOT EXISTS file_state (
			path TEXT PRIMARY KEY,
			file_id BIGINT NOT NULL,
			size_bytes BIGINT NOT NULL,
			mtime_ns BIGINT NOT NULL,
			last_seen_unix BIGINT NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS run_meta (
			key TEXT PRIMARY KEY,
			value TEXT NOT NULL
		)`,
	}

	for _, stmt := range stmts {
		if _, err := conn.ExecContext(ctx, stmt); err != nil {
			return err
		}
	}

	return nil
}

func createDBIndexes(ctx context.Context, conn *sql.Conn) error {
	stmts := []string{
		`CREATE INDEX idx_files_path ON files(path)`,
		`CREATE INDEX idx_nodes_file ON nodes(file_id)`,
		`CREATE INDEX idx_nodes_parent ON nodes(file_id, parent_ordinal)`,
		`CREATE INDEX idx_nodes_kind ON nodes(kind)`,
		`CREATE INDEX idx_nodes_file_offset ON nodes(file_id, start_offset)`,
	}

	for _, stmt := range stmts {
		if _, err := conn.ExecContext(ctx, stmt); err != nil {
			if !strings.Contains(strings.ToLower(err.Error()), "already exists") {
				return err
			}
		}
	}

	return nil
}

func addIndexesInPlace(path, fingerprint string) error {
	db, err := sql.Open("duckdb", path)
	if err != nil {
		return fmt.Errorf("open duckdb: %w", err)
	}
	defer func() { _ = db.Close() }()

	ctx := context.Background()
	conn, err := db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("open duckdb conn: %w", err)
	}
	defer func() { _ = conn.Close() }()

	if err := createDBIndexes(ctx, conn); err != nil {
		return err
	}
	if err := writeRunMeta(ctx, conn, fingerprint, true); err != nil {
		return err
	}
	return nil
}

func writeRunMeta(ctx context.Context, conn *sql.Conn, fingerprint string, indexesEnabled bool) error {
	items := map[string]string{
		"schema_version":     schemaVersion,
		"source_fingerprint": fingerprint,
		"indexes_enabled":    strconv.FormatBool(indexesEnabled),
		"updated_unix":       strconv.FormatInt(time.Now().Unix(), 10),
	}
	for k, v := range items {
		if _, err := conn.ExecContext(ctx, `INSERT INTO run_meta (key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value=excluded.value`, k, v); err != nil {
			return err
		}
	}
	return nil
}

func inspectDuckDB(path string) (dbState, error) {
	if _, err := os.Stat(path); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return dbState{Exists: false, TrackedFiles: map[string]trackedFile{}}, nil
		}
		return dbState{}, fmt.Errorf("stat duckdb file: %w", err)
	}

	db, err := sql.Open("duckdb", path)
	if err != nil {
		return dbState{}, fmt.Errorf("open duckdb: %w", err)
	}
	defer func() { _ = db.Close() }()

	needed := []string{"files", "nodes", "file_state", "run_meta"}
	for _, t := range needed {
		exists, err := tableExists(db, t)
		if err != nil {
			return dbState{}, err
		}
		if !exists {
			return dbState{Exists: true, TrackedFiles: map[string]trackedFile{}}, nil
		}
	}

	meta, err := readMeta(db)
	if err != nil {
		return dbState{}, err
	}
	tracked, err := readTrackedFiles(db)
	if err != nil {
		return dbState{}, err
	}
	filesCount, nodesCount, err := fetchCounts(db)
	if err != nil {
		return dbState{}, err
	}

	return dbState{
		Exists:            true,
		SchemaVersion:     meta["schema_version"],
		SourceFingerprint: meta["source_fingerprint"],
		IndexesEnabled:    strings.EqualFold(meta["indexes_enabled"], "true"),
		FilesCount:        filesCount,
		NodesCount:        nodesCount,
		TrackedFiles:      tracked,
	}, nil
}

func tableExists(db *sql.DB, name string) (bool, error) {
	var count int
	err := db.QueryRow(`SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ?`, name).Scan(&count)
	if err != nil {
		return false, err
	}
	return count > 0, nil
}

func readMeta(db *sql.DB) (map[string]string, error) {
	rows, err := db.Query(`SELECT key, value FROM run_meta`)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	m := map[string]string{}
	for rows.Next() {
		var key string
		var value string
		if err := rows.Scan(&key, &value); err != nil {
			return nil, err
		}
		m[key] = value
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return m, nil
}

func readTrackedFiles(db *sql.DB) (map[string]trackedFile, error) {
	rows, err := db.Query(`SELECT path, file_id, size_bytes, mtime_ns FROM file_state`)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	tracked := map[string]trackedFile{}
	for rows.Next() {
		var path string
		var tf trackedFile
		if err := rows.Scan(&path, &tf.FileID, &tf.Size, &tf.ModUnixNano); err != nil {
			return nil, err
		}
		tracked[path] = tf
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return tracked, nil
}

func fetchCounts(db *sql.DB) (int64, int64, error) {
	var filesCount int64
	if err := db.QueryRow(`SELECT COUNT(*) FROM files`).Scan(&filesCount); err != nil {
		return 0, 0, err
	}
	var nodesCount int64
	if err := db.QueryRow(`SELECT COUNT(*) FROM nodes`).Scan(&nodesCount); err != nil {
		return 0, 0, err
	}
	return filesCount, nodesCount, nil
}

func collectGoFiles(repoRoot, subdir string, maxFiles int) ([]fileMeta, error) {
	skipDirs := map[string]struct{}{
		".git":         {},
		"vendor":       {},
		"node_modules": {},
		"bin":          {},
		".tmp":         {},
		"tmp":          {},
		".cache":       {},
	}

	walkRoot := repoRoot
	if subdir != "" {
		walkRoot = filepath.Join(repoRoot, subdir)
	}

	metas := make([]fileMeta, 0, 1024)
	err := filepath.WalkDir(walkRoot, func(path string, d os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}

		if d.Type()&os.ModeSymlink != 0 {
			if d.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}

		if d.IsDir() {
			if _, ok := skipDirs[d.Name()]; ok {
				return filepath.SkipDir
			}
			return nil
		}

		if !strings.HasSuffix(d.Name(), ".go") {
			return nil
		}

		rel, err := filepath.Rel(repoRoot, path)
		if err != nil {
			return err
		}
		info, err := d.Info()
		if err != nil {
			return err
		}

		metas = append(metas, fileMeta{RelPath: filepath.ToSlash(rel), Size: info.Size(), ModUnixNano: info.ModTime().UnixNano()})
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("walk repo: %w", err)
	}

	sort.Slice(metas, func(i, j int) bool { return metas[i].RelPath < metas[j].RelPath })
	if maxFiles > 0 && len(metas) > maxFiles {
		metas = metas[:maxFiles]
	}
	return metas, nil
}

func fileIDForPath(path string) int64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(path))
	return int64(h.Sum64() & 0x7fffffffffffffff)
}

func sourceFingerprint(metas []fileMeta) string {
	h := fnv.New64a()
	for _, m := range metas {
		_, _ = h.Write([]byte(m.RelPath))
		_, _ = h.Write([]byte{0})
		_, _ = h.Write([]byte(strconv.FormatInt(m.Size, 10)))
		_, _ = h.Write([]byte{0})
		_, _ = h.Write([]byte(strconv.FormatInt(m.ModUnixNano, 10)))
		_, _ = h.Write([]byte{0})
	}
	return fmt.Sprintf("%x", h.Sum64())
}

func cleanupDuckDB(path string) {
	_ = os.Remove(path)
	_ = os.Remove(path + ".wal")
	_ = os.Remove(path + "-wal")
	_ = os.Remove(path + "-shm")
}

func defaultQuerySet() []querySpec {
	return []querySpec{
		{Name: "count_nodes", SQL: `SELECT COUNT(*) FROM nodes`},
		{Name: "group_by_kind_top20", SQL: `SELECT kind, COUNT(*) AS n FROM nodes GROUP BY kind ORDER BY n DESC LIMIT 20`},
		{Name: "funcdecl_join_files", SQL: `SELECT f.path, COUNT(*) AS n FROM nodes n JOIN files f ON f.file_id = n.file_id WHERE n.kind = '*ast.FuncDecl' GROUP BY f.path ORDER BY n DESC LIMIT 50`},
		{Name: "top_files_by_nodes", SQL: `SELECT f.path, COUNT(*) AS n FROM nodes n JOIN files f ON f.file_id = n.file_id GROUP BY f.path ORDER BY n DESC LIMIT 50`},
	}
}

type querySpec struct {
	Name string
	SQL  string
}

func benchmarkQueries(duckdbPath string, queries []querySpec, warmup, iters int) ([]QueryResult, error) {
	if warmup < 0 {
		warmup = 0
	}
	if iters <= 0 {
		iters = 1
	}

	db, err := sql.Open("duckdb", duckdbPath)
	if err != nil {
		return nil, fmt.Errorf("open duckdb for query bench: %w", err)
	}
	defer func() { _ = db.Close() }()

	results := make([]QueryResult, 0, len(queries))
	for _, q := range queries {
		for i := 0; i < warmup; i++ {
			if err := executeQuery(db, q.SQL); err != nil {
				return nil, fmt.Errorf("warmup query %s: %w", q.Name, err)
			}
		}

		start := time.Now()
		for i := 0; i < iters; i++ {
			if err := executeQuery(db, q.SQL); err != nil {
				return nil, fmt.Errorf("run query %s: %w", q.Name, err)
			}
		}
		results = append(results, QueryResult{Name: q.Name, Elapsed: time.Since(start)})
	}

	return results, nil
}

func executeQuery(db *sql.DB, query string) error {
	rows, err := db.Query(query)
	if err != nil {
		return err
	}
	defer func() { _ = rows.Close() }()

	cols, err := rows.Columns()
	if err != nil {
		return err
	}
	vals := make([]any, len(cols))
	ptrs := make([]any, len(cols))
	for i := range vals {
		ptrs[i] = &vals[i]
	}

	for rows.Next() {
		if err := rows.Scan(ptrs...); err != nil {
			return err
		}
	}

	return rows.Err()
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
