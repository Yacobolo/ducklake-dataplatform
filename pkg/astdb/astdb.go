// Package astdb builds a lightweight Go AST index in DuckDB for governance queries.
package astdb

import (
	"context"
	"database/sql"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"sort"
	"strings"

	// Register DuckDB SQL driver.
	_ "github.com/duckdb/duckdb-go/v2"
)

// Options controls AST indexing behavior.
type Options struct {
	RepoRoot        string
	Subdir          string
	MaxFiles        int
	Mode            string
	QueryBench      bool
	DuckDBPath      string
	KeepOutputFiles bool
	Reuse           bool
	ForceRebuild    bool
}

// Result summarizes indexing output.
type Result struct {
	Files int
	Nodes int
}

// DefaultOptions returns baseline indexing options.
func DefaultOptions() Options {
	return Options{
		RepoRoot:   ".",
		Subdir:     "internal",
		MaxFiles:   0,
		Mode:       "build",
		QueryBench: false,
		DuckDBPath: filepath.Join(os.TempDir(), "astdb.duckdb"),
	}
}

// Run indexes Go files under the configured root into DuckDB tables.
func Run(ctx context.Context, opts Options) (*Result, error) {
	if strings.TrimSpace(opts.RepoRoot) == "" {
		return nil, fmt.Errorf("repo root is required")
	}
	if strings.TrimSpace(opts.DuckDBPath) == "" {
		return nil, fmt.Errorf("duckdb path is required")
	}

	root := opts.RepoRoot
	if opts.Subdir != "" {
		root = filepath.Join(root, opts.Subdir)
	}

	files, err := collectGoFiles(root, opts.MaxFiles)
	if err != nil {
		return nil, err
	}

	db, err := sql.Open("duckdb", opts.DuckDBPath)
	if err != nil {
		return nil, fmt.Errorf("open duckdb: %w", err)
	}
	defer func() { _ = db.Close() }()

	if err := ensureSchema(ctx, db); err != nil {
		return nil, err
	}
	if err := clearTables(ctx, db); err != nil {
		return nil, err
	}

	insertFile, err := db.PrepareContext(ctx, `INSERT INTO files (file_id, path) VALUES (?, ?)`)
	if err != nil {
		return nil, fmt.Errorf("prepare file insert: %w", err)
	}
	defer func() { _ = insertFile.Close() }()

	insertNode, err := db.PrepareContext(ctx, `INSERT INTO nodes (file_id, kind, node_text) VALUES (?, ?, ?)`)
	if err != nil {
		return nil, fmt.Errorf("prepare node insert: %w", err)
	}
	defer func() { _ = insertNode.Close() }()

	fileCount := 0
	nodeCount := 0

	for i, file := range files {
		relPath, err := filepath.Rel(opts.RepoRoot, file)
		if err != nil {
			return nil, fmt.Errorf("relative path for %s: %w", file, err)
		}
		relPath = filepath.ToSlash(relPath)

		fileID := i + 1
		if _, err := insertFile.ExecContext(ctx, fileID, relPath); err != nil {
			return nil, fmt.Errorf("insert file %s: %w", relPath, err)
		}
		fileCount++

		fset := token.NewFileSet()
		node, err := parser.ParseFile(fset, file, nil, parser.ImportsOnly)
		if err != nil {
			return nil, fmt.Errorf("parse %s: %w", file, err)
		}

		for _, imp := range node.Imports {
			if _, err := insertNode.ExecContext(ctx, fileID, "*ast.ImportSpec", imp.Path.Value); err != nil {
				return nil, fmt.Errorf("insert import node for %s: %w", relPath, err)
			}
			nodeCount++
		}

		if _, err := insertNode.ExecContext(ctx, fileID, "*ast.File", relPath); err != nil {
			return nil, fmt.Errorf("insert file node for %s: %w", relPath, err)
		}
		nodeCount++

		fullAst, err := parser.ParseFile(fset, file, nil, 0)
		if err != nil {
			return nil, fmt.Errorf("parse full AST %s: %w", file, err)
		}
		for _, decl := range fullAst.Decls {
			if fn, ok := decl.(*ast.FuncDecl); ok {
				name := ""
				if fn.Name != nil {
					name = fn.Name.Name
				}
				if _, err := insertNode.ExecContext(ctx, fileID, "*ast.FuncDecl", name); err != nil {
					return nil, fmt.Errorf("insert func node for %s: %w", relPath, err)
				}
				nodeCount++
			}
		}
	}

	return &Result{Files: fileCount, Nodes: nodeCount}, nil
}

func collectGoFiles(root string, maxFiles int) ([]string, error) {
	out := make([]string, 0, 512)
	err := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			if d.Name() == ".git" || d.Name() == "vendor" || d.Name() == ".tmp" || d.Name() == ".goast" {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(d.Name(), ".go") {
			return nil
		}
		out = append(out, path)
		if maxFiles > 0 && len(out) >= maxFiles {
			return filepath.SkipDir
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("walk go files: %w", err)
	}
	sort.Strings(out)
	if maxFiles > 0 && len(out) > maxFiles {
		out = out[:maxFiles]
	}
	return out, nil
}

func ensureSchema(ctx context.Context, db *sql.DB) error {
	if _, err := db.ExecContext(ctx, `
CREATE TABLE IF NOT EXISTS files (
  file_id BIGINT,
  path TEXT NOT NULL
)`); err != nil {
		return fmt.Errorf("ensure files table: %w", err)
	}
	if _, err := db.ExecContext(ctx, `
CREATE TABLE IF NOT EXISTS nodes (
  file_id BIGINT NOT NULL,
  kind TEXT NOT NULL,
  node_text TEXT
)`); err != nil {
		return fmt.Errorf("ensure nodes table: %w", err)
	}
	return nil
}

func clearTables(ctx context.Context, db *sql.DB) error {
	if _, err := db.ExecContext(ctx, `DELETE FROM nodes`); err != nil {
		return fmt.Errorf("clear nodes: %w", err)
	}
	if _, err := db.ExecContext(ctx, `DELETE FROM files`); err != nil {
		return fmt.Errorf("clear files: %w", err)
	}
	return nil
}
