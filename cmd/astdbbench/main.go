package main

import (
	"context"
	"flag"
	"fmt"
	"log"

	"duck-demo/pkg/astdb"
)

func main() {
	defaults := astdb.DefaultOptions()

	opts := astdb.Options{}
	flag.StringVar(&opts.RepoRoot, "repo", defaults.RepoRoot, "repository root to scan")
	flag.StringVar(&opts.Subdir, "subdir", defaults.Subdir, "optional subdirectory under repo root to scan")
	flag.IntVar(&opts.MaxFiles, "max-files", defaults.MaxFiles, "optional cap for number of .go files after sorting (0 = all)")
	flag.IntVar(&opts.Workers, "workers", defaults.Workers, "number of parser workers")
	flag.StringVar(&opts.DuckDBPath, "duckdb", defaults.DuckDBPath, "output DuckDB database path")
	flag.StringVar(&opts.Mode, "mode", defaults.Mode, "run mode: both, build, query")
	flag.BoolVar(&opts.Reuse, "reuse", defaults.Reuse, "reuse existing DB and apply incremental updates")
	flag.BoolVar(&opts.ForceRebuild, "force-rebuild", defaults.ForceRebuild, "force full rebuild")
	flag.BoolVar(&opts.CreateIndexes, "indexes", defaults.CreateIndexes, "create indexes for query speed")
	flag.BoolVar(&opts.QueryBench, "query-bench", defaults.QueryBench, "run read query benchmarks")
	flag.IntVar(&opts.QueryWarmup, "query-warmup", defaults.QueryWarmup, "warmup runs per query")
	flag.IntVar(&opts.QueryIters, "query-iters", defaults.QueryIters, "measured iterations per query")
	flag.BoolVar(&opts.KeepOutputFiles, "keep", defaults.KeepOutputFiles, "keep output database file")
	flag.Parse()

	result, err := astdb.Run(context.Background(), opts)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("scan: files=%d subdir=%q max_files=%d scan_ms=%d\n", result.ScanFiles, result.Subdir, result.MaxFiles, result.ScanElapsed.Milliseconds())
	fmt.Printf("build: action=%s reason=%q workers=%d changed=%d deleted=%d unchanged=%d parse_errors=%d parse_ms=%d load_ms=%d indexes=%t\n",
		result.Sync.Action,
		result.Sync.Reason,
		opts.Workers,
		result.Sync.Changed,
		result.Sync.Deleted,
		result.Sync.Unchanged,
		result.Sync.ParseErrors,
		result.Sync.ParseElapsed.Milliseconds(),
		result.Sync.LoadElapsed.Milliseconds(),
		result.Sync.Indexes,
	)
	fmt.Printf("db: files=%d nodes=%d\n", result.Sync.FilesCount, result.Sync.NodesCount)

	if len(result.QueryResults) > 0 {
		fmt.Printf("queries: warmup=%d iters=%d\n", result.QueryWarmup, result.QueryIters)
		for i, q := range result.QueryResults {
			avgMS := float64(q.Elapsed.Milliseconds()) / float64(result.QueryIters)
			fmt.Printf("query[%d] %s: total_ms=%d avg_ms=%.3f\n", i+1, q.Name, q.Elapsed.Milliseconds(), avgMS)
		}
	}
}
