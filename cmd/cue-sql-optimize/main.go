// Command cue-sql-optimize rewrites querydefs into a compact canonical CUE form.
package main

import (
	"flag"
	"fmt"
	"os"

	"duck-demo/internal/cuesqlgen"
)

func main() {
	srcDir := flag.String("src", "internal/db/querydefs", "directory containing cue-sql querydefs")
	flag.Parse()

	stats, err := cuesqlgen.OptimizeQuerydefFiles(*srcDir)
	if err != nil {
		fmt.Fprintf(os.Stderr, "cue-sql-optimize: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("optimized %d queries across %d files\n", stats.QueriesChanged, stats.FilesChanged)
}
