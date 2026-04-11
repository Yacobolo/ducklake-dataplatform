// Command cue-sql-gen generates typed Go query code from CUE query definitions.
package main

import (
	"flag"
	"fmt"
	"os"

	"duck-demo/internal/cuesqlgen"
)

func main() {
	var (
		srcDir        = flag.String("src", "internal/db/querydefs", "directory containing CUE query definitions")
		outDir        = flag.String("outdir", "internal/db/cuestore", "directory for generated Go output")
		migrationsDir = flag.String("migrations", "internal/db/migrations", "directory containing SQLite migrations")
	)
	flag.Parse()

	generator := cuesqlgen.Generator{
		MigrationsDir: *migrationsDir,
		SourceDir:     *srcDir,
		OutputDir:     *outDir,
	}
	if err := generator.Run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
