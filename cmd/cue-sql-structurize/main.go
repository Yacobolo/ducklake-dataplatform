// Command cue-sql-structurize converts simple legacy raw querydefs into structured cue-sql statements.
package main

import (
	"flag"
	"fmt"
	"os"

	"duck-demo/internal/cuesqlgen"
)

func main() {
	srcDir := flag.String("src", "internal/db/querydefs", "directory containing legacy cue-sql querydefs")
	flag.Parse()

	stats, err := cuesqlgen.StructurizeLegacyFiles(*srcDir)
	if err != nil {
		fmt.Fprintf(os.Stderr, "cue-sql-structurize: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("structurized %d queries across %d files\n", stats.QueriesChanged, stats.FilesChanged)
}
