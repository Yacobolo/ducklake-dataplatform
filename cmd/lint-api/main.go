// Command lint-api checks an OpenAPI 3.x spec for project convention violations.
//
// Usage:
//
//	go run ./cmd/lint-api [flags] <openapi.yaml>
//
// Flags:
//
//	-severity  Minimum severity to report: error, warning, info (default: all)
package main

import (
	"flag"
	"fmt"
	"os"

	"duck-demo/pkg/apilint"
)

func main() {
	severity := flag.String("severity", "", "minimum severity to report: error, warning, info (default: all)")
	flag.Parse()

	if flag.NArg() < 1 {
		fmt.Fprintf(os.Stderr, "usage: lint-api [flags] <openapi.yaml>\n")
		os.Exit(2)
	}
	path := flag.Arg(0)

	linter, err := apilint.New(path)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(2)
	}

	violations := linter.Run()

	if *severity != "" {
		sev := apilint.Severity(*severity)
		switch sev {
		case apilint.SeverityError, apilint.SeverityWarning, apilint.SeverityInfo:
			violations = apilint.Filter(violations, sev)
		default:
			fmt.Fprintf(os.Stderr, "error: unknown severity %q (use: error, warning, info)\n", *severity)
			os.Exit(2)
		}
	}

	for _, v := range violations {
		fmt.Println(v)
	}

	if len(violations) == 0 {
		fmt.Printf("%s: ok (0 violations)\n", path)
	} else {
		fmt.Printf("\n%d violation(s) found\n", len(violations))
	}

	if apilint.HasErrors(violations) {
		os.Exit(1)
	}
}
