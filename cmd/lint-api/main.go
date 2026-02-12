// Command lint-api checks an OpenAPI 3.x spec for project convention violations.
//
// Usage:
//
//	go run ./cmd/lint-api [flags] <openapi.yaml>
//
// Flags:
//
//	-severity   Minimum severity to report: error, warning, info (default: all)
//	-format     Output format: text, json (default: text)
//	-config     Path to .apilint.yaml configuration file
//	-list-rules Print all registered rules and exit
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"text/tabwriter"

	"duck-demo/pkg/apilint"
)

func main() {
	severity := flag.String("severity", "", "minimum severity to report: error, warning, info (default: all)")
	format := flag.String("format", "text", "output format: text, json")
	configPath := flag.String("config", "", "path to .apilint.yaml configuration file")
	listRules := flag.Bool("list-rules", false, "print all registered rules and exit")
	flag.Parse()

	if *listRules {
		printRules()
		return
	}

	if flag.NArg() < 1 {
		fmt.Fprintf(os.Stderr, "usage: lint-api [flags] <openapi.yaml>\n")
		os.Exit(2)
	}
	path := flag.Arg(0)

	// Load optional config.
	var cfg *apilint.Config
	if *configPath != "" {
		var err error
		cfg, err = apilint.LoadConfig(*configPath)
		if err != nil {
			fmt.Fprintf(os.Stderr, "error: %v\n", err)
			os.Exit(2)
		}
	}

	linter, err := apilint.New(path)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(2)
	}

	violations := linter.RunWithConfig(cfg)

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

	switch *format {
	case "text":
		printText(violations, path)
	case "json":
		printJSON(violations)
	default:
		fmt.Fprintf(os.Stderr, "error: unknown format %q (use: text, json)\n", *format)
		os.Exit(2)
	}

	if apilint.HasErrors(violations) {
		os.Exit(1)
	}
}

func printText(violations []apilint.Violation, path string) {
	for _, v := range violations {
		fmt.Println(v)
	}
	if len(violations) == 0 {
		fmt.Printf("%s: ok (0 violations)\n", path)
	} else {
		fmt.Printf("\n%d violation(s) found\n", len(violations))
	}
}

func printJSON(violations []apilint.Violation) {
	type jsonViolation struct {
		File     string `json:"file"`
		Line     int    `json:"line"`
		RuleID   string `json:"rule_id"`
		Severity string `json:"severity"`
		Message  string `json:"message"`
	}

	out := make([]jsonViolation, len(violations))
	for i, v := range violations {
		out[i] = jsonViolation{
			File:     v.File,
			Line:     v.Line,
			RuleID:   v.RuleID,
			Severity: string(v.Severity),
			Message:  v.Message,
		}
	}

	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	_ = enc.Encode(out)
}

func printRules() {
	rules := apilint.RegisteredRules()
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	_, _ = fmt.Fprintf(w, "ID\tSEVERITY\tDESCRIPTION\n")
	_, _ = fmt.Fprintf(w, "--\t--------\t-----------\n")
	for _, r := range rules {
		_, _ = fmt.Fprintf(w, "%s\t%s\t%s\n", r.ID(), r.DefaultSeverity(), r.Description())
	}
	_ = w.Flush()
}
