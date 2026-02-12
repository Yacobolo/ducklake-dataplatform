// Package main is the entry point for the duck CLI binary.
package main

import (
	"os"

	cli "duck-demo/pkg/cli"
)

func main() {
	os.Exit(cli.Execute())
}
