// Package main is the entry point for the quack CLI binary.
package main

import (
	"os"

	cli "github.com/Yacobolo/quackstack/pkg/cli"
)

func main() {
	os.Exit(cli.Execute())
}
