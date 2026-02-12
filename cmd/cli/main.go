package main

import (
	"os"

	cli "duck-demo/pkg/cli"
)

func main() {
	os.Exit(cli.Execute())
}
