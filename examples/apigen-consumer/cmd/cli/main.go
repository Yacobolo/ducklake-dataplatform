package main

import (
	"log"

	cobraruntime "duck-demo/apigen/runtime/cobra"
	"github.com/spf13/cobra"

	"github.com/example/apigen-consumer/cmd/cli/gen"
)

func main() {
	root := &cobra.Command{
		Use:   "example",
		Short: "Example APIGen CLI consumer",
	}

	client := cobraruntime.NewClient("http://127.0.0.1:8081", "", "")
	if err := cobraruntime.AddGeneratedCommands(root, client, gen.APIGeneratedEndpoints); err != nil {
		log.Fatal(err)
	}
	if err := root.Execute(); err != nil {
		log.Fatal(err)
	}
}
