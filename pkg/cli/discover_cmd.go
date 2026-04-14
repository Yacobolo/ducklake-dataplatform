package cli

import (
	"os"

	"github.com/spf13/cobra"

	"github.com/Yacobolo/quackstack/pkg/cli/apiruntime"
	clipkg "github.com/Yacobolo/quackstack/pkg/cli/discovery"
)

func newDiscoverCmd() *cobra.Command {
	var (
		kind  string
		limit int
	)

	cmd := &cobra.Command{
		Use:   "discover <query>",
		Short: "Search commands, docs, and API operations in one place",
		Long:  "Unified offline search across CLI commands, product docs, and API reference metadata.",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			corpus := loadDiscoveryCorpus(cmd.Root())
			results := corpus.Search(args[0], clipkg.SearchOptions{Kind: kind, Limit: limit})

			if getOutputFormat(cmd) == "json" {
				return apiruntime.PrintJSON(os.Stdout, map[string]any{"results": results})
			}

			rows := make([][]string, 0, len(results))
			for _, result := range results {
				rows = append(rows, []string{result.Kind, result.Title, result.Summary})
			}
			apiruntime.PrintTable(os.Stdout, []string{"kind", "title", "summary"}, rows)
			return nil
		},
	}

	cmd.Flags().StringVar(&kind, "kind", "all", "Filter by result kind: command, operation, doc, all")
	cmd.Flags().IntVar(&limit, "limit", 10, "Maximum number of results")
	return cmd
}
