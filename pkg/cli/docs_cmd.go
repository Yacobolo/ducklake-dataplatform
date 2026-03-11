package cli

import (
	"fmt"
	"os"
	"strings"

	"github.com/spf13/cobra"

	"duck-demo/pkg/cli/apiruntime"
	clipkg "duck-demo/pkg/cli/discovery"
)

func newDocsCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "docs",
		Short: "Browse offline product and reference docs",
		Long:  "Search and display local documentation generated from the canonical docs tree and OpenAPI contract.",
	}

	cmd.AddCommand(newDocsSearchCmd())
	cmd.AddCommand(newDocsShowCmd())
	cmd.AddCommand(newDocsListCmd())
	return cmd
}

func newDocsSearchCmd() *cobra.Command {
	var (
		opts clipkg.SearchOptions
	)

	cmd := &cobra.Command{
		Use:   "search <query>",
		Short: "Search docs by title, headings, keywords, and examples",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			corpus := loadDiscoveryCorpus(cmd.Root())
			opts.Kind = "doc"
			results := corpus.Search(args[0], opts)

			if getOutputFormat(cmd) == "json" {
				return apiruntime.PrintJSON(os.Stdout, map[string]any{"results": results})
			}

			rows := make([][]string, 0, len(results))
			for _, result := range results {
				rows = append(rows, []string{result.ID, result.Title, result.Summary})
			}
			apiruntime.PrintTable(os.Stdout, []string{"doc_id", "title", "summary"}, rows)
			return nil
		},
	}

	cmd.Flags().StringVar(&opts.Section, "section", "", "Filter by docs section")
	cmd.Flags().StringVar(&opts.Audience, "audience", "", "Filter by audience")
	cmd.Flags().StringVar(&opts.ProductArea, "product-area", "", "Filter by product area")
	cmd.Flags().StringVar(&opts.Surface, "surface", "", "Filter by access surface")
	cmd.Flags().StringVar(&opts.Task, "task", "", "Filter by task")
	cmd.Flags().StringVar(&opts.DocKind, "doc-kind", "", "Filter by doc kind")
	cmd.Flags().IntVar(&opts.Limit, "limit", 10, "Maximum number of results")
	return cmd
}

func newDocsShowCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "show <doc-id>",
		Short: "Show a documentation page from the local docs corpus",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			corpus := loadDiscoveryCorpus(cmd.Root())
			doc, ok := corpus.FindDoc(args[0])
			if !ok {
				return fmt.Errorf("doc %q not found", args[0])
			}

			payload := map[string]any{
				"doc":                doc,
				"related_docs":       corpus.RelatedDocsForDoc(doc.ID),
				"related_commands":   corpus.RelatedCommandsForDoc(doc.ID),
				"related_operations": corpus.RelatedOperationsForDoc(doc.ID),
			}
			if getOutputFormat(cmd) == "json" {
				return apiruntime.PrintJSON(os.Stdout, payload)
			}

			_, _ = fmt.Fprintf(os.Stdout, "%s\n", strings.ToUpper(doc.Title))
			_, _ = fmt.Fprintf(os.Stdout, "id:          %s\n", doc.ID)
			_, _ = fmt.Fprintf(os.Stdout, "path:        %s\n", doc.Path)
			_, _ = fmt.Fprintf(os.Stdout, "section:     %s\n", doc.Section)
			_, _ = fmt.Fprintf(os.Stdout, "doc_kind:    %s\n", doc.DocKind)
			if len(doc.Audiences) > 0 {
				_, _ = fmt.Fprintf(os.Stdout, "audiences:   %s\n", strings.Join(doc.Audiences, ", "))
			}
			if len(doc.ProductAreas) > 0 {
				_, _ = fmt.Fprintf(os.Stdout, "areas:       %s\n", strings.Join(doc.ProductAreas, ", "))
			}
			if len(doc.Surfaces) > 0 {
				_, _ = fmt.Fprintf(os.Stdout, "surfaces:    %s\n", strings.Join(doc.Surfaces, ", "))
			}
			if len(doc.Tasks) > 0 {
				_, _ = fmt.Fprintf(os.Stdout, "tasks:       %s\n", strings.Join(doc.Tasks, ", "))
			}
			if len(doc.Permissions) > 0 {
				_, _ = fmt.Fprintf(os.Stdout, "permissions: %s\n", strings.Join(doc.Permissions, ", "))
			}
			if doc.LastVerified != "" {
				_, _ = fmt.Fprintf(os.Stdout, "verified:    %s\n", doc.LastVerified)
			}
			if doc.Description != "" {
				_, _ = fmt.Fprintf(os.Stdout, "description: %s\n", doc.Description)
			}
			if doc.Excerpt != "" {
				_, _ = fmt.Fprintf(os.Stdout, "\n%s\n", doc.Excerpt)
			}
			if len(doc.Headings) > 0 {
				_, _ = fmt.Fprintln(os.Stdout, "\nHEADINGS:")
				for _, heading := range doc.Headings {
					_, _ = fmt.Fprintf(os.Stdout, "- %s\n", heading)
				}
			}
			if len(doc.CodeExamples) > 0 {
				_, _ = fmt.Fprintln(os.Stdout, "\nEXAMPLES:")
				for _, example := range doc.CodeExamples {
					_, _ = fmt.Fprintln(os.Stdout, "```")
					_, _ = fmt.Fprintln(os.Stdout, example)
					_, _ = fmt.Fprintln(os.Stdout, "```")
				}
			}
			if relatedDocs := corpus.RelatedDocsForDoc(doc.ID); len(relatedDocs) > 0 {
				_, _ = fmt.Fprintln(os.Stdout, "\nRELATED DOCS:")
				for _, relatedDoc := range relatedDocs {
					_, _ = fmt.Fprintf(os.Stdout, "- %s\n", relatedDoc)
				}
			}
			return nil
		},
	}
}

func newDocsListCmd() *cobra.Command {
	var opts clipkg.SearchOptions

	cmd := &cobra.Command{
		Use:   "list",
		Short: "List available docs in the local docs corpus",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			corpus := loadDiscoveryCorpus(cmd.Root())
			docs := corpus.ListDocs(opts)

			if getOutputFormat(cmd) == "json" {
				return apiruntime.PrintJSON(os.Stdout, docs)
			}

			rows := make([][]string, 0, len(docs))
			for _, doc := range docs {
				rows = append(rows, []string{doc.ID, doc.Section, doc.Title})
			}
			apiruntime.PrintTable(os.Stdout, []string{"doc_id", "section", "title"}, rows)
			return nil
		},
	}

	cmd.Flags().StringVar(&opts.Section, "section", "", "Filter by docs section")
	cmd.Flags().StringVar(&opts.Audience, "audience", "", "Filter by audience")
	cmd.Flags().StringVar(&opts.ProductArea, "product-area", "", "Filter by product area")
	cmd.Flags().StringVar(&opts.Surface, "surface", "", "Filter by access surface")
	cmd.Flags().StringVar(&opts.Task, "task", "", "Filter by task")
	cmd.Flags().StringVar(&opts.DocKind, "doc-kind", "", "Filter by doc kind")
	return cmd
}
