package cli

import (
	"os"
	"strings"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"

	"duck-demo/pkg/cli/gen"
)

// CommandEntry represents a single CLI command for introspection output.
type CommandEntry struct {
	Path    string      `json:"path"`
	Group   string      `json:"group"`
	Short   string      `json:"short"`
	Long    string      `json:"long,omitempty"`
	Example string      `json:"example,omitempty"`
	Args    string      `json:"args,omitempty"`
	Flags   []FlagEntry `json:"flags,omitempty"`
}

// FlagEntry represents a single CLI flag for introspection output.
type FlagEntry struct {
	Name     string `json:"name"`
	Short    string `json:"shorthand,omitempty"`
	Type     string `json:"type"`
	Default  string `json:"default,omitempty"`
	Usage    string `json:"usage,omitempty"`
	Required bool   `json:"required,omitempty"`
}

func newCommandsCmd() *cobra.Command {
	var (
		filter string
		group  string
	)

	cmd := &cobra.Command{
		Use:   "commands",
		Short: "List all available CLI commands with their flags and descriptions",
		Long: `Introspects the command tree and lists all commands with their paths,
descriptions, flags, and examples. Works offline (no API calls needed).

This is designed for AI agents to discover available CLI capabilities in a single call.`,
		Example: `  # List all commands
  duck commands

  # Search for commands related to row filters
  duck commands --filter "row-filter"

  # List only security commands as JSON
  duck commands --group security --output json

  # Get full command metadata for agent consumption
  duck commands --output json`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			entries := walkCommands(cmd.Root(), "")

			// Apply filters
			if group != "" {
				var filtered []CommandEntry
				for _, e := range entries {
					if e.Group == group {
						filtered = append(filtered, e)
					}
				}
				entries = filtered
			}
			if filter != "" {
				lowerFilter := strings.ToLower(filter)
				var filtered []CommandEntry
				for _, e := range entries {
					searchText := strings.ToLower(e.Path + " " + e.Short + " " + e.Long)
					if strings.Contains(searchText, lowerFilter) {
						filtered = append(filtered, e)
					}
				}
				entries = filtered
			}

			// Output
			if getOutputFormat(cmd) == "json" {
				return gen.PrintJSON(os.Stdout, entries)
			}

			// Table output
			columns := []string{"path", "description"}
			rows := make([][]string, 0, len(entries))
			for _, e := range entries {
				rows = append(rows, []string{e.Path, e.Short})
			}
			gen.PrintTable(os.Stdout, columns, rows)
			return nil
		},
	}

	cmd.Flags().StringVar(&filter, "filter", "", "Substring search across command names and descriptions")
	cmd.Flags().StringVar(&group, "group", "", "Filter by command group (e.g., catalog, security)")

	return cmd
}

// walkCommands recursively walks the cobra command tree and collects leaf commands.
func walkCommands(cmd *cobra.Command, parentPath string) []CommandEntry {
	var entries []CommandEntry

	for _, child := range cmd.Commands() {
		if child.Hidden || child.Name() == "help" || child.Name() == "completion" {
			continue
		}

		childPath := child.Name()
		if parentPath != "" {
			childPath = parentPath + " " + child.Name()
		}

		// If the child has its own subcommands, recurse
		if child.HasSubCommands() {
			entries = append(entries, walkCommands(child, childPath)...)
			continue
		}

		// Leaf command — collect its metadata
		group := ""
		parts := strings.SplitN(childPath, " ", 2)
		if len(parts) > 0 {
			group = parts[0]
		}

		// Extract positional args from Use string
		args := ""
		useParts := strings.Fields(child.Use)
		if len(useParts) > 1 {
			args = strings.Join(useParts[1:], " ")
		}

		entry := CommandEntry{
			Path:    childPath,
			Group:   group,
			Short:   child.Short,
			Long:    child.Long,
			Example: child.Example,
			Args:    args,
			Flags:   collectFlags(child),
		}
		entries = append(entries, entry)
	}

	return entries
}

// collectFlags gathers flag metadata from a command.
func collectFlags(cmd *cobra.Command) []FlagEntry {
	var flags []FlagEntry
	cmd.Flags().VisitAll(func(f *pflag.Flag) {
		if f.Hidden {
			return
		}
		// Skip help flag
		if f.Name == "help" {
			return
		}
		entry := FlagEntry{
			Name:    f.Name,
			Short:   f.Shorthand,
			Type:    f.Value.Type(),
			Default: f.DefValue,
			Usage:   f.Usage,
		}
		// Check if flag is required by looking at annotations
		if ann, ok := f.Annotations[cobra.BashCompOneRequiredFlag]; ok && len(ann) > 0 && ann[0] == "true" {
			entry.Required = true
		}
		flags = append(flags, entry)
	})
	return flags
}
