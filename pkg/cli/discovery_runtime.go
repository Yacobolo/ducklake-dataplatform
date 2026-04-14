package cli

import (
	"github.com/spf13/cobra"

	clipkg "github.com/Yacobolo/quackstack/pkg/cli/discovery"
	"github.com/Yacobolo/quackstack/pkg/cli/gen"
)

func loadDiscoveryCorpus(root *cobra.Command) clipkg.Corpus {
	entries := walkCommands(root, "")
	commands := make([]clipkg.CommandInfo, 0, len(entries))
	for _, entry := range entries {
		flags := make([]string, 0, len(entry.Flags))
		for _, flag := range entry.Flags {
			flags = append(flags, flag.Name)
		}
		commands = append(commands, clipkg.CommandInfo{
			Path:    entry.Path,
			Group:   entry.Group,
			Short:   entry.Short,
			Long:    entry.Long,
			Example: entry.Example,
			Args:    entry.Args,
			Flags:   flags,
		})
	}
	return clipkg.NewCorpus(commands, gen.CLIReferenceIndex)
}
