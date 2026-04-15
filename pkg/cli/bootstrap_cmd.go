package cli

import (
	"github.com/spf13/cobra"

	"github.com/Yacobolo/quackstack/pkg/cli/apiruntime"
)

func newBootstrapCmd(client *apiruntime.Client) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "bootstrap",
		Short: "Bootstrap opinionated platform setups",
	}

	medallion := &cobra.Command{
		Use:   "medallion",
		Short: "Bootstrap the opinionated medallion baseline",
		Long:  "Creates or verifies an opinionated medallion baseline (landing + bronze/silver/gold, storage bindings, and RBAC presets).",
	}
	medallion.AddCommand(newInitPlanCmd(client))
	medallion.AddCommand(newInitApplyCmd(client))
	medallion.AddCommand(newInitDestroyCmd(client))
	medallion.AddCommand(newInitVerifyCmd(client))
	cmd.AddCommand(medallion)

	return cmd
}
