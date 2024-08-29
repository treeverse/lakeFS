package cmd

import (
	"github.com/spf13/cobra"
)

var authCmd = &cobra.Command{
	Use:   "auth [sub-command]",
	Short: "Manage authentication and authorization",
	Long: `Manage authentication and authorization including users, groups and ACLs
This functionality is supported with an external auth service only.`,
}

func addPaginationFlags(cmd *cobra.Command) {
	cmd.Flags().SortFlags = false
	cmd.Flags().Int("amount", defaultAmountArgumentValue, "how many results to return")
	cmd.Flags().String("after", "", "show results after this value (used for pagination)")
}

//nolint:gochecknoinits
func init() {
	rootCmd.AddCommand(authCmd)
}
