package cmd

import (
	"github.com/spf13/cobra"
)

var authCmd = &cobra.Command{
	Use:   "auth [sub-command]",
	Short: "Manage authentication and authorization",
	Long:  "manage authentication and authorization including users, groups and ACLs",
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
