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

//nolint:gochecknoinits
func init() {
	rootCmd.AddCommand(authCmd)
}
