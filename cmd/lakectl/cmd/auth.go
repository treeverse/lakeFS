package cmd

import (
	"github.com/spf13/cobra"
)

var authCmd = &cobra.Command{
	Use:   "auth [sub-command]",
	Short: "Manage authentication and authorization",
	Long:  "Manage authentication and authorization including users, groups and policies",
}

//nolint:gochecknoinits
func init() {
	rootCmd.AddCommand(authCmd)
}
