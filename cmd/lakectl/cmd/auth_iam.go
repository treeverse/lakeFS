package cmd

import "github.com/spf13/cobra"

var authIAMCmd = &cobra.Command{
	Use:   "iam",
	Short: "Manage IAM",
}

//nolint:gochecknoinits
func init() {
	authCmd.AddCommand(authIAMCmd)
}
