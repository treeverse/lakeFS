package cmd

import "github.com/spf13/cobra"

var authUsersAWSIAMCmd = &cobra.Command{
	Use:   "aws-iam",
	Short: "Manage AWS IAM",
}

//nolint:gochecknoinits
func init() {
	authUsersCmd.AddCommand(authUsersAWSIAMCmd)
}
