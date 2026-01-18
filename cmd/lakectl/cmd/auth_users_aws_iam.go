package cmd

import "github.com/spf13/cobra"

var authUsersAWSIAMCmd = &cobra.Command{
	Use:   "aws-iam",
	Short: "Manage AWS IAM Role for lakeFS Users (External Principals API)",
}

//nolint:gochecknoinits
func init() {
	authUsersCmd.AddCommand(authUsersAWSIAMCmd)
}

var idHelperText = "lakeFS Username (default: current user)"
