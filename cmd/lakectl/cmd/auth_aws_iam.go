package cmd

import "github.com/spf13/cobra"

var authAWSIAMCmd = &cobra.Command{
	Use:   "aws-iam",
	Short: "Manage AWS IAM",
}

//nolint:gochecknoinits
func init() {
	authCmd.AddCommand(authAWSIAMCmd)
}
