/*
Copyright © 2020 NAME HERE <EMAIL ADDRESS>

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package cmd

import (
	"context"

	"github.com/spf13/cobra"
	"github.com/treeverse/lakefs/uri"
)

// logCmd represents the log command
var logCmd = &cobra.Command{
	Use:   "log [branch uri]",
	Short: "show log of commits for the given branch",
	Args: ValidationChain(
		HasNArgs(1),
		IsBranchURI(0),
	),
	RunE: func(cmd *cobra.Command, args []string) error {
		client, err := getClient()
		if err != nil {
			return err
		}
		branchURI := uri.Must(uri.Parse(args[0]))
		commits, err := client.GetCommitLog(context.Background(), branchURI.Repository, branchURI.Refspec)
		if err != nil {
			return err
		}

		for _, commit := range commits {
			printCommit(commit)
		}
		return nil
	},
}

func init() {
	rootCmd.AddCommand(logCmd)

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// logCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// logCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}
