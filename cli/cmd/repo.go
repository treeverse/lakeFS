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
	"fmt"
	"os"
	"time"

	"github.com/go-openapi/swag"

	"github.com/jedib0t/go-pretty/table"

	"github.com/spf13/cobra"
)

// repoCmd represents the repo command
var repoCmd = &cobra.Command{
	Use:   "repo",
	Short: "manage and explore repos",
}

var repoListCmd = &cobra.Command{
	Use:   "list",
	Short: "list repositories",
	RunE: func(cmd *cobra.Command, args []string) error {

		amount, _ := cmd.Flags().GetInt("amount")
		after, _ := cmd.Flags().GetString("after")

		clt, err := getClient()
		if err != nil {
			return err
		}

		repos, pagination, err := clt.ListRepositories(context.Background(), after, amount)
		// generate table
		t := table.NewWriter()
		t.SetOutputMirror(os.Stdout)
		t.AppendHeader(table.Row{"Repository", "Creation Date", "Default Branch Name", "Storage Namespace"})
		for _, repo := range repos {
			ts := time.Unix(repo.CreationDate, 0).Format(time.RFC1123Z)
			t.AppendRow([]interface{}{repo.ID, ts, repo.DefaultBranch, repo.BucketName})
		}
		t.Render()

		if pagination != nil && swag.BoolValue(pagination.HasMore) {
			fmt.Printf("for more results, run with `--amount %d --after \"%s\"\n", amount, pagination.NextOffset)
		}

		return nil
	},
}

func init() {
	rootCmd.AddCommand(repoCmd)
	repoCmd.AddCommand(repoListCmd)

	repoListCmd.Flags().Int("amount", -1, "how many results to return, or-1 for all results (used for pagination)")
	repoListCmd.Flags().String("after", "", "show results after this value (used for pagination)")
}
