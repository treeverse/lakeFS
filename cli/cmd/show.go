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
	"strings"

	"github.com/treeverse/lakefs/api/gen/models"

	"github.com/treeverse/lakefs/uri"

	"github.com/spf13/cobra"
)

// showCmd represents the show command
var showCmd = &cobra.Command{
	Use:   "show [repository uri]",
	Short: "See detailed information about an entity by ID (commit, user, etc)",
	Args: ValidationChain(
		HasNArgs(1),
		IsRepoURI(0),
	),
	Run: func(cmd *cobra.Command, args []string) {
		u := uri.Must(uri.Parse(args[0]))
		oneOf := []string{"commit"}
		var found bool
		var showType, identifier string
		for _, flagName := range oneOf {
			value, err := cmd.Flags().GetString(flagName)
			if err != nil {
				continue
			}
			if len(value) > 0 {
				if found {
					DieFmt("please specify one of \"%s\"", strings.Join(oneOf, ", "))
				}
				found = true
				showType = flagName
				identifier = value
			}
		}

		switch showType {
		case "commit":
			client := getClient()
			commit, err := client.GetCommit(context.Background(), u.Repository, identifier)
			if err != nil {
				DieErr(err)
			}
			Write(commitsTemplate, []*models.Commit{commit})
		}
	},
}

func init() {
	rootCmd.AddCommand(showCmd)

	showCmd.Flags().String("commit", "", "commit id to show")
}
