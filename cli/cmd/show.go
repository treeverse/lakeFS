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
	"sort"
	"strings"
	"time"

	"github.com/jedib0t/go-pretty/text"

	"github.com/jedib0t/go-pretty/table"

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
	RunE: func(cmd *cobra.Command, args []string) error {
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
					return fmt.Errorf("please specify one of \"%s\"", strings.Join(oneOf, ", "))
				}
				found = true
				showType = flagName
				identifier = value
			}
		}

		switch showType {
		case "commit":
			return showCommit(u.Repository, identifier)
		}
		return nil
	},
}

func printCommit(commit *models.Commit) {

	os.Stdout.WriteString(text.FgYellow.Sprintf("commit %s\n", commit.ID))

	fmt.Printf("Author: %s\nDate: %s\nparents: ",
		commit.Committer,
		time.Unix(commit.CreationDate, 0).Format(time.RFC1123Z))

	os.Stdout.WriteString(text.FgYellow.Sprintf("%s\n", strings.Join(commit.Parents, ", ")))

	if len(commit.Metadata) > 0 {
		t := table.NewWriter()
		t.SetOutputMirror(os.Stdout)
		t.AppendHeader(table.Row{"Metadata Key", "Metadata Value"})
		keys := make([]string, len(commit.Metadata))
		var i int
		for k := range commit.Metadata {
			keys[i] = k
			i++
		}
		sort.Strings(keys)
		for _, key := range keys {
			t.AppendRow([]interface{}{key, commit.Metadata[key]})
		}
		t.Render()
	}

	fmt.Printf("\n\n\t%s\n\n", commit.Message)
}

func showCommit(repoId, identifier string) error {
	client, err := getClient()
	if err != nil {
		return err
	}
	commit, err := client.GetCommit(context.Background(), repoId, identifier)
	if err != nil {
		return err
	}
	printCommit(commit)
	return nil
}

func init() {
	rootCmd.AddCommand(showCmd)

	showCmd.Flags().String("commit", "", "commit id to show")
}
