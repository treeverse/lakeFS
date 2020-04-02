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
	"github.com/jedib0t/go-pretty/text"
	"github.com/spf13/cobra"
	"github.com/treeverse/lakefs/api/gen/models"
	"github.com/treeverse/lakefs/uri"
	"os"
)

// mergeCmd represents the merge command
var mergeCmd = &cobra.Command{
	Use:   "merge",
	Short: "merge  source into  destination ",
	Long:  "merge & commit changes from source branch into destination branch",
	Args: ValidationChain(
		HasRangeArgs(2, 2),
		IsRefURI(0),
		IsRefURI(1),
	),
	Run: func(cmd *cobra.Command, args []string) {
		client := getClient()

		var err error
		if err := IsRefURI(1)(args); err != nil {
			DieErr(err)
		}
		rightRefURI := uri.Must(uri.Parse(args[0]))
		leftRefURI := uri.Must(uri.Parse(args[1]))

		if leftRefURI.Repository != rightRefURI.Repository {
			DieFmt("both references must belong to the same repository")
		}

		success, conflicts, err := client.Merge(context.Background(), leftRefURI.Repository, leftRefURI.Ref, rightRefURI.Ref)
		if success != nil {
			_, _ = os.Stdout.WriteString(fmt.Sprintf("new: %d modified: %d removed: %d \n", success.Created, success.Updated, success.Removed))
		} else if conflicts != nil {
			for _, line := range conflicts {
				FmtMerge(line)
			}
		} else {
			DieErr(err)
		}

	},
}

func FmtMerge(diff *models.MergeConflict) {
	var color text.Color
	var action string

	switch diff.Type {
	case models.DiffTypeADDED:
		color = text.FgGreen
		action = "+ added"
	case models.DiffTypeREMOVED:
		color = text.FgRed
		action = "- removed"
	default:
		color = text.FgYellow
		action = "~ modified"
	}

	_, _ = os.Stdout.WriteString(
		color.Sprintf("%s %s \n", action, diff.Path),
	)
}

func init() {
	rootCmd.AddCommand(mergeCmd)

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// mergeCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// mergeCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}
