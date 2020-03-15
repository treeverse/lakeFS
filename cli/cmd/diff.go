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
	"os"
	"strings"

	"github.com/jedib0t/go-pretty/text"

	"github.com/treeverse/lakefs/api/gen/models"

	"github.com/spf13/cobra"
	"github.com/treeverse/lakefs/uri"
)

var diffCmd = &cobra.Command{
	Use:   "diff [ref uri] <other ref uri>",
	Short: "diff between commits/hashes",
	Long:  "see the list of paths added/changed/removed in a branch or between two references (could be either commit hash or branch name)",
	Args: ValidationChain(
		HasRangeArgs(1, 2),
		IsRefURI(0),
	),
	Run: func(cmd *cobra.Command, args []string) {
		client := getClient()

		var diff []*models.Diff
		var err error
		if len(args) == 2 {
			if err := IsRefURI(1)(args); err != nil {
				DieErr(err)
			}
			leftRefURI := uri.Must(uri.Parse(args[0]))
			rightRefURI := uri.Must(uri.Parse(args[1]))

			if leftRefURI.Repository != rightRefURI.Repository {
				DieFmt("both references must belong to the same repository")
			}

			diff, err = client.DiffRefs(context.Background(), leftRefURI.Repository, leftRefURI.Ref, rightRefURI.Ref)
			if err != nil {
				DieErr(err)
			}
			for _, line := range diff {
				FmtDiff(line, true)
			}
		} else {
			branchURI := uri.Must(uri.Parse(args[0]))
			diff, err = client.DiffBranch(context.Background(), branchURI.Repository, branchURI.Ref)
			if err != nil {
				DieErr(err)
			}
			for _, line := range diff {
				FmtDiff(line, false)
			}
		}
	},
}

func FmtDiff(diff *models.Diff, withDirection bool) {
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

	var direction string
	switch diff.Direction {
	case models.DiffDirectionLEFT:
		direction = "<"
	case models.DiffDirectionRIGHT:
		direction = ">"
	default:
		direction = "*"
	}

	if !withDirection {
		_, _ = os.Stdout.WriteString(
			color.Sprintf("%s %s %s\n",
				action, strings.ToLower(diff.PathType), diff.Path),
		)
		return
	}

	_, _ = os.Stdout.WriteString(
		color.Sprintf("%s %s %s %s\n",
			direction, action, strings.ToLower(diff.PathType), diff.Path),
	)
}

func init() {
	rootCmd.AddCommand(diffCmd)
}
