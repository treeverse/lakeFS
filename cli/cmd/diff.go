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
	"strings"

	"github.com/jedib0t/go-pretty/text"

	"github.com/treeverse/lakefs/api/gen/models"

	"github.com/spf13/cobra"
	"github.com/treeverse/lakefs/uri"
)

// diffCmd represents the diff command
var diffCmd = &cobra.Command{
	Use:   "diff [branch uri] <other branch uri>",
	Short: "see the list of paths added/changed/removed in a branch or between two branches",
	Args: ValidationChain(
		HasRangeArgs(1, 2),
		IsBranchURI(0),
	),
	RunE: func(cmd *cobra.Command, args []string) error {
		client, err := getClient()
		if err != nil {
			return err
		}

		var diff []*models.Diff

		if len(args) == 2 {
			if err := IsBranchURI(1)(args); err != nil {
				return err
			}
			leftBranchURI := uri.Must(uri.Parse(args[0]))
			rightBranchURI := uri.Must(uri.Parse(args[1]))

			if leftBranchURI.Repository != rightBranchURI.Repository {
				return fmt.Errorf("both branches must belong to the same repository")
			}

			diff, err = client.DiffBranches(context.Background(), leftBranchURI.Repository, leftBranchURI.Refspec, rightBranchURI.Refspec)
			if err != nil {
				return err
			}
			for _, line := range diff {
				FmtDiff(line, true)
			}
		} else {
			branchURI := uri.Must(uri.Parse(args[0]))
			diff, err = client.DiffBranch(context.Background(), branchURI.Repository, branchURI.Refspec)
			if err != nil {
				return err
			}
			for _, line := range diff {
				FmtDiff(line, false)
			}
		}

		return nil
	},
}

func FmtDiff(diff *models.Diff, withDirection bool) {
	var color text.Color
	var action string

	switch diff.Type {
	case "ADDED":
		color = text.FgGreen
		action = "+ added"
	case "REMOVED":
		color = text.FgRed
		action = "- removed"
	default:
		color = text.FgYellow
		action = "~ modified"
	}

	var direction string
	switch diff.Direction {
	case "LEFT":
		direction = "<"
	case "RIGHT":
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
