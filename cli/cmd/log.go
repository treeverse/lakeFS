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

var commitsTemplate = `{{ range $val := . }}
{{ if gt  ($val.Parents|len) 0 -}}
commit {{ $val.ID|yellow }}
Author: {{ $val.Committer }}
Date: {{ $val.CreationDate|date }}
{{ if gt ($val.Parents|len) 1 -}}
Merge: {{ $val.Parents|join ", "|bold }}
{{ end }}

    {{ $val.Message }}

    {{ range $key, $value := $val.Metadata }}
    {{ $key }} = {{ $value }}
	{{ end -}}
{{ end -}}
{{ end }}
`

// logCmd represents the log command
var logCmd = &cobra.Command{
	Use:   "log [branch uri]",
	Short: "show log of commits for the given branch",
	Args: ValidationChain(
		HasNArgs(1),
		IsBranchURI(0),
	),
	Run: func(cmd *cobra.Command, args []string) {
		client := getClient()
		branchURI := uri.Must(uri.Parse(args[0]))
		commits, err := client.GetCommitLog(context.Background(), branchURI.Repository, branchURI.Refspec)
		if err != nil {
			DieErr(err)
		}
		Write(commitsTemplate, commits)
	},
}

func init() {
	rootCmd.AddCommand(logCmd)
}
