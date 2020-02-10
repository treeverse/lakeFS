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
	"strings"

	"github.com/treeverse/lakefs/uri"

	"github.com/spf13/cobra"
)

// commitCmd represents the commit command
// lakectl commit lakefs://myrepo@master --message "commit message"
var commitCmd = &cobra.Command{
	Use:   "commit [branch uri]",
	Short: "commit changes on a given branch",
	Args: ValidationChain(
		HasNArgs(1),
		IsBranchURI(0),
	),
	RunE: func(cmd *cobra.Command, args []string) error {
		// validate message
		kvPairs, err := getKV(cmd, "meta")
		if err != nil {
			return err
		}
		message, err := cmd.Flags().GetString("message")
		if err != nil {
			return err
		}
		branchUri := uri.Must(uri.Parse(args[0]))

		// do commit
		client, err := getClient()
		if err != nil {
			return err
		}

		commit, err := client.Commit(context.Background(), branchUri.Repository, branchUri.Refspec, message, kvPairs)
		if err != nil {
			return err
		}

		fmt.Printf("Commit for branch '%s' done:\nID: %s\ntimestamp: %d\nparents: %s\n",
			branchUri.Refspec, commit.ID, commit.CreationDate, strings.Join(commit.Parents, ", "))

		return nil
	},
}

func getKV(cmd *cobra.Command, name string) (map[string]string, error) {
	kvList, err := cmd.Flags().GetStringSlice(name)
	fmt.Printf("kv: %+v\n", kvList)
	if err != nil {
		return nil, err
	}
	kv := make(map[string]string)
	for _, pair := range kvList {
		parts := strings.SplitN(pair, "=", 2)
		if len(parts) != 2 {
			return nil, fmt.Errorf("invalid key/value pair - should be seperated by \"=\"")
		}
		kv[parts[0]] = parts[1]
	}
	return kv, nil
}

func init() {
	rootCmd.AddCommand(commitCmd)

	commitCmd.Flags().StringP("message", "m", "", "commit message")
	_ = commitCmd.MarkFlagRequired("message")

	commitCmd.Flags().StringSlice("meta", []string{}, "key value pair in the form of key=value")
}
