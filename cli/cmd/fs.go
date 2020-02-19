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
	"time"

	"github.com/treeverse/lakefs/api/gen/models"

	"github.com/spf13/cobra"
	"github.com/treeverse/lakefs/uri"
)

var fsStatCmd = &cobra.Command{
	Use:   "stat [path uri]",
	Short: "view object metadata",
	Args: ValidationChain(
		HasNArgs(1),
		IsPathURI(0),
	),
	RunE: func(cmd *cobra.Command, args []string) error {
		pathURI := uri.Must(uri.Parse(args[0]))

		client, err := getClient()
		if err != nil {
			return err
		}

		stat, err := client.StatObject(context.Background(), pathURI.Repository, pathURI.Refspec, pathURI.Path)
		if err != nil {
			return err
		}

		printStat(stat)

		return nil
	},
}

func printStat(stats *models.ObjectStats) {
	fmt.Printf("path: %s\nsize: %d bytes\nsize_human: %s\nmodified: %s\nchecksum: %s\n", stats.Path, stats.SizeBytes, byteCountDecimal(stats.SizeBytes), time.Unix(stats.Mtime, 0), stats.Checksum)
}

func byteCountDecimal(b int64) string {
	const unit = 1000
	if b < unit {
		return fmt.Sprintf("%d B", b)
	}
	div, exp := int64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(b)/float64(div), "kMGTPE"[exp])
}

// fsCmd represents the fs command
var fsCmd = &cobra.Command{
	Use:   "fs",
	Short: "view and manipulate objects",
}

func init() {
	rootCmd.AddCommand(fsCmd)
	fsCmd.AddCommand(fsStatCmd)
}
