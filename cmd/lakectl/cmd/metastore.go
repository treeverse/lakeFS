package cmd

import (
	"context"
	"fmt"

	"github.com/treeverse/lakefs/metastore"

	"github.com/treeverse/lakefs/config"

	"github.com/treeverse/lakefs/metastore/glue"

	"github.com/treeverse/lakefs/metastore/hive"

	"github.com/spf13/cobra"
)

var metastoreCmd = &cobra.Command{
	Use:   "metastore",
	Short: "manage metastore commands",
}

type metastoreClient interface {
	CopyOrMerge(fromDB, fromTable, toDB, toTable, toBranch, serde string, partition []string) error
	Diff(fromDB, fromTable, toDB, toTable string) (*metastore.MetaDiff, error)
}

var metastoreCopyCmd = &cobra.Command{
	Use:   "copy",
	Short: "copy or merge table",
	Long:  "copy or merge table. the destination table will point to the selected branch",
	Run: func(cmd *cobra.Command, args []string) {
		msType, _ := cmd.Flags().GetString("type")
		address, _ := cmd.Flags().GetString("address")
		fromDB, _ := cmd.Flags().GetString("from-schema")
		fromTable, _ := cmd.Flags().GetString("from-table")
		toDB, _ := cmd.Flags().GetString("to-schema")
		toTable, _ := cmd.Flags().GetString("to-table")
		toBranch, _ := cmd.Flags().GetString("to-branch")
		serde, _ := cmd.Flags().GetString("serde")
		partition, _ := cmd.Flags().GetStringArray("partition")

		if serde == "" {
			serde = toTable
		}
		var client metastoreClient
		var err error
		switch msType {
		case "hive":
			hiveClient, err := hive.NewMSClient(address, false)
			if err != nil {
				DieErr(err)
			}
			defer func() {
				err = hiveClient.Close()
				if err != nil {
					DieErr(err)
				}
			}()
			client = hiveClient

		case "glue":
			client, err = glue.NewMSClient(config.GetMetastoreAwsConfig(), address)
			if err != nil {
				DieErr(err)
			}

		case "default":
			DieErr(fmt.Errorf("unknown type, expected hive or glue got: %s", msType))
		}
		err = client.CopyOrMerge(fromDB, fromTable, toDB, toTable, toBranch, serde, partition)
		if err != nil {
			DieErr(err)
		}
	},
}

var metastoreDiffCmd = &cobra.Command{
	Use:   "diff",
	Short: "show column and partition differences between two tables",
	Run: func(cmd *cobra.Command, args []string) {
		msType, _ := cmd.Flags().GetString("type")
		address, _ := cmd.Flags().GetString("address")
		fromDB, _ := cmd.Flags().GetString("from-schema")
		fromTable, _ := cmd.Flags().GetString("from-table")
		toDB, _ := cmd.Flags().GetString("to-schema")
		toTable, _ := cmd.Flags().GetString("to-table")

		var diff *metastore.MetaDiff
		var client metastoreClient
		var err error
		switch msType {
		case "hive":
			var hiveClient *hive.MSClient
			hiveClient, err = hive.NewMSClient(address, false)
			if err != nil {
				DieErr(err)
			}
			defer func() {
				err = hiveClient.Close()
				if err != nil {
					DieErr(err)
				}
			}()
			client = hiveClient

		case "glue":
			client, err = glue.NewMSClient(config.GetMetastoreAwsConfig(), address)
			if err != nil {
				DieErr(err)
			}
		}

		diff, err = client.Diff(fromDB, fromTable, toDB, toTable)
		if err != nil {
			DieErr(err)
		}

		if len(diff.ColumnsDiff) == 0 {
			println("Columns are identical")
		} else {
			println("Columns")
			for _, column := range diff.ColumnsDiff {
				println(column.String())
			}
		}
		if len(diff.PartitionDiff) == 0 {
			println("Partitions are identical")
		} else {
			println("Partitions")
			for _, partition := range diff.PartitionDiff {
				println(partition.String())
			}
		}
	},
}

var glueSymlinkCmd = &cobra.Command{
	Use:   "create-symlink",
	Short: "create symlink table and data",
	Long:  "create table with symlinks, and create the symlinks in s3 in order to access from external devices that could only access s3 directly (e.g athena)",
	Run: func(cmd *cobra.Command, args []string) {
		repo, _ := cmd.Flags().GetString("repo")
		branch, _ := cmd.Flags().GetString("branch")
		path, _ := cmd.Flags().GetString("path")
		address, _ := cmd.Flags().GetString("address")
		fromDB, _ := cmd.Flags().GetString("from-schema")
		fromTable, _ := cmd.Flags().GetString("from-table")
		toDB, _ := cmd.Flags().GetString("to-schema")
		toTable, _ := cmd.Flags().GetString("to-table")

		client := getClient()
		location, err := client.Symlink(context.Background(), repo, branch, path)
		if err != nil {
			DieErr(err)
		}
		msClient, err := glue.NewMSClient(config.GetMetastoreAwsConfig(), address)
		if err != nil {
			DieErr(err)
		}
		err = msClient.CopyOrMergeToSymlink(fromDB, fromTable, toDB, toTable, location)
		if err != nil {
			DieErr(err)
		}
	},
}

func init() {
	rootCmd.AddCommand(metastoreCmd)
	metastoreCmd.AddCommand(metastoreCopyCmd)
	_ = metastoreCopyCmd.Flags().String("type", "", "metastore type [hive, glue]")
	_ = metastoreCopyCmd.MarkFlagRequired("type")
	_ = metastoreCopyCmd.Flags().String("address", "", "Hive metastore address/ glue catalog ID")
	_ = metastoreCopyCmd.MarkFlagRequired("address")
	_ = metastoreCopyCmd.Flags().String("from-schema", "", "source schema name")
	_ = metastoreCopyCmd.MarkFlagRequired("from-schema")
	_ = metastoreCopyCmd.Flags().String("from-table", "", "source table name")
	_ = metastoreCopyCmd.MarkFlagRequired("from-table")
	_ = metastoreCopyCmd.Flags().String("to-schema", "", "destination schema name")
	_ = metastoreCopyCmd.MarkFlagRequired("to-schema")
	_ = metastoreCopyCmd.Flags().String("to-table", "", "destination table name")
	_ = metastoreCopyCmd.MarkFlagRequired("to-table")
	_ = metastoreCopyCmd.Flags().String("to-branch", "", "lakeFS branch name")
	_ = metastoreCopyCmd.MarkFlagRequired("to-branch")
	_ = metastoreCopyCmd.Flags().String("serde", "", "serde to set copy to  [default is - to-table]")
	_ = metastoreCopyCmd.Flags().StringArray("partition", nil, "partition to copy")

	metastoreCmd.AddCommand(metastoreDiffCmd)
	_ = metastoreDiffCmd.Flags().String("type", "", "metastore type [hive, Glue]")
	_ = metastoreDiffCmd.MarkFlagRequired("type")
	_ = metastoreDiffCmd.Flags().String("address", "", "Hive metastore address/ Glue catalog ID")
	_ = metastoreDiffCmd.MarkFlagRequired("address")
	_ = metastoreDiffCmd.Flags().String("from-schema", "", "source schema name")
	_ = metastoreDiffCmd.MarkFlagRequired("from-schema")
	_ = metastoreDiffCmd.Flags().String("from-table", "", "source table name")
	_ = metastoreDiffCmd.MarkFlagRequired("from-table")
	_ = metastoreDiffCmd.Flags().String("to-schema", "", "destination schema name")
	_ = metastoreDiffCmd.MarkFlagRequired("to-schema")
	_ = metastoreDiffCmd.Flags().String("to-table", "", "destination table name")
	_ = metastoreDiffCmd.MarkFlagRequired("to-table")

	metastoreCmd.AddCommand(glueSymlinkCmd)
	_ = glueSymlinkCmd.Flags().String("repo", "", "lakeFS repository name")
	_ = glueSymlinkCmd.MarkFlagRequired("repo")
	_ = glueSymlinkCmd.Flags().String("branch", "", "lakeFS branch name")
	_ = glueSymlinkCmd.MarkFlagRequired("branch")
	_ = glueSymlinkCmd.Flags().String("path", "", "path to table on lakeFS")
	_ = glueSymlinkCmd.MarkFlagRequired("path")
	_ = glueSymlinkCmd.Flags().String("address", "", "Glue catalog ID")
	_ = glueSymlinkCmd.MarkFlagRequired("address")
	_ = glueSymlinkCmd.Flags().String("from-schema", "", "source schema name")
	_ = glueSymlinkCmd.MarkFlagRequired("from-schema")
	_ = glueSymlinkCmd.Flags().String("from-table", "", "source table name")
	_ = glueSymlinkCmd.MarkFlagRequired("from-table")
	_ = glueSymlinkCmd.Flags().String("to-schema", "", "destination schema name")
	_ = glueSymlinkCmd.MarkFlagRequired("to-schema")
	_ = glueSymlinkCmd.Flags().String("to-table", "", "destination table name")
	_ = glueSymlinkCmd.MarkFlagRequired("to-table")

}
