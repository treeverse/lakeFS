package cmd

import (
	"context"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/spf13/viper"

	"github.com/treeverse/lakefs/metastore/glueClient"

	"github.com/treeverse/lakefs/metastore/hive"

	"github.com/spf13/cobra"
)

var metastoreCmd = &cobra.Command{
	Use:   "metastore",
	Short: "manage metastore commands",
}

// hive commands

var hiveCopyCmd = &cobra.Command{
	Use:   "hive-copy",
	Short: "copy table to new table with different locations",
	Run: func(cmd *cobra.Command, args []string) {
		address, _ := cmd.Flags().GetString("address")
		fromDB, _ := cmd.Flags().GetString("from-schema")
		fromTable, _ := cmd.Flags().GetString("from-table")
		fromBranch, _ := cmd.Flags().GetString("from-branch")
		toDB, _ := cmd.Flags().GetString("to-schema")
		toTable, _ := cmd.Flags().GetString("to-table")
		toBranch, _ := cmd.Flags().GetString("to-branch")
		serde, _ := cmd.Flags().GetString("serde")
		partition, _ := cmd.Flags().GetStringArray("partition")

		hiveClientWrapper := hive.NewHiveClientWrapper(address, false)
		err := hiveClientWrapper.Open()
		if err != nil {
			DieErr(err)
		}
		defer func() {
			err = hiveClientWrapper.Close()
			if err != nil {
				DieErr(err)
			}
		}()

		msClient := hive.NewMetastoreClient(context.Background(), hiveClientWrapper.GetClient())
		if len(partition) > 0 {
			err = msClient.CopyPartition(fromDB, fromTable, fromBranch, toDB, toTable, toBranch, partition)
		} else {
			err = msClient.CopyOrMerge(fromDB, fromTable, fromBranch, toDB, toTable, toBranch, serde)
		}
		if err != nil {
			DieErr(err)
		}
	},
}

var hiveDiffCmd = &cobra.Command{
	Use:   "hive-diff",
	Short: "show column and partition differences between two tables",
	Run: func(cmd *cobra.Command, args []string) {
		address, _ := cmd.Flags().GetString("address")
		fromDB, _ := cmd.Flags().GetString("from-schema")
		fromTable, _ := cmd.Flags().GetString("from-table")
		toDB, _ := cmd.Flags().GetString("to-schema")
		toTable, _ := cmd.Flags().GetString("to-table")
		hiveClientWrapper := hive.NewHiveClientWrapper(address, false)
		err := hiveClientWrapper.Open()
		if err != nil {
			DieErr(err)
		}
		defer func() {
			err = hiveClientWrapper.Close()
			if err != nil {
				DieErr(err)
			}
		}()
		msClient := hive.NewMetastoreClient(context.Background(), hiveClientWrapper.GetClient())

		diff, err := msClient.Diff(fromDB, fromTable, toDB, toTable)
		if err != nil {
			DieErr(err)
		}
		if len(diff.ColumnsDiff) == 0 {
			println("Columns are Identical")
		} else {
			println("Columns")
			for _, column := range diff.ColumnsDiff {
				println(column.String())
			}
		}
		if len(diff.PartitionDiff) == 0 {
			println("Partitions are Identical")
		} else {
			println("Partitions")
			for _, partition := range diff.PartitionDiff {
				println(partition.String())
			}
		}
	},
}

//glue commands
func getGlueCfg() *aws.Config {
	cfg := &aws.Config{
		Region: aws.String(viper.GetString("metastore.s3.region")),
		//Logger: &config.LogrusAWSAdapter{},
	}
	if viper.IsSet("metastore.s3.profile") || viper.IsSet("metastore.s3.credentials_file") {
		cfg.Credentials = credentials.NewSharedCredentials(
			viper.GetString("metastore.s3.credentials_file"),
			viper.GetString("metastore.s3.profile"))
	}
	if viper.IsSet("metastore.s3.credentials") {
		cfg.Credentials = credentials.NewStaticCredentials(
			viper.GetString("metastore.s3.credentials.access_key_id"),
			viper.GetString("metastore.s3.credentials.access_secret_key"),
			viper.GetString("metastore.s3.credentials.session_token"))
	}
	return cfg
}

var glueCopyCmd = &cobra.Command{
	Use:   "glue-copy",
	Short: "copy table to new table with different locations",
	Run: func(cmd *cobra.Command, args []string) {
		catalog, _ := cmd.Flags().GetString("address")
		fromDB, _ := cmd.Flags().GetString("from-schema")
		fromTable, _ := cmd.Flags().GetString("from-table")
		fromBranch, _ := cmd.Flags().GetString("from-branch")
		toDB, _ := cmd.Flags().GetString("to-schema")
		toTable, _ := cmd.Flags().GetString("to-table")
		toBranch, _ := cmd.Flags().GetString("to-branch")
		partition, _ := cmd.Flags().GetStringArray("partition")

		//copy := glueClient.GlueMSCopy{}.init (context.Background(), address, false)

		svc := glueClient.GetGlueService(getGlueCfg())
		msClient := glueClient.NewGlueMSClient(svc, catalog)
		var err error
		if len(partition) > 0 {
			err = msClient.CopyPartition(fromDB, fromTable, fromBranch, toDB, toTable, toBranch, partition)
		} else {
			err = msClient.CopyOrMerge(fromDB, fromTable, fromBranch, toDB, toTable, toBranch)
		}
		if err != nil {
			DieErr(err)
		}
	},
}

var glueDiffCmd = &cobra.Command{
	Use:   "glue-diff",
	Short: "show column and partition differences between two tables",
	Run: func(cmd *cobra.Command, args []string) {
		catalogID, _ := cmd.Flags().GetString("address")
		fromDB, _ := cmd.Flags().GetString("from-schema")
		fromTable, _ := cmd.Flags().GetString("from-table")
		toDB, _ := cmd.Flags().GetString("to-schema")
		toTable, _ := cmd.Flags().GetString("to-table")

		svc := glueClient.GetGlueService(getGlueCfg())
		msClient := glueClient.NewGlueMSClient(svc, catalogID)

		diff, err := msClient.Diff(fromDB, fromTable, toDB, toTable)
		if err != nil {
			DieErr(err)
		}

		if len(diff.ColumnsDiff) == 0 {
			println("Columns are Identical")
		} else {
			println("Columns")
			for _, column := range diff.ColumnsDiff {
				println(column.String())
			}
		}
		if len(diff.PartitionDiff) == 0 {
			println("Partitions are Identical")
		} else {
			println("Partitions")
			for _, partition := range diff.PartitionDiff {
				println(partition.String())
			}
		}
	},
}

var glueSymlinkCmd = &cobra.Command{
	Use:   "glue-create-symlink",
	Short: "create symlink table and data",
	Long:  "create table with symlinks, and create the symlinks in s3 in order to access from external devices that could only access s3 directly (e.g athena)",
	Run: func(cmd *cobra.Command, args []string) {
		client := getClient()

		repo, _ := cmd.Flags().GetString("repo")
		branch, _ := cmd.Flags().GetString("branch")
		path, _ := cmd.Flags().GetString("path")
		address, _ := cmd.Flags().GetString("address")
		fromDB, _ := cmd.Flags().GetString("from-schema")
		fromTable, _ := cmd.Flags().GetString("from-table")
		toDB, _ := cmd.Flags().GetString("to-schema")
		toTable, _ := cmd.Flags().GetString("to-table")

		location, err := client.Symlink(context.Background(), repo, branch, path)
		if err != nil {
			DieErr(err)
		}
		svc := glueClient.GetGlueService(getGlueCfg())
		msClient := glueClient.NewGlueMSClient(svc, address)
		err = msClient.CopyOrMergeToSymlink(fromDB, fromTable, toDB, toTable, location)
		if err != nil {
			DieErr(err)
		}
	},
}

func init() {
	rootCmd.AddCommand(metastoreCmd)
	metastoreCmd.AddCommand(hiveCopyCmd)
	_ = hiveCopyCmd.Flags().String("address", "", "hive metastore address")
	_ = hiveCopyCmd.MarkFlagRequired("address")
	_ = hiveCopyCmd.Flags().String("from-schema", "", "schema where orig table exists")
	_ = hiveCopyCmd.MarkFlagRequired("from-schema")
	_ = hiveCopyCmd.Flags().String("from-table", "", "table where orig table exists")
	_ = hiveCopyCmd.MarkFlagRequired("from-table")
	_ = hiveCopyCmd.Flags().String("from-branch", "", "branch containing the orig data")
	_ = hiveCopyCmd.MarkFlagRequired("from-branch")
	_ = hiveCopyCmd.Flags().String("to-schema", "", "shcema to copy to")
	_ = hiveCopyCmd.MarkFlagRequired("to-schema")
	_ = hiveCopyCmd.Flags().String("to-table", "", "new table name")
	_ = hiveCopyCmd.MarkFlagRequired("to-table")
	_ = hiveCopyCmd.Flags().String("to-branch", "", "branch containing the data")
	_ = hiveCopyCmd.MarkFlagRequired("to-branch")
	_ = hiveCopyCmd.Flags().String("serde", "", "serde to set copy to  [default is - to-table]")
	_ = hiveCopyCmd.Flags().StringArray("partition", nil, "partition to copy")

	metastoreCmd.AddCommand(hiveDiffCmd)
	_ = hiveDiffCmd.Flags().String("address", "", "hive metastore address")
	_ = hiveDiffCmd.MarkFlagRequired("address")
	_ = hiveDiffCmd.Flags().String("from-schema", "", "schema where orig table exists")
	_ = hiveDiffCmd.MarkFlagRequired("from-schema")
	_ = hiveDiffCmd.Flags().String("from-table", "", "table where orig table exists")
	_ = hiveDiffCmd.MarkFlagRequired("from-table")
	_ = hiveDiffCmd.Flags().String("to-schema", "", "shcema to copy to")
	_ = hiveDiffCmd.MarkFlagRequired("to-schema")
	_ = hiveDiffCmd.Flags().String("to-table", "", "new table name")
	_ = hiveDiffCmd.MarkFlagRequired("to-table")

	metastoreCmd.AddCommand(glueCopyCmd)
	_ = glueCopyCmd.Flags().String("address", "", "hive metastore address")
	_ = glueCopyCmd.MarkFlagRequired("address")
	_ = glueCopyCmd.Flags().String("from-schema", "", "schema where orig table exists")
	_ = glueCopyCmd.MarkFlagRequired("from-schema")
	_ = glueCopyCmd.Flags().String("from-table", "", "table where orig table exists")
	_ = glueCopyCmd.MarkFlagRequired("from-table")
	_ = glueCopyCmd.Flags().String("from-branch", "", "branch containing the orig data")
	_ = glueCopyCmd.MarkFlagRequired("from-branch")
	_ = glueCopyCmd.Flags().String("to-schema", "", "shcema to copy to")
	_ = glueCopyCmd.MarkFlagRequired("to-schema")
	_ = glueCopyCmd.Flags().String("to-table", "", "new table name")
	_ = glueCopyCmd.MarkFlagRequired("to-table")
	_ = glueCopyCmd.Flags().String("to-branch", "", "branch containing the data")
	_ = glueCopyCmd.MarkFlagRequired("to-branch")
	_ = glueCopyCmd.Flags().String("serde", "", "serde to set copy to  [default is - to-table]")
	_ = glueCopyCmd.Flags().StringArray("partition", nil, "partition to copy")

	metastoreCmd.AddCommand(glueDiffCmd)
	_ = glueDiffCmd.Flags().String("address", "", "hive metastore address")
	_ = glueDiffCmd.MarkFlagRequired("address")
	_ = glueDiffCmd.Flags().String("from-schema", "", "schema where orig table exists")
	_ = glueDiffCmd.MarkFlagRequired("from-schema")
	_ = glueDiffCmd.Flags().String("from-table", "", "table where orig table exists")
	_ = glueDiffCmd.MarkFlagRequired("from-table")
	_ = glueDiffCmd.Flags().String("to-schema", "", "shcema to copy to")
	_ = glueDiffCmd.MarkFlagRequired("to-schema")
	_ = glueDiffCmd.Flags().String("to-table", "", "new table name")
	_ = glueDiffCmd.MarkFlagRequired("to-table")

	metastoreCmd.AddCommand(glueSymlinkCmd)
	_ = glueSymlinkCmd.Flags().String("repo", "", "hive metastore address")
	_ = glueSymlinkCmd.MarkFlagRequired("repo")
	_ = glueSymlinkCmd.Flags().String("branch", "", "schema where orig table exists")
	_ = glueSymlinkCmd.MarkFlagRequired("branch")
	_ = glueSymlinkCmd.Flags().String("path", "", "table where orig table exists")
	_ = glueSymlinkCmd.MarkFlagRequired("path")
	_ = glueSymlinkCmd.Flags().String("address", "", "hive metastore address")
	_ = glueSymlinkCmd.MarkFlagRequired("address")
	_ = glueSymlinkCmd.Flags().String("from-schema", "", "schema where orig table exists")
	_ = glueSymlinkCmd.MarkFlagRequired("from-schema")
	_ = glueSymlinkCmd.Flags().String("from-table", "", "table where orig table exists")
	_ = glueSymlinkCmd.MarkFlagRequired("from-table")
	_ = glueSymlinkCmd.Flags().String("to-schema", "", "shcema to copy to")
	_ = glueSymlinkCmd.MarkFlagRequired("to-schema")
	_ = glueSymlinkCmd.Flags().String("to-table", "", "new table name")
	_ = glueSymlinkCmd.MarkFlagRequired("to-table")

}
