package cmd

import (
	"context"

	"github.com/treeverse/lakefs/metastore"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/spf13/viper"

	"github.com/treeverse/lakefs/metastore/glue"

	"github.com/treeverse/lakefs/metastore/hive"

	"github.com/spf13/cobra"
)

var metastoreCmd = &cobra.Command{
	Use:   "metastore",
	Short: "manage metastore commands",
}

// hive commands

var metastoreCopyCmd = &cobra.Command{
	Use:   "copy",
	Short: "copy table to new table with different locations",
	Run: func(cmd *cobra.Command, args []string) {
		msType, _ := cmd.Flags().GetString("type")
		address, _ := cmd.Flags().GetString("address")
		fromDB, _ := cmd.Flags().GetString("from-schema")
		fromTable, _ := cmd.Flags().GetString("from-table")
		fromBranch, _ := cmd.Flags().GetString("from-branch")
		toDB, _ := cmd.Flags().GetString("to-schema")
		toTable, _ := cmd.Flags().GetString("to-table")
		toBranch, _ := cmd.Flags().GetString("to-branch")
		serde, _ := cmd.Flags().GetString("serde")
		partition, _ := cmd.Flags().GetStringArray("partition")

		if serde == "" {
			serde = toTable
		}

		switch msType {
		case "hive":
			hiveClient, err := hive.NewClient(address, false)
			if err != nil {
				DieErr(err)
			}
			defer func() {
				err = hiveClient.Close()
				if err != nil {
					DieErr(err)
				}
			}()
			err = hiveClient.CopyOrMerge(fromDB, fromTable, fromBranch, toDB, toTable, toBranch, serde, partition)
			if err != nil {
				DieErr(err)
			}

		case "glue":
			msClient, err := glue.NewClient(getGlueCfg(), address)
			if err != nil {
				DieErr(err)
			}
			if len(partition) > 0 {
				err = msClient.CopyPartition(fromDB, fromTable, fromBranch, toDB, toTable, toBranch, "", partition)
			} else {
				err = msClient.CopyOrMerge(fromDB, fromTable, fromBranch, toDB, toTable, toBranch, "", nil)
			}
			if err != nil {
				DieErr(err)
			}
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
		switch msType {
		case "hive":
			hiveClient, err := hive.NewClient(address, false)
			if err != nil {
				DieErr(err)
			}
			defer func() {
				err = hiveClient.Close()
				if err != nil {
					DieErr(err)
				}
			}()
			diff, err = hiveClient.Diff(fromDB, fromTable, toDB, toTable)
			if err != nil {
				DieErr(err)
			}

		case "glue":
			client, err := glue.NewClient(getGlueCfg(), address)
			if err != nil {
				DieErr(err)
			}
			diff, err = client.Diff(fromDB, fromTable, toDB, toTable)
			if err != nil {
				DieErr(err)
			}
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

var glueSymlinkCmd = &cobra.Command{
	Use:   "create-symlink",
	Short: "create symlink table and data",
	Long:  "create table with symlinks, and create the symlinks in s3 in order to access from external devices that could only access s3 directly (e.g athena)",
	Run: func(cmd *cobra.Command, args []string) {
		client := getClient()

		repo, _ := cmd.Flags().GetString("repo")
		branch, _ := cmd.Flags().GetString("branch")
		path, _ := cmd.Flags().GetString("path")
		address, _ := cmd.Flags().GetString("catalog-id")
		fromDB, _ := cmd.Flags().GetString("from-schema")
		fromTable, _ := cmd.Flags().GetString("from-table")
		toDB, _ := cmd.Flags().GetString("to-schema")
		toTable, _ := cmd.Flags().GetString("to-table")

		location, err := client.Symlink(context.Background(), repo, branch, path)
		if err != nil {
			DieErr(err)
		}
		svc := glue.GetGlueService(getGlueCfg())
		msClient := glue.NewGlueMSClient(svc, address)
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
	_ = metastoreCopyCmd.Flags().String("from-schema", "", "schema where orig table exists")
	_ = metastoreCopyCmd.MarkFlagRequired("from-schema")
	_ = metastoreCopyCmd.Flags().String("from-table", "", "table where orig table exists")
	_ = metastoreCopyCmd.MarkFlagRequired("from-table")
	_ = metastoreCopyCmd.Flags().String("from-branch", "", "branch containing the orig data")
	_ = metastoreCopyCmd.MarkFlagRequired("from-branch")
	_ = metastoreCopyCmd.Flags().String("to-schema", "", "shcema to copy to")
	_ = metastoreCopyCmd.MarkFlagRequired("to-schema")
	_ = metastoreCopyCmd.Flags().String("to-table", "", "new table name")
	_ = metastoreCopyCmd.MarkFlagRequired("to-table")
	_ = metastoreCopyCmd.Flags().String("to-branch", "", "branch containing the data")
	_ = metastoreCopyCmd.MarkFlagRequired("to-branch")
	_ = metastoreCopyCmd.Flags().String("serde", "", "serde to set copy to  [default is - to-table]")
	_ = metastoreCopyCmd.Flags().StringArray("partition", nil, "partition to copy")

	metastoreCmd.AddCommand(metastoreDiffCmd)
	_ = metastoreDiffCmd.Flags().String("type", "", "metastore type [hive, Glue]")
	_ = metastoreDiffCmd.MarkFlagRequired("type")
	_ = metastoreDiffCmd.Flags().String("address", "", "Hive metastore address/ Glue catalog ID")
	_ = metastoreDiffCmd.Flags().String("address", "", "Hive metastore address/ Glue catalog ID")
	_ = metastoreDiffCmd.MarkFlagRequired("address")
	_ = metastoreDiffCmd.Flags().String("from-schema", "", "schema where orig table exists")
	_ = metastoreDiffCmd.MarkFlagRequired("from-schema")
	_ = metastoreDiffCmd.Flags().String("from-table", "", "table where orig table exists")
	_ = metastoreDiffCmd.MarkFlagRequired("from-table")
	_ = metastoreDiffCmd.Flags().String("to-schema", "", "shcema to copy to")
	_ = metastoreDiffCmd.MarkFlagRequired("to-schema")
	_ = metastoreDiffCmd.Flags().String("to-table", "", "new table name")
	_ = metastoreDiffCmd.MarkFlagRequired("to-table")

	//metastoreCmd.AddCommand(glueDiffCmd)
	//_ = glueDiffCmd.Flags().String("catalog-id", "", "Glue catalog ID")
	//_ = glueDiffCmd.MarkFlagRequired("catalog-id")
	//_ = glueDiffCmd.Flags().String("from-schema", "", "schema where orig table exists")
	//_ = glueDiffCmd.MarkFlagRequired("from-schema")
	//_ = glueDiffCmd.Flags().String("from-table", "", "table where orig table exists")
	//_ = glueDiffCmd.MarkFlagRequired("from-table")
	//_ = glueDiffCmd.Flags().String("to-schema", "", "shcema to copy to")
	//_ = glueDiffCmd.MarkFlagRequired("to-schema")
	//_ = glueDiffCmd.Flags().String("to-table", "", "new table name")
	//_ = glueDiffCmd.MarkFlagRequired("to-table")

	metastoreCmd.AddCommand(glueSymlinkCmd)
	_ = glueSymlinkCmd.Flags().String("repo", "", "lakefs repository name")
	_ = glueSymlinkCmd.MarkFlagRequired("repo")
	_ = glueSymlinkCmd.Flags().String("branch", "", "lakefs branch name")
	_ = glueSymlinkCmd.MarkFlagRequired("branch")
	_ = glueSymlinkCmd.Flags().String("path", "", "table where orig table exists")
	_ = glueSymlinkCmd.MarkFlagRequired("path")
	_ = glueSymlinkCmd.Flags().String("catalog-id", "", "Glue catalog ID")
	_ = glueSymlinkCmd.MarkFlagRequired("catalog-id")
	_ = glueSymlinkCmd.Flags().String("from-schema", "", "schema where orig table exists")
	_ = glueSymlinkCmd.MarkFlagRequired("from-schema")
	_ = glueSymlinkCmd.Flags().String("from-table", "", "table where orig table exists")
	_ = glueSymlinkCmd.MarkFlagRequired("from-table")
	_ = glueSymlinkCmd.Flags().String("to-schema", "", "shcema to copy to")
	_ = glueSymlinkCmd.MarkFlagRequired("to-schema")
	_ = glueSymlinkCmd.Flags().String("to-table", "", "new table name")
	_ = glueSymlinkCmd.MarkFlagRequired("to-table")

}
