package cmd

import (
	"fmt"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/treeverse/lakefs/pkg/config"
	"github.com/treeverse/lakefs/pkg/metastore"
	"github.com/treeverse/lakefs/pkg/metastore/glue"
	"github.com/treeverse/lakefs/pkg/metastore/hive"
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
		fromDB, _ := cmd.Flags().GetString("from-schema")
		fromTable, _ := cmd.Flags().GetString("from-table")
		toDB, _ := cmd.Flags().GetString("to-schema")
		toTable, _ := cmd.Flags().GetString("to-table")
		toBranch, _ := cmd.Flags().GetString("to-branch")
		serde, _ := cmd.Flags().GetString("serde")
		partition, _ := cmd.Flags().GetStringSlice("partition")

		var client metastoreClient
		var err error
		msType := config.GetMetastoreType()
		switch msType {
		case "hive":
			hiveClient, err := hive.NewMSClient(nil, config.GetMetastoreHiveURI(), false)
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
			client, err = glue.NewMSClient(nil, config.GetMetastoreAwsConfig(), config.GetMetastoreGlueCatalogID())
			if err != nil {
				DieErr(err)
			}

		case "default":
			Die("unknown type, expected hive or glue got: "+msType, 1)
		}
		if len(toDB) == 0 {
			toDB = toBranch
		}
		if len(toTable) == 0 {
			toTable = fromTable
		}
		if len(serde) == 0 {
			serde = toTable
		}
		fmt.Printf("copy %s %s.%s -> %s.%s\n", msType, fromDB, fromTable, toDB, toTable)
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
		fromDB, _ := cmd.Flags().GetString("from-schema")
		fromTable, _ := cmd.Flags().GetString("from-table")
		toDB, _ := cmd.Flags().GetString("to-schema")
		toTable, _ := cmd.Flags().GetString("to-table")

		var diff *metastore.MetaDiff
		var client metastoreClient
		var err error
		msType := config.GetMetastoreType()
		switch msType {
		case "hive":
			var hiveClient *hive.MSClient
			hiveClient, err = hive.NewMSClient(cmd.Context(), config.GetMetastoreHiveURI(), false)
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
			client, err = glue.NewMSClient(cmd.Context(), config.GetMetastoreAwsConfig(), config.GetMetastoreGlueCatalogID())
			if err != nil {
				DieErr(err)
			}
		default:
			DieFmt("unknown type: %s, valid values are: glue, hive", msType)
		}

		if len(toDB) == 0 {
			toDB = fromDB
		}
		if len(toTable) == 0 {
			toTable = fromTable
		}
		diff, err = client.Diff(fromDB, fromTable, toDB, toTable)
		if err != nil {
			DieErr(err)
		}
		fmt.Printf("%s.%s <--> %s.%s\n", fromDB, fromTable, toDB, toTable)
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
	Long:  "create table with symlinks, and create the symlinks in s3 in order to access from external services that could only access s3 directly (e.g athena)",
	Run: func(cmd *cobra.Command, args []string) {
		repo, _ := cmd.Flags().GetString("repo")
		branch, _ := cmd.Flags().GetString("branch")
		path, _ := cmd.Flags().GetString("path")
		fromDB, _ := cmd.Flags().GetString("from-schema")
		fromTable, _ := cmd.Flags().GetString("from-table")
		toDB, _ := cmd.Flags().GetString("to-schema")
		toTable, _ := cmd.Flags().GetString("to-table")

		client := getClient()
		location, err := client.Symlink(cmd.Context(), repo, branch, path)
		if err != nil {
			DieErr(err)
		}
		msClient, err := glue.NewMSClient(nil, config.GetMetastoreAwsConfig(), config.GetMetastoreGlueCatalogID())
		if err != nil {
			DieErr(err)
		}
		err = msClient.CopyOrMergeToSymlink(fromDB, fromTable, toDB, toTable, location)
		if err != nil {
			DieErr(err)
		}
	},
}

//nolint:gochecknoinits
func init() {
	rootCmd.AddCommand(metastoreCmd)
	metastoreCmd.AddCommand(metastoreCopyCmd)
	_ = metastoreCopyCmd.Flags().String("type", "", "metastore type [hive, glue]")
	_ = viper.BindPFlag(config.MetaStoreType, metastoreCopyCmd.Flag("type"))
	_ = metastoreCopyCmd.Flags().String("metastore-uri", "", "Hive metastore URI")
	_ = viper.BindPFlag(config.MetaStoreHiveURI, metastoreCopyCmd.Flag("metastore-uri"))
	_ = metastoreCopyCmd.Flags().String("catalog-id", "", "Glue catalog ID")
	_ = viper.BindPFlag(config.MetastoreGlueCatalogID, metastoreCopyCmd.Flag("catalog-id"))
	_ = metastoreCopyCmd.Flags().String("from-schema", "", "source schema name")
	_ = metastoreCopyCmd.MarkFlagRequired("from-schema")
	_ = metastoreCopyCmd.Flags().String("from-table", "", "source table name")
	_ = metastoreCopyCmd.MarkFlagRequired("from-table")
	_ = metastoreCopyCmd.Flags().String("to-schema", "", "destination schema name [default is from-branch]")
	_ = metastoreCopyCmd.Flags().String("to-table", "", "destination table name [default is  from-table] ")
	_ = metastoreCopyCmd.Flags().String("to-branch", "", "lakeFS branch name")
	_ = metastoreCopyCmd.MarkFlagRequired("to-branch")
	_ = metastoreCopyCmd.Flags().String("serde", "", "serde to set copy to  [default is  to-table]")
	_ = metastoreCopyCmd.Flags().StringSliceP("partition", "p", nil, "partition to copy")

	metastoreCmd.AddCommand(metastoreDiffCmd)
	_ = metastoreDiffCmd.Flags().String("type", "", "metastore type [hive, glue]")
	_ = viper.BindPFlag(config.MetaStoreType, metastoreDiffCmd.Flag("type"))
	_ = metastoreDiffCmd.Flags().String("metastore-uri", "", "Hive metastore URI")
	_ = viper.BindPFlag(config.MetaStoreHiveURI, metastoreDiffCmd.Flag("metastore-uri"))
	_ = metastoreDiffCmd.Flags().String("catalog-id", "", "Glue catalog ID")
	_ = viper.BindPFlag(config.MetastoreGlueCatalogID, metastoreDiffCmd.Flag("catalog-id"))
	_ = metastoreDiffCmd.Flags().String("from-schema", "", "source schema name")
	_ = metastoreDiffCmd.MarkFlagRequired("from-schema")
	_ = metastoreDiffCmd.Flags().String("from-table", "", "source table name")
	_ = metastoreDiffCmd.MarkFlagRequired("from-table")
	_ = metastoreDiffCmd.Flags().String("to-schema", "", "destination schema name ")
	_ = metastoreDiffCmd.Flags().String("to-table", "", "destination table name [default is from-table]")

	metastoreCmd.AddCommand(glueSymlinkCmd)
	_ = glueSymlinkCmd.Flags().String("repo", "", "lakeFS repository name")
	_ = glueSymlinkCmd.MarkFlagRequired("repo")
	_ = glueSymlinkCmd.Flags().String("branch", "", "lakeFS branch name")
	_ = glueSymlinkCmd.MarkFlagRequired("branch")
	_ = glueSymlinkCmd.Flags().String("path", "", "path to table on lakeFS")
	_ = glueSymlinkCmd.MarkFlagRequired("path")
	_ = glueSymlinkCmd.Flags().String("catalog-id", "", "Glue catalog ID")
	_ = viper.BindPFlag(config.MetastoreGlueCatalogID, glueSymlinkCmd.Flag("catalog-id"))
	_ = glueSymlinkCmd.Flags().String("from-schema", "", "source schema name")
	_ = glueSymlinkCmd.MarkFlagRequired("from-schema")
	_ = glueSymlinkCmd.Flags().String("from-table", "", "source table name")
	_ = glueSymlinkCmd.MarkFlagRequired("from-table")
	_ = glueSymlinkCmd.Flags().String("to-schema", "", "destination schema name")
	_ = glueSymlinkCmd.MarkFlagRequired("to-schema")
	_ = glueSymlinkCmd.Flags().String("to-table", "", "destination table name")
	_ = glueSymlinkCmd.MarkFlagRequired("to-table")
}
