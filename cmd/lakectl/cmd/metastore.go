package cmd

import (
	"fmt"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/treeverse/lakefs/pkg/api"
	"github.com/treeverse/lakefs/pkg/logging"
	"github.com/treeverse/lakefs/pkg/metastore"
	"github.com/treeverse/lakefs/pkg/metastore/glue"
	"github.com/treeverse/lakefs/pkg/metastore/hive"
)

var metastoreCmd = &cobra.Command{
	Use:   "metastore",
	Short: "manage metastore commands",
}

var metastoreCopyCmd = &cobra.Command{
	Use:   "copy",
	Short: "copy or merge table",
	Long:  "copy or merge table. the destination table will point to the selected branch",
	Run: func(cmd *cobra.Command, args []string) {
		fromClientType, _ := cmd.Flags().GetString("from-client-type")
		fromDB, _ := cmd.Flags().GetString("from-schema")
		fromTable, _ := cmd.Flags().GetString("from-table")
		toClientType, _ := cmd.Flags().GetString("to-client-type")
		toDB, _ := cmd.Flags().GetString("to-schema")
		toTable, _ := cmd.Flags().GetString("to-table")
		toBranch, _ := cmd.Flags().GetString("to-branch")
		serde, _ := cmd.Flags().GetString("serde")
		partition, _ := cmd.Flags().GetStringSlice("partition")

		fromClient, fromClientDeferFunc := getMetastoreClient(fromClientType, "")
		defer fromClientDeferFunc()
		toClient, toClientDeferFunc := getMetastoreClient(toClientType, "")
		defer toClientDeferFunc()
		if len(toDB) == 0 {
			toDB = toBranch
		}
		if len(toTable) == 0 {
			toTable = fromTable
		}
		if len(serde) == 0 {
			serde = toTable
		}
		logging.Default().WithFields(logging.Fields{
			"form_client_type": fromClientType,
			"from_schema":      fromDB,
			"from_table":       fromTable,
			"to_client_type":   toClientType,
			"to_schema":        toDB,
			"to_table":         toTable,
			"to_branch":        toBranch,
			"serde":            serde,
			"partition":        partition,
		}).Info("Metadata copy or merge table")
		fmt.Printf("copy %s.%s -> %s.%s\n", fromDB, fromTable, toDB, toTable)
		err := metastore.CopyOrMerge(cmd.Context(), fromClient, toClient, fromDB, fromTable, toDB, toTable, toBranch, serde, partition)
		if err != nil {
			DieErr(err)
		}
	},
}

func getMetastoreClient(msType, hiveAddress string) (metastore.Client, func()) {
	if msType == "" {
		msType = cfg.GetMetastoreType()
	}
	switch msType {
	case "hive":
		if len(hiveAddress) == 0 {
			hiveAddress = cfg.GetMetastoreHiveURI()
		}
		hiveClient, err := hive.NewMSClient(hiveAddress, false, cfg.GetHiveDBLocationURI())
		if err != nil {
			DieErr(err)
		}
		deferFunc := func() {
			err := hiveClient.Close()
			if err != nil {
				DieErr(err)
			}
		}
		return hiveClient, deferFunc

	case "glue":
		client, err := glue.NewMSClient(cfg.GetMetastoreAwsConfig(), cfg.GetMetastoreGlueCatalogID(), cfg.GetGlueDBLocationURI())
		if err != nil {
			DieErr(err)
		}
		return client, func() {}

	default:
		Die("unknown type, expected hive or glue got: "+msType, 1)
	}
	return nil, nil
}

var metastoreCopyAllCmd = &cobra.Command{
	Use:   "copy-all",
	Short: "copy from one metastore to another",
	Long:  "copy or merge requested tables between hive metastores. the destination tables will point to the selected branch",
	Run: func(cmd *cobra.Command, args []string) {
		fromClientType, _ := cmd.Flags().GetString("from-client-type")
		fromAddress, _ := cmd.Flags().GetString("from-address")
		toClientType, _ := cmd.Flags().GetString("to-client-type")
		toAddress, _ := cmd.Flags().GetString("to-address")
		schemaFilter, _ := cmd.Flags().GetString("schema-filter")
		tableFilter, _ := cmd.Flags().GetString("table-filter")
		branch, _ := cmd.Flags().GetString("branch")
		continueOnError, _ := cmd.Flags().GetBool("continue-on-error")

		if fromAddress == toAddress {
			Die("from-address must be different than to-address", 1)
		}

		fromClient, deferFunc := getMetastoreClient(fromClientType, fromAddress)
		defer deferFunc()
		toClient, toDeferFunc := getMetastoreClient(toClientType, toAddress)
		defer toDeferFunc()

		fmt.Printf("copy %s -> %s\n", fromAddress, toAddress)
		err := metastore.CopyOrMergeAll(cmd.Context(), fromClient, toClient, schemaFilter, tableFilter, branch, continueOnError)
		if err != nil {
			DieErr(err)
		}
	},
}

var metastoreDiffCmd = &cobra.Command{
	Use:   "diff",
	Short: "show column and partition differences between two tables",
	Run: func(cmd *cobra.Command, args []string) {
		fromClientType, _ := cmd.Flags().GetString("from-client-type")
		toAddress, _ := cmd.Flags().GetString("to-address")
		fromDB, _ := cmd.Flags().GetString("from-schema")
		fromTable, _ := cmd.Flags().GetString("from-table")
		toClientType, _ := cmd.Flags().GetString("to-client-type")
		fromAddress, _ := cmd.Flags().GetString("from-address")
		toDB, _ := cmd.Flags().GetString("to-schema")
		toTable, _ := cmd.Flags().GetString("to-table")

		msType := cfg.GetMetastoreType()
		if len(fromClientType) == 0 {
			fromClientType = msType
		}
		if len(toClientType) == 0 {
			toClientType = msType
		}
		fromClient, fromClientDeferFunc := getMetastoreClient(fromClientType, fromAddress)
		defer fromClientDeferFunc()
		toClient, toClientDeferFunc := getMetastoreClient(toClientType, toAddress)
		defer toClientDeferFunc()

		if len(toDB) == 0 {
			toDB = fromDB
		}
		if len(toTable) == 0 {
			toTable = fromTable
		}
		diff, err := metastore.GetDiff(cmd.Context(), fromClient, toClient, fromDB, fromTable, toDB, toTable)
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
		res, err := client.CreateSymlinkFileWithResponse(cmd.Context(), repo, branch, &api.CreateSymlinkFileParams{Location: &path})
		if err != nil {
			DieErr(err)
		}
		DieOnResponseError(res, err)
		location := res.JSON201.Location

		msClient, err := glue.NewMSClient(cfg.GetMetastoreAwsConfig(), cfg.GetMetastoreGlueCatalogID(), cfg.GetGlueDBLocationURI())
		if err != nil {
			DieErr(err)
		}

		err = metastore.CopyOrMergeToSymlink(cmd.Context(), msClient, fromDB, fromTable, toDB, toTable, location)
		if err != nil {
			DieErr(err)
		}
	},
}

//nolint:gochecknoinits
func init() {
	rootCmd.AddCommand(metastoreCmd)
	metastoreCmd.AddCommand(metastoreCopyCmd)
	_ = metastoreCopyCmd.Flags().String("from-client-type", "", "metastore type [hive, glue]")
	_ = metastoreCopyCmd.Flags().String("metastore-uri", "", "Hive metastore URI")
	_ = viper.BindPFlag("metastore.hive.uri", metastoreCopyCmd.Flag("metastore-uri"))
	_ = metastoreCopyCmd.Flags().String("catalog-id", "", "Glue catalog ID")
	_ = viper.BindPFlag("metastore.glue.catalog_id", metastoreCopyCmd.Flag("catalog-id"))
	_ = metastoreCopyCmd.Flags().String("from-schema", "", "source schema name")
	_ = metastoreCopyCmd.MarkFlagRequired("from-schema")
	_ = metastoreCopyCmd.Flags().String("from-table", "", "source table name")
	_ = metastoreCopyCmd.MarkFlagRequired("from-table")
	_ = metastoreCopyCmd.Flags().String("to-client-type", "", "metastore type [hive, glue]")
	_ = metastoreCopyCmd.Flags().String("to-schema", "", "destination schema name [default is from-branch]")
	_ = metastoreCopyCmd.Flags().String("to-table", "", "destination table name [default is  from-table] ")
	_ = metastoreCopyCmd.Flags().String("to-branch", "", "lakeFS branch name")
	_ = metastoreCopyCmd.MarkFlagRequired("to-branch")
	_ = metastoreCopyCmd.Flags().String("serde", "", "serde to set copy to  [default is  to-table]")
	_ = metastoreCopyCmd.Flags().StringSliceP("partition", "p", nil, "partition to copy")

	metastoreCmd.AddCommand(metastoreDiffCmd)
	_ = metastoreDiffCmd.Flags().String("from-client-type", "", "metastore type [hive, glue]")
	_ = metastoreDiffCmd.Flags().String("metastore-uri", "", "Hive metastore URI")
	_ = viper.BindPFlag("metastore.hive.URI", metastoreDiffCmd.Flag("metastore-uri"))
	_ = metastoreDiffCmd.Flags().String("catalog-id", "", "Glue catalog ID")
	_ = viper.BindPFlag("metastore.glue.catalog_id", metastoreDiffCmd.Flag("catalog-id"))
	_ = metastoreDiffCmd.Flags().String("from-address", "", "source metastore address")
	_ = metastoreDiffCmd.Flags().String("from-schema", "", "source schema name")
	_ = metastoreDiffCmd.MarkFlagRequired("from-schema")
	_ = metastoreDiffCmd.Flags().String("from-table", "", "source table name")
	_ = metastoreDiffCmd.Flags().String("to-client-type", "", "metastore type [hive, glue]")
	_ = metastoreDiffCmd.Flags().String("to-address", "", "destination metastore address")
	_ = metastoreDiffCmd.MarkFlagRequired("from-table")
	_ = metastoreDiffCmd.Flags().String("to-schema", "", "destination schema name ")
	_ = metastoreDiffCmd.Flags().String("to-table", "", "destination table name [default is from-table]")

	metastoreCmd.AddCommand(metastoreCopyAllCmd)
	_ = metastoreCopyAllCmd.Flags().String("from-client-type", "", "metastore type [hive, glue]")
	_ = metastoreCopyAllCmd.Flags().String("from-address", "", "source metastore address")
	_ = metastoreCopyAllCmd.Flags().String("to-client-type", "", "metastore type [hive, glue]")
	_ = metastoreCopyAllCmd.Flags().String("to-address", "", "destination metastore address")
	_ = metastoreCopyAllCmd.MarkFlagRequired("to-address")
	_ = metastoreCopyAllCmd.Flags().String("schema-filter", ".*", "filter for schemas to copy in metastore pattern")
	_ = metastoreCopyAllCmd.Flags().String("table-filter", ".*", "filter for tables to copy in metastore pattern")
	_ = metastoreCopyAllCmd.Flags().String("branch", "", "lakeFS branch name")
	_ = metastoreCopyAllCmd.MarkFlagRequired("branch")
	_ = metastoreCopyAllCmd.Flags().String("continue-on-error", "", "prevent copy-all from failing when a single table fails")

	metastoreCmd.AddCommand(glueSymlinkCmd)
	_ = glueSymlinkCmd.Flags().String("repo", "", "lakeFS repository name")
	_ = glueSymlinkCmd.MarkFlagRequired("repo")
	_ = glueSymlinkCmd.Flags().String("branch", "", "lakeFS branch name")
	_ = glueSymlinkCmd.MarkFlagRequired("branch")
	_ = glueSymlinkCmd.Flags().String("path", "", "path to table on lakeFS")
	_ = glueSymlinkCmd.MarkFlagRequired("path")
	_ = glueSymlinkCmd.Flags().String("catalog-id", "", "Glue catalog ID")
	_ = viper.BindPFlag("metastore.glue.catalog_id", glueSymlinkCmd.Flag("catalog-id"))
	_ = glueSymlinkCmd.Flags().String("from-schema", "", "source schema name")
	_ = glueSymlinkCmd.MarkFlagRequired("from-schema")
	_ = glueSymlinkCmd.Flags().String("from-table", "", "source table name")
	_ = glueSymlinkCmd.MarkFlagRequired("from-table")
	_ = glueSymlinkCmd.Flags().String("to-schema", "", "destination schema name")
	_ = glueSymlinkCmd.MarkFlagRequired("to-schema")
	_ = glueSymlinkCmd.Flags().String("to-table", "", "destination table name")
	_ = glueSymlinkCmd.MarkFlagRequired("to-table")
}
