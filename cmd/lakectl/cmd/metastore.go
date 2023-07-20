package cmd

import (
	"fmt"
	"net/http"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/treeverse/lakefs/cmd/lakectl/cmd/utils"
	"github.com/treeverse/lakefs/pkg/api"
	"github.com/treeverse/lakefs/pkg/logging"
	"github.com/treeverse/lakefs/pkg/metastore"
	"github.com/treeverse/lakefs/pkg/metastore/glue"
	"github.com/treeverse/lakefs/pkg/metastore/hive"
)

var metastoreCmd = &cobra.Command{
	Use:   "metastore",
	Short: "Manage metastore commands",
}

var metastoreCopyCmd = &cobra.Command{
	Use:   "copy",
	Short: "Copy or merge table",
	Long:  "Copy or merge table. the destination table will point to the selected branch",
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
		dbfsLocation, _ := cmd.Flags().GetString("dbfs-root")

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
		err := metastore.CopyOrMerge(cmd.Context(), fromClient, toClient, fromDB, fromTable, toDB, toTable, toBranch, serde, partition, cfg.GetFixSparkPlaceholder(), dbfsLocation)
		if err != nil {
			utils.DieErr(err)
		}
	},
}

var metastoreCopySchemaCmd = &cobra.Command{
	Use:   "copy-schema",
	Short: "Copy schema",
	Long:  "Copy schema (without tables). the destination schema will point to the selected branch",
	Run: func(cmd *cobra.Command, args []string) {
		fromClientType, _ := cmd.Flags().GetString("from-client-type")
		fromDB, _ := cmd.Flags().GetString("from-schema")
		toClientType, _ := cmd.Flags().GetString("to-client-type")
		toDB, _ := cmd.Flags().GetString("to-schema")
		toBranch, _ := cmd.Flags().GetString("to-branch")
		dbfsLocation, _ := cmd.Flags().GetString("dbfs-root")

		fromClient, fromClientDeferFunc := getMetastoreClient(fromClientType, "")
		defer fromClientDeferFunc()
		toClient, toClientDeferFunc := getMetastoreClient(toClientType, "")
		defer toClientDeferFunc()
		if len(toDB) == 0 {
			toDB = toBranch
		}

		logging.Default().WithFields(logging.Fields{
			"form_client_type": fromClientType,
			"from_db":          fromDB,
			"to_client_type":   toClientType,
			"to_schema":        toDB,
			"to_branch":        toBranch,
		}).Info("Metadata copy schema")
		fmt.Printf("copy %s -> %s\n", fromDB, toDB)
		err := metastore.CopyDB(cmd.Context(), fromClient, toClient, fromDB, toDB, toBranch, dbfsLocation)
		if err != nil {
			utils.DieErr(err)
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
			utils.DieErr(err)
		}
		deferFunc := func() {
			err := hiveClient.Close()
			if err != nil {
				utils.DieErr(err)
			}
		}
		return hiveClient, deferFunc

	case "glue":
		client, err := glue.NewMSClient(cfg.GetMetastoreAwsConfig(), cfg.GetMetastoreGlueCatalogID(), cfg.GetGlueDBLocationURI())
		if err != nil {
			utils.DieErr(err)
		}
		return client, func() {}

	default:
		utils.Die("unknown type, expected hive or glue got: "+msType, 1)
	}
	return nil, nil
}

var metastoreCopyAllCmd = &cobra.Command{
	Use:   "copy-all",
	Short: "Copy from one metastore to another",
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
		dbfsLocation, _ := cmd.Flags().GetString("dbfs-root")

		if fromAddress == toAddress {
			utils.Die("from-address must be different than to-address", 1)
		}

		fromClient, deferFunc := getMetastoreClient(fromClientType, fromAddress)
		defer deferFunc()
		toClient, toDeferFunc := getMetastoreClient(toClientType, toAddress)
		defer toDeferFunc()

		fmt.Printf("copy %s -> %s\n", fromAddress, toAddress)
		err := metastore.CopyOrMergeAll(cmd.Context(), fromClient, toClient, schemaFilter, tableFilter, branch, continueOnError, cfg.GetFixSparkPlaceholder(), dbfsLocation)
		if err != nil {
			utils.DieErr(err)
		}
	},
}

var metastoreImportAllCmd = &cobra.Command{
	Use:   "import-all",
	Short: "Import from one metastore to another",
	Long: `
import requested tables between hive metastores. the destination tables will point to the selected repository and branch
table with location s3://my-s3-bucket/path/to/table 
will be transformed to location s3://repo-param/bucket-param/path/to/table
	`,
	Run: func(cmd *cobra.Command, args []string) {
		fromClientType, _ := cmd.Flags().GetString("from-client-type")
		fromAddress, _ := cmd.Flags().GetString("from-address")
		toClientType, _ := cmd.Flags().GetString("to-client-type")
		toAddress, _ := cmd.Flags().GetString("to-address")
		schemaFilter, _ := cmd.Flags().GetString("schema-filter")
		tableFilter, _ := cmd.Flags().GetString("table-filter")
		repo, _ := cmd.Flags().GetString("repo")
		branch, _ := cmd.Flags().GetString("branch")
		continueOnError, _ := cmd.Flags().GetBool("continue-on-error")
		dbfsLocation, _ := cmd.Flags().GetString("dbfs-root")
		if fromAddress == toAddress {
			utils.Die("from-address must be different than to-address", 1)
		}

		fromClient, deferFunc := getMetastoreClient(fromClientType, fromAddress)
		defer deferFunc()
		toClient, toDeferFunc := getMetastoreClient(toClientType, toAddress)
		defer toDeferFunc()

		fmt.Printf("import %s -> %s\n", fromAddress, toAddress)
		err := metastore.ImportAll(cmd.Context(), fromClient, toClient, schemaFilter, tableFilter, repo, branch, continueOnError, cfg.GetFixSparkPlaceholder(), dbfsLocation)
		if err != nil {
			utils.DieErr(err)
		}
	},
}

var metastoreDiffCmd = &cobra.Command{
	Use:   "diff",
	Short: "Show column and partition differences between two tables",
	Run: func(cmd *cobra.Command, args []string) {
		toAddress, _ := cmd.Flags().GetString("to-address")
		fromClientType, _ := cmd.Flags().GetString("from-client-type")
		fromDB, _ := cmd.Flags().GetString("from-schema")
		fromTable, _ := cmd.Flags().GetString("from-table")
		toClientType, _ := cmd.Flags().GetString("to-client-type")
		fromAddress, _ := cmd.Flags().GetString("from-address")
		toDB, _ := cmd.Flags().GetString("to-schema")
		toTable, _ := cmd.Flags().GetString("to-table")

		fromClient, toClient, fromClientDeferFunc, toClientDeferFunc := getClients(fromClientType, toClientType, fromAddress, toAddress)
		defer fromClientDeferFunc()
		defer toClientDeferFunc()

		if len(toDB) == 0 {
			toDB = fromDB
		}
		if len(toTable) == 0 {
			toTable = fromTable
		}
		diff, err := metastore.GetDiff(cmd.Context(), fromClient, toClient, fromDB, fromTable, toDB, toTable)
		if err != nil {
			utils.DieErr(err)
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

func getClients(fromClientType, toClientType, fromAddress, toAddress string) (metastore.Client, metastore.Client, func(), func()) {
	msType := cfg.GetMetastoreType()
	if len(fromClientType) == 0 {
		fromClientType = msType
	}
	if len(toClientType) == 0 {
		toClientType = msType
	}
	fromClient, fromClientDeferFunc := getMetastoreClient(fromClientType, fromAddress)
	toClient, toClientDeferFunc := getMetastoreClient(toClientType, toAddress)

	return fromClient, toClient, fromClientDeferFunc, toClientDeferFunc
}

var createSymlinkCmd = &cobra.Command{
	Use:   "create-symlink",
	Short: "Create symlink table and data",
	Long:  "create table with symlinks, and create the symlinks in s3 in order to access from external services that could only access s3 directly (e.g athena)",
	Run: func(cmd *cobra.Command, args []string) {
		repo, _ := cmd.Flags().GetString("repo")
		branch, _ := cmd.Flags().GetString("branch")
		path, _ := cmd.Flags().GetString("path")
		fromDB, _ := cmd.Flags().GetString("from-schema")
		fromTable, _ := cmd.Flags().GetString("from-table")
		toDB, _ := cmd.Flags().GetString("to-schema")
		toTable, _ := cmd.Flags().GetString("to-table")
		fromClientType, _ := cmd.Flags().GetString("from-client-type")

		apiClient := getClient()
		fromClient, toClient, fromClientDeferFunc, toClientDeferFunc := getClients(fromClientType, "glue", "", "")
		defer fromClientDeferFunc()
		defer toClientDeferFunc()

		resp, err := apiClient.CreateSymlinkFileWithResponse(cmd.Context(), repo, branch, &api.CreateSymlinkFileParams{Location: &path})
		if err != nil {
			utils.DieErr(err)
		}
		utils.DieOnErrorOrUnexpectedStatusCode(resp, err, http.StatusCreated)
		if resp.JSON201 == nil {
			utils.Die("Bad response from server", 1)
		}
		location := resp.JSON201.Location

		err = metastore.CopyOrMergeToSymlink(cmd.Context(), fromClient, toClient, fromDB, fromTable, toDB, toTable, location, cfg.GetFixSparkPlaceholder())
		if err != nil {
			utils.DieErr(err)
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
	_ = metastoreCopyCmd.Flags().String("dbfs-root", "", "dbfs location root will replace `dbfs:/` in the location before transforming")
	metastoreCmd.AddCommand(metastoreCopySchemaCmd)
	_ = metastoreCopySchemaCmd.Flags().String("from-client-type", "", "metastore type [hive, glue]")
	_ = metastoreCopySchemaCmd.Flags().String("metastore-uri", "", "Hive metastore URI")
	_ = viper.BindPFlag("metastore.hive.uri", metastoreCopySchemaCmd.Flag("metastore-uri"))
	_ = metastoreCopySchemaCmd.Flags().String("catalog-id", "", "Glue catalog ID")
	_ = viper.BindPFlag("metastore.glue.catalog_id", metastoreCopySchemaCmd.Flag("catalog-id"))
	_ = metastoreCopySchemaCmd.Flags().String("from-schema", "", "source schema name")
	_ = metastoreCopySchemaCmd.MarkFlagRequired("from-schema")
	_ = metastoreCopySchemaCmd.Flags().String("to-client-type", "", "metastore type [hive, glue]")
	_ = metastoreCopySchemaCmd.Flags().String("to-schema", "", "destination schema name [default is from-branch]")
	_ = metastoreCopySchemaCmd.Flags().String("to-branch", "", "lakeFS branch name")
	_ = metastoreCopySchemaCmd.MarkFlagRequired("to-branch")
	_ = metastoreCopySchemaCmd.Flags().String("dbfs-root", "", "dbfs location root will replace `dbfs:/` in the location before transforming")
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
	_ = metastoreCopyAllCmd.Flags().Bool("continue-on-error", false, "prevent copy-all from failing when a single table fails")
	_ = metastoreCopyAllCmd.Flags().String("dbfs-root", "", "dbfs location root will replace `dbfs:/` in the location before transforming")
	metastoreCmd.AddCommand(metastoreImportAllCmd)
	_ = metastoreImportAllCmd.Flags().String("from-client-type", "", "metastore type [hive, glue]")
	_ = metastoreImportAllCmd.Flags().String("from-address", "", "source metastore address")
	_ = metastoreImportAllCmd.Flags().String("to-client-type", "", "metastore type [hive, glue]")
	_ = metastoreImportAllCmd.Flags().String("to-address", "", "destination metastore address")
	_ = metastoreImportAllCmd.MarkFlagRequired("to-address")
	_ = metastoreImportAllCmd.Flags().String("schema-filter", ".*", "filter for schemas to copy in metastore pattern")
	_ = metastoreImportAllCmd.Flags().String("table-filter", ".*", "filter for tables to copy in metastore pattern")
	_ = metastoreImportAllCmd.Flags().String("repo", "", "lakeFS repo name")
	_ = metastoreImportAllCmd.MarkFlagRequired("repo")
	_ = metastoreImportAllCmd.Flags().String("branch", "", "lakeFS branch name")
	_ = metastoreImportAllCmd.MarkFlagRequired("branch")
	_ = metastoreImportAllCmd.Flags().Bool("continue-on-error", false, "prevent import-all from failing when a single table fails")
	_ = metastoreImportAllCmd.Flags().String("dbfs-root", "", "dbfs location root will replace `dbfs:/` in the location before transforming")
	metastoreCmd.AddCommand(createSymlinkCmd)
	_ = createSymlinkCmd.Flags().String("repo", "", "lakeFS repository name")
	_ = createSymlinkCmd.MarkFlagRequired("repo")
	_ = createSymlinkCmd.Flags().String("branch", "", "lakeFS branch name")
	_ = createSymlinkCmd.MarkFlagRequired("branch")
	_ = createSymlinkCmd.Flags().String("path", "", "path to table on lakeFS")
	_ = createSymlinkCmd.MarkFlagRequired("path")
	_ = createSymlinkCmd.Flags().String("catalog-id", "", "Glue catalog ID")
	_ = viper.BindPFlag("metastore.glue.catalog_id", createSymlinkCmd.Flag("catalog-id"))
	_ = createSymlinkCmd.Flags().String("from-schema", "", "source schema name")
	_ = createSymlinkCmd.MarkFlagRequired("from-schema")
	_ = createSymlinkCmd.Flags().String("from-table", "", "source table name")
	_ = createSymlinkCmd.MarkFlagRequired("from-table")
	_ = createSymlinkCmd.Flags().String("to-schema", "", "destination schema name")
	_ = createSymlinkCmd.MarkFlagRequired("to-schema")
	_ = createSymlinkCmd.Flags().String("to-table", "", "destination table name")
	_ = createSymlinkCmd.MarkFlagRequired("to-table")
	_ = createSymlinkCmd.Flags().String("from-client-type", "", "metastore type [hive, glue]")
}
