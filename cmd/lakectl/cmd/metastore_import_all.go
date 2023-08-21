package cmd

import (
	"fmt"

	"github.com/spf13/cobra"
	"github.com/treeverse/lakefs/pkg/metastore"
)

var metastoreImportAllCmd = &cobra.Command{
	Use:   "import-all",
	Short: "Import from one metastore to another",
	Long: `
import requested tables between hive metastores. the destination tables will point to the selected repository and branch
table with location s3://my-s3-bucket/path/to/table 
will be transformed to location s3://repo-param/bucket-param/path/to/table
	`,
	Run: func(cmd *cobra.Command, args []string) {
		fromClientType := Must(cmd.Flags().GetString("from-client-type"))
		fromAddress := Must(cmd.Flags().GetString("from-address"))
		toClientType := Must(cmd.Flags().GetString("to-client-type"))
		toAddress := Must(cmd.Flags().GetString("to-address"))
		schemaFilter := Must(cmd.Flags().GetString("schema-filter"))
		tableFilter := Must(cmd.Flags().GetString("table-filter"))
		repo := Must(cmd.Flags().GetString("repo"))
		branch := Must(cmd.Flags().GetString("branch"))
		continueOnError := Must(cmd.Flags().GetBool("continue-on-error"))
		dbfsLocation := Must(cmd.Flags().GetString("dbfs-root"))
		if fromAddress == toAddress {
			Die("from-address must be different than to-address", 1)
		}

		fromClient, deferFunc := getMetastoreClient(fromClientType, fromAddress)
		defer deferFunc()
		toClient, toDeferFunc := getMetastoreClient(toClientType, toAddress)
		defer toDeferFunc()

		fmt.Printf("import %s -> %s\n", fromAddress, toAddress)
		err := metastore.ImportAll(cmd.Context(), fromClient, toClient, schemaFilter, tableFilter, repo, branch, continueOnError, cfg.Metastore.FixSparkPlaceholder, dbfsLocation)
		if err != nil {
			DieErr(err)
		}
	},
}

//nolint:gochecknoinits
func init() {
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

	metastoreCmd.AddCommand(metastoreImportAllCmd)
}
