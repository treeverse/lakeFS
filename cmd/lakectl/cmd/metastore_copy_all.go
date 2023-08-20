package cmd

import (
	"fmt"

	"github.com/spf13/cobra"
	"github.com/treeverse/lakefs/pkg/metastore"
)

var metastoreCopyAllCmd = &cobra.Command{
	Use:   "copy-all",
	Short: "Copy from one metastore to another",
	Long:  "copy or merge requested tables between hive metastores. the destination tables will point to the selected branch",
	Run: func(cmd *cobra.Command, args []string) {
		fromClientType := Must(cmd.Flags().GetString("from-client-type"))
		fromAddress := Must(cmd.Flags().GetString("from-address"))
		toClientType := Must(cmd.Flags().GetString("to-client-type"))
		toAddress := Must(cmd.Flags().GetString("to-address"))
		schemaFilter := Must(cmd.Flags().GetString("schema-filter"))
		tableFilter := Must(cmd.Flags().GetString("table-filter"))
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

		fmt.Printf("copy %s -> %s\n", fromAddress, toAddress)
		err := metastore.CopyOrMergeAll(cmd.Context(), fromClient, toClient, schemaFilter, tableFilter, branch, continueOnError, cfg.Metastore.FixSparkPlaceholder, dbfsLocation)
		if err != nil {
			DieErr(err)
		}
	},
}

//nolint:gochecknoinits
func init() {
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

	metastoreCmd.AddCommand(metastoreCopyAllCmd)
}
