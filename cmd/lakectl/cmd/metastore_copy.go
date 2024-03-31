package cmd

import (
	"fmt"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/treeverse/lakefs/pkg/metastore"
	"github.com/treeverse/lakefs/util/logging"
)

var metastoreCopyCmd = &cobra.Command{
	Use:        "copy",
	Short:      "Copy or merge table",
	Long:       "Copy or merge table. the destination table will point to the selected branch",
	Deprecated: "Upcoming releases of lakectl will no longer support this command.",
	Run: func(cmd *cobra.Command, args []string) {
		fromClientType := Must(cmd.Flags().GetString("from-client-type"))
		fromDB := Must(cmd.Flags().GetString("from-schema"))
		fromTable := Must(cmd.Flags().GetString("from-table"))
		toClientType := Must(cmd.Flags().GetString("to-client-type"))
		toDB := Must(cmd.Flags().GetString("to-schema"))
		toTable := Must(cmd.Flags().GetString("to-table"))
		toBranch := Must(cmd.Flags().GetString("to-branch"))
		serde := Must(cmd.Flags().GetString("serde"))
		partition := Must(cmd.Flags().GetStringSlice("partition"))
		dbfsLocation := Must(cmd.Flags().GetString("dbfs-root"))

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
		logging.ContextUnavailable().WithFields(logging.Fields{
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
		err := metastore.CopyOrMerge(cmd.Context(), fromClient, toClient, fromDB, fromTable, toDB, toTable, toBranch, serde, partition, cfg.Metastore.FixSparkPlaceholder, dbfsLocation)
		if err != nil {
			DieErr(err)
		}
	},
}

//nolint:gochecknoinits
func init() {
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

	metastoreCmd.AddCommand(metastoreCopyCmd)
}
