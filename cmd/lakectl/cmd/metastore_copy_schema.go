package cmd

import (
	"fmt"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/treeverse/lakefs/pkg/logging"
	"github.com/treeverse/lakefs/pkg/metastore"
)

var metastoreCopySchemaCmd = &cobra.Command{
	Use:   "copy-schema",
	Short: "Copy schema",
	Long:  "Copy schema (without tables). the destination schema will point to the selected branch",
	Run: func(cmd *cobra.Command, args []string) {
		fromClientType := Must(cmd.Flags().GetString("from-client-type"))
		fromDB := Must(cmd.Flags().GetString("from-schema"))
		toClientType := Must(cmd.Flags().GetString("to-client-type"))
		toDB := Must(cmd.Flags().GetString("to-schema"))
		toBranch := Must(cmd.Flags().GetString("to-branch"))
		dbfsLocation := Must(cmd.Flags().GetString("dbfs-root"))

		fromClient, fromClientDeferFunc := getMetastoreClient(fromClientType, "")
		defer fromClientDeferFunc()
		toClient, toClientDeferFunc := getMetastoreClient(toClientType, "")
		defer toClientDeferFunc()
		if len(toDB) == 0 {
			toDB = toBranch
		}

		logging.ContextUnavailable().WithFields(logging.Fields{
			"form_client_type": fromClientType,
			"from_db":          fromDB,
			"to_client_type":   toClientType,
			"to_schema":        toDB,
			"to_branch":        toBranch,
		}).Info("Metadata copy schema")
		fmt.Printf("copy %s -> %s\n", fromDB, toDB)
		err := metastore.CopyDB(cmd.Context(), fromClient, toClient, fromDB, toDB, toBranch, dbfsLocation)
		if err != nil {
			DieErr(err)
		}
	},
}

//nolint:gochecknoinits
func init() {
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

	metastoreCmd.AddCommand(metastoreCopySchemaCmd)
}
