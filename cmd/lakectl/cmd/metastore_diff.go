package cmd

import (
	"fmt"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/treeverse/lakefs/pkg/metastore"
)

var metastoreDiffCmd = &cobra.Command{
	Use:   "diff",
	Short: "Show column and partition differences between two tables",
	Run: func(cmd *cobra.Command, args []string) {
		toAddress := Must(cmd.Flags().GetString("to-address"))
		fromClientType := Must(cmd.Flags().GetString("from-client-type"))
		fromDB := Must(cmd.Flags().GetString("from-schema"))
		fromTable := Must(cmd.Flags().GetString("from-table"))
		toClientType := Must(cmd.Flags().GetString("to-client-type"))
		fromAddress := Must(cmd.Flags().GetString("from-address"))
		toDB := Must(cmd.Flags().GetString("to-schema"))
		toTable := Must(cmd.Flags().GetString("to-table"))

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
			DieErr(err)
		}
		fmt.Printf("%s.%s <--> %s.%s\n", fromDB, fromTable, toDB, toTable)
		if len(diff.ColumnsDiff) == 0 {
			fmt.Println("Columns are identical")
		} else {
			fmt.Println("Columns")
			for _, column := range diff.ColumnsDiff {
				fmt.Println(column)
			}
		}
		if len(diff.PartitionDiff) == 0 {
			fmt.Println("Partitions are identical")
		} else {
			fmt.Println("Partitions")
			for _, partition := range diff.PartitionDiff {
				fmt.Println(partition)
			}
		}
	},
}

//nolint:gochecknoinits
func init() {
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

	metastoreCmd.AddCommand(metastoreDiffCmd)
}
