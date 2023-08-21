package cmd

import (
	"net/http"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/treeverse/lakefs/pkg/api"
	"github.com/treeverse/lakefs/pkg/metastore"
)

var metastoreCreateSymlinkCmd = &cobra.Command{
	Use:   "create-symlink",
	Short: "Create symlink table and data",
	Long:  "create table with symlinks, and create the symlinks in s3 in order to access from external services that could only access s3 directly (e.g athena)",
	Run: func(cmd *cobra.Command, args []string) {
		repo := Must(cmd.Flags().GetString("repo"))
		branch := Must(cmd.Flags().GetString("branch"))
		path := Must(cmd.Flags().GetString("path"))
		fromDB := Must(cmd.Flags().GetString("from-schema"))
		fromTable := Must(cmd.Flags().GetString("from-table"))
		toDB := Must(cmd.Flags().GetString("to-schema"))
		toTable := Must(cmd.Flags().GetString("to-table"))
		fromClientType := Must(cmd.Flags().GetString("from-client-type"))

		apiClient := getClient()
		fromClient, toClient, fromClientDeferFunc, toClientDeferFunc := getClients(fromClientType, "glue", "", "")
		defer fromClientDeferFunc()
		defer toClientDeferFunc()

		resp, err := apiClient.CreateSymlinkFileWithResponse(cmd.Context(), repo, branch, &api.CreateSymlinkFileParams{Location: &path})
		if err != nil {
			DieErr(err)
		}
		DieOnErrorOrUnexpectedStatusCode(resp, err, http.StatusCreated)
		if resp.JSON201 == nil {
			Die("Bad response from server", 1)
		}
		location := resp.JSON201.Location

		err = metastore.CopyOrMergeToSymlink(cmd.Context(), fromClient, toClient, fromDB, fromTable, toDB, toTable, location, cfg.Metastore.FixSparkPlaceholder)
		if err != nil {
			DieErr(err)
		}
	},
}

//nolint:gochecknoinits
func init() {
	_ = metastoreCreateSymlinkCmd.Flags().String("repo", "", "lakeFS repository name")
	_ = metastoreCreateSymlinkCmd.MarkFlagRequired("repo")
	_ = metastoreCreateSymlinkCmd.Flags().String("branch", "", "lakeFS branch name")
	_ = metastoreCreateSymlinkCmd.MarkFlagRequired("branch")
	_ = metastoreCreateSymlinkCmd.Flags().String("path", "", "path to table on lakeFS")
	_ = metastoreCreateSymlinkCmd.MarkFlagRequired("path")
	_ = metastoreCreateSymlinkCmd.Flags().String("catalog-id", "", "Glue catalog ID")
	_ = viper.BindPFlag("metastore.glue.catalog_id", metastoreCreateSymlinkCmd.Flag("catalog-id"))
	_ = metastoreCreateSymlinkCmd.Flags().String("from-schema", "", "source schema name")
	_ = metastoreCreateSymlinkCmd.MarkFlagRequired("from-schema")
	_ = metastoreCreateSymlinkCmd.Flags().String("from-table", "", "source table name")
	_ = metastoreCreateSymlinkCmd.MarkFlagRequired("from-table")
	_ = metastoreCreateSymlinkCmd.Flags().String("to-schema", "", "destination schema name")
	_ = metastoreCreateSymlinkCmd.MarkFlagRequired("to-schema")
	_ = metastoreCreateSymlinkCmd.Flags().String("to-table", "", "destination table name")
	_ = metastoreCreateSymlinkCmd.MarkFlagRequired("to-table")
	_ = metastoreCreateSymlinkCmd.Flags().String("from-client-type", "", "metastore type [hive, glue]")

	metastoreCmd.AddCommand(metastoreCreateSymlinkCmd)
}
