package cmd

import (
	"context"
	"github.com/spf13/cobra"
	"github.com/treeverse/lakefs/catalog"
	"github.com/treeverse/lakefs/cmd"
	"github.com/treeverse/lakefs/config"
	"github.com/treeverse/lakefs/onboard"
	"github.com/treeverse/lakefs/uri"
)

// repoImportCmd represents the import repo command
// lakefs import s3://example-bucket/inventory/manifest.json lakefs://myrepo s3://my-bucket/
var repoImportCmd = &cobra.Command{
	Use:   "import  [s3 manifest url] [repository uri]",
	Short: "import from an s3 inventory manifest to a lakeFS repository",
	Args: cmd.ValidationChain(
		cmd.HasNArgs(2),
		cmd.IsRepoURI(1),
	),
	Run: func(cmd *cobra.Command, args []string) {
		cdb := cfg.ConnectDatabase(config.DBKeyCatalog)
		cataloger := catalog.NewCataloger(cdb)
		ctx := context.Background()
		u := uri.Must(uri.Parse(args[1]))

		s3svc := cfg.BuildS3Service()
		err := onboard.Import(ctx, s3svc, cataloger, args[0], u.Repository)
		if err != nil {
			println("Error: " + err.Error()) // TODO handle
		}
	},
}

func init() {
	rootCmd.AddCommand(repoImportCmd)
}
