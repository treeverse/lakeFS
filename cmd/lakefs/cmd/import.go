package cmd

import (
	"context"
	"errors"
	"fmt"
	"os"
	"regexp"
	"time"

	"github.com/briandowns/spinner"
	"github.com/jedib0t/go-pretty/text"
	"github.com/spf13/cobra"
	lakectl "github.com/treeverse/lakefs/cmd/lakectl/cmd"
	"github.com/treeverse/lakefs/uri"

	"github.com/treeverse/lakefs/block/factory"
	"github.com/treeverse/lakefs/catalog"
	"github.com/treeverse/lakefs/config"
	"github.com/treeverse/lakefs/db"
	"github.com/treeverse/lakefs/logging"
	"github.com/treeverse/lakefs/onboard"
)

const (
	DryRunFlagName         = "dry-run"
	ManifestUrlFlagName    = "manifest"
	ManifestUrlFormat      = "s3://example-bucket/inventory/YYYY-MM-DDT00-00Z/manifest.json"
	ImportCmdNumArgs       = 1
	ImportSpinnerFrequency = 100 * time.Millisecond
)

var importCmd = &cobra.Command{
	Use:   "import <repository uri> --manifest <s3 uri to manifest.json>",
	Short: "Import data from S3 to a lakeFS repository",
	Long:  "Import from an S3 inventory to lakeFS without copying the data.",
	Args: lakectl.ValidationChain(
		lakectl.HasNArgs(ImportCmdNumArgs),
		lakectl.IsRepoURI(0),
	),
	Run: func(cmd *cobra.Command, args []string) {
		ctx := context.Background()
		conf := config.NewConfig()
		logger := logging.FromContext(ctx)
		dbPool := db.BuildDatabaseConnection(cfg.GetDatabaseParams())
		cataloger := catalog.NewCataloger(dbPool, catalog.WithParams(conf.GetCatalogerCatalogParams()))
		blockStore, err := factory.BuildBlockAdapter(cfg)
		dryRun, _ := cmd.Flags().GetBool(DryRunFlagName)

		u := uri.Must(uri.Parse(args[0]))
		if err != nil {
			fmt.Printf("failed to create block adapter: %v\n", err)
			os.Exit(1)
		}
		manifestUrl, _ := cmd.Flags().GetString(ManifestUrlFlagName)
		match, err := regexp.MatchString("s3://.*/manifest.json", manifestUrl)

		if err != nil || !match {
			fmt.Printf("invalid manifest url. expected format: %s\n", ManifestUrlFormat)
		}
		var repo *catalog.Repository
		if !dryRun {
			repo, err = cataloger.GetRepository(ctx, u.Repository)
			if err != nil {
				fmt.Printf("failed to read repository %s: %v\n", u.Repository, err)
				os.Exit(1)
			}
			_, err = cataloger.GetBranchReference(ctx, u.Repository, onboard.DefaultBranchName)
			if errors.Is(err, db.ErrNotFound) {
				fmt.Printf("Branch %s does not exist, creating.\n", onboard.DefaultBranchName)
				_, err = cataloger.CreateBranch(ctx, u.Repository, onboard.DefaultBranchName, repo.DefaultBranch)
				if err != nil {
					fmt.Printf("failed to create branch %s in repo %s: %v\n", onboard.DefaultBranchName, u.Repository, err)
					os.Exit(1)
				}
			} else if err != nil {
				fmt.Printf("error when fetching branches for repo %s: %v\n", u.Repository, err)
				os.Exit(1)
			}
		}
		importConfig := &onboard.Config{
			CommitUsername:     "lakefs",
			InventoryURL:       manifestUrl,
			Repository:         u.Repository,
			InventoryGenerator: blockStore,
			Cataloger:          cataloger,
		}
		s := spinner.New(spinner.CharSets[34], ImportSpinnerFrequency)
		importer, err := onboard.CreateImporter(ctx, logger, func(event *onboard.ProgressEvent) {
			s.Suffix = fmt.Sprintf(" Objects Created/Changed: %d\tObjects Deleted: %d", *event.AddedOrChanged, *event.Deleted)
		}, importConfig)
		if err != nil {
			fmt.Printf("import failed: %v\n", err)
			os.Exit(1)
		}
		s.Start()
		stats, err := importer.Import(ctx, dryRun)
		s.Stop()
		if err != nil {
			fmt.Printf("import failed: %v\n", err)
			os.Exit(1)
		}
		fmt.Print(text.FgYellow.Sprint("Added or changed objects: "), fmt.Sprintf("%d\n", *stats.AddedOrChanged))
		fmt.Print(text.FgYellow.Sprint("Deleted objects: "), fmt.Sprintf("%d\n", *stats.Deleted))
		fmt.Print(text.FgYellow.Sprint("Previously imported inventory: "), fmt.Sprintf("%s\n", stats.PreviousInventoryURL))
		fmt.Print(text.FgYellow.Sprint("Previous import date: "), fmt.Sprintf("%v\n\n", stats.PreviousImportDate))

		if !dryRun {
			fmt.Print(text.FgYellow.Sprint("Commit ref: "), fmt.Sprintf("%s\n\n", stats.CommitRef))
			fmt.Printf("Import finished successfully to branch %s\n", onboard.DefaultBranchName)
			fmt.Printf("To list imported objects, run:\n\tlakectl fs ls lakefs://%s@%s/\n", u.Repository, stats.CommitRef)
			fmt.Printf("To merge the changes to your main branch, run:\n\tlakectl merge lakefs://%s@%s/ lakefs://%s@%s/\n", u.Repository, onboard.DefaultBranchName, u.Repository, repo.DefaultBranch)
		} else {
			fmt.Println("Dry run successful. No changes were made.")
		}
	},
}

//nolint:gochecknoinits
func init() {
	rootCmd.AddCommand(importCmd)
	importCmd.Flags().Bool(DryRunFlagName, false, "Only read inventory and print stats, without making any changes")
	importCmd.Flags().StringP(ManifestUrlFlagName, "m", "", fmt.Sprintf("S3 uri to the manifest.json to use for the import. Format: %s", ManifestUrlFormat))
}
