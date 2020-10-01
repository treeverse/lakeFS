package cmd

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"os"
	"strings"

	"github.com/jedib0t/go-pretty/text"
	"github.com/spf13/cobra"
	"github.com/treeverse/lakefs/cmdutils"
	"github.com/treeverse/lakefs/uri"

	"github.com/treeverse/lakefs/block/factory"
	"github.com/treeverse/lakefs/catalog"
	"github.com/treeverse/lakefs/config"
	"github.com/treeverse/lakefs/db"
	"github.com/treeverse/lakefs/logging"
	"github.com/treeverse/lakefs/onboard"
)

const (
	DryRunFlagName      = "dry-run"
	WithMergeFlagName   = "with-merge"
	ManifestURLFlagName = "manifest"
	ManifestURLFormat   = "s3://example-bucket/inventory/YYYY-MM-DDT00-00Z/manifest.json"
	ImportCmdNumArgs    = 1
	CommitterName       = "lakefs"
)

var importCmd = &cobra.Command{
	Use:   "import <repository uri> --manifest <s3 uri to manifest.json>",
	Short: "Import data from S3 to a lakeFS repository",
	Long:  "Import from an S3 inventory to lakeFS without copying the data.",
	Args: cmdutils.ValidationChain(
		cobra.ExactArgs(ImportCmdNumArgs),
		cmdutils.FuncValidator(0, uri.ValidateRepoURI),
	),
	Run: func(cmd *cobra.Command, args []string) {
		dryRun, _ := cmd.Flags().GetBool(DryRunFlagName)
		manifestURL, _ := cmd.Flags().GetString(ManifestURLFlagName)
		withMerge, _ := cmd.Flags().GetBool(WithMergeFlagName)

		ctx := context.Background()
		conf := config.NewConfig()
		err := db.ValidateSchemaUpToDate(conf.GetDatabaseParams())
		if err != nil {
			fmt.Printf("%s\n", err)
			os.Exit(1)
		}
		logger := logging.FromContext(ctx)
		dbPool := db.BuildDatabaseConnection(cfg.GetDatabaseParams())
		defer func() { _ = dbPool.Close() }()

		cataloger := catalog.NewCataloger(dbPool, catalog.WithParams(conf.GetCatalogerCatalogParams()))
		defer func() { _ = cataloger.Close() }()

		u := uri.Must(uri.Parse(args[0]))
		blockStore, err := factory.BuildBlockAdapter(cfg)
		if err != nil {
			fmt.Printf("Failed to create block adapter: %s\n", err)
			os.Exit(1)
		}
		if blockStore.BlockstoreType() != "s3" {
			fmt.Printf("Configuration uses unsupported block adapter: %s. Only s3 is supported.\n", blockStore.BlockstoreType())
			os.Exit(1)
		}
		repoName := u.Repository
		parsedURL, err := url.Parse(manifestURL)
		if err != nil || parsedURL.Scheme != "s3" || !strings.HasSuffix(parsedURL.Path, "/manifest.json") {
			fmt.Printf("Invalid manifest url. expected format: %s\n", ManifestURLFormat)
			os.Exit(1)
		}
		repo, err := cataloger.GetRepository(ctx, repoName)
		if err != nil {
			fmt.Printf("Failed to read repository %s: %s\n", repoName, err)
			os.Exit(1)
		}
		if !dryRun {
			err = prepareBranch(ctx, cataloger, repo, onboard.DefaultBranchName)
			if err != nil {
				fmt.Println(err)
				os.Exit(1)
			}
		} else {
			fmt.Print("Starting import dry run. Will not perform any changes.\n\n")
		}
		importConfig := &onboard.Config{
			CommitUsername:     CommitterName,
			InventoryURL:       manifestURL,
			Repository:         repoName,
			InventoryGenerator: blockStore,
			Cataloger:          cataloger,
		}

		importer, err := onboard.CreateImporter(ctx, logger, importConfig)
		if err != nil {
			fmt.Printf("Import failed: %s\n", err)
			os.Exit(1)
		}
		multiBar := cmdutils.NewMultiBar(importer)
		multiBar.Start()
		stats, err := importer.Import(ctx, dryRun)
		if err != nil {
			multiBar.Stop()
			fmt.Printf("Import failed: %s\n", err)
			os.Exit(1)
		}
		multiBar.Stop()
		fmt.Println()
		fmt.Println(text.FgYellow.Sprint("Added or changed objects:"), stats.AddedOrChanged)
		fmt.Println(text.FgYellow.Sprint("Deleted objects:"), stats.Deleted)
		if stats.PreviousInventoryURL != "" {
			fmt.Println(text.FgYellow.Sprint("Previously imported inventory:"), stats.PreviousInventoryURL)
			fmt.Println(text.FgYellow.Sprint("Previous import date:"), stats.PreviousImportDate)
			fmt.Println()
		}
		if dryRun {
			fmt.Println("Dry run successful. No changes were made.")
			return
		}

		fmt.Print(text.FgYellow.Sprint("Commit ref:"), stats.CommitRef)
		fmt.Println()
		fmt.Printf("Import to branch %s finished successfully.\n", onboard.DefaultBranchName)
		if withMerge {
			fmt.Printf("Merging import changes into lakefs://%s@%s/\n", repoName, repo.DefaultBranch)
			msg := fmt.Sprintf(onboard.CommitMsgTemplate, stats.CommitRef)
			commitLog, err := cataloger.Merge(ctx, repoName, onboard.DefaultBranchName, repo.DefaultBranch, CommitterName, msg, nil)
			if err != nil {
				fmt.Printf("Merge failed: %s\n", err)
				os.Exit(1)
			}
			fmt.Println("Merge was completed successfully")
			fmt.Printf("To list imported objects, run:\n\t$ lakectl fs ls lakefs://%s@%s/\n", repoName, commitLog.Reference)
		} else {
			fmt.Printf("To list imported objects, run:\n\t$ lakectl fs ls lakefs://%s@%s/\n", repoName, stats.CommitRef)
			fmt.Printf("To merge the changes to your main branch, run:\n\t$ lakectl merge lakefs://%s@%s lakefs://%s@%s\n", repoName, onboard.DefaultBranchName, repoName, repo.DefaultBranch)
		}
	},
}

func prepareBranch(ctx context.Context, cataloger catalog.Cataloger, repo *catalog.Repository, branch string) error {
	repoName := repo.Name
	_, err := cataloger.GetBranchReference(ctx, repoName, branch)
	if err == nil {
		fmt.Printf("Branch %s already exists, no need to create it.\n\n", branch)
		return nil
	}
	if errors.Is(err, db.ErrNotFound) {
		fmt.Printf("Branch %s does not exist, creating.\n\n", branch)
		_, err = cataloger.CreateBranch(ctx, repoName, branch, repo.DefaultBranch)
		if err != nil {
			return fmt.Errorf("failed to create branch %s in repo %s: %w", branch, repoName, err)
		}
		return nil
	}
	return fmt.Errorf("error when fetching branches for repo %s: %w", repoName, err)
}

//nolint:gochecknoinits
func init() {
	rootCmd.AddCommand(importCmd)
	importCmd.Flags().Bool(DryRunFlagName, false, "Only read inventory and print stats, without making any changes")
	importCmd.Flags().StringP(ManifestURLFlagName, "m", "", fmt.Sprintf("S3 uri to the manifest.json to use for the import. Format: %s", ManifestURLFormat))
	_ = importCmd.MarkFlagRequired(ManifestURLFlagName)
	importCmd.Flags().Bool(WithMergeFlagName, false, "With merge after import completes")
}
