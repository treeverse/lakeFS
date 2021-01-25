package cmd

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"net/url"
	"os"
	"strings"

	"github.com/treeverse/lakefs/catalog/rocks"

	"github.com/jedib0t/go-pretty/text"
	"github.com/spf13/cobra"
	"github.com/treeverse/lakefs/block/factory"
	"github.com/treeverse/lakefs/catalog"
	catalogfactory "github.com/treeverse/lakefs/catalog/factory"
	"github.com/treeverse/lakefs/cmdutils"
	"github.com/treeverse/lakefs/config"
	"github.com/treeverse/lakefs/db"
	"github.com/treeverse/lakefs/logging"
	"github.com/treeverse/lakefs/onboard"
	"github.com/treeverse/lakefs/uri"
)

const (
	DryRunFlagName       = "dry-run"
	WithMergeFlagName    = "with-merge"
	HideProgressFlagName = "hide-progress"
	ManifestURLFlagName  = "manifest"
	PrefixesFileFlagName = "prefix-file"
	ManifestURLFormat    = "s3://example-bucket/inventory/YYYY-MM-DDT00-00Z/manifest.json"
	ImportCmdNumArgs     = 1
	CommitterName        = "lakefs"
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
		flags := cmd.Flags()
		dryRun, _ := flags.GetBool(DryRunFlagName)
		manifestURL, _ := flags.GetString(ManifestURLFlagName)
		withMerge, _ := flags.GetBool(WithMergeFlagName)
		hideProgress, _ := flags.GetBool(HideProgressFlagName)
		prefixFile, _ := flags.GetString(PrefixesFileFlagName)

		ctx := context.Background()
		conf := config.NewConfig()
		err := db.ValidateSchemaUpToDate(conf.GetDatabaseParams())
		if errors.Is(err, db.ErrSchemaNotCompatible) {
			fmt.Println("Migration version mismatch, for more information see https://docs.lakefs.io/deploying/upgrade.html")
			os.Exit(1)
		}
		if err != nil {
			fmt.Printf("%s\n", err)
			os.Exit(1)
		}
		logger := logging.FromContext(ctx)
		dbPool := db.BuildDatabaseConnection(cfg.GetDatabaseParams())
		defer dbPool.Close()

		isRocks := cfg.GetCatalogerType() == "rocks"
		cataloger, err := catalogfactory.BuildCataloger(dbPool, cfg)
		if err != nil {
			fmt.Printf("Failed to create cataloger: %s\n", err)
			os.Exit(1)
		}
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

		repo, err := getRepository(ctx, cataloger, repoName, dryRun, isRocks)
		if err != nil {
			fmt.Println("Error", err)
			if errors.Is(err, catalog.ErrBranchNotFound) {
				fmt.Println("This repository was created with an older version of lakeFS. To use the import feature, create a new repository")
			}
			os.Exit(1)
		}

		if dryRun {
			fmt.Print("Starting import dry run. Will not perform any changes.\n\n")
		}
		var prefixes []string
		if prefixFile != "" {
			file, err := os.Open(prefixFile)
			if err != nil {
				fmt.Printf("Failed to read prefix filter: %s\n", err)
				os.Exit(1)
			}
			defer func() {
				_ = file.Close()
			}()
			scanner := bufio.NewScanner(file)
			for scanner.Scan() {
				prefix := scanner.Text()
				if prefix != "" {
					prefixes = append(prefixes, prefix)
				}
			}
			if err := scanner.Err(); err != nil {
				fmt.Printf("Failed to read prefix filter: %s\n", err)
				os.Exit(1)
			}
			fmt.Printf("Filtering according to %d prefixes\n", len(prefixes))
		}

		var entryCataloger *rocks.EntryCatalog
		if isRocks {
			entryCataloger, err = rocks.NewEntryCatalog(cfg, dbPool)
			if err != nil {
				fmt.Printf("Failed to build entry catalog: %s\n", err)
				os.Exit(1)
			}
		}

		importConfig := &onboard.Config{
			CommitUsername:     CommitterName,
			InventoryURL:       manifestURL,
			Repository:         repoName,
			InventoryGenerator: blockStore,
			Cataloger:          cataloger,
			KeyPrefixes:        prefixes,
			Rocks:              isRocks,
			EntryCatalog:       entryCataloger,
		}

		importer, err := onboard.CreateImporter(ctx, logger, importConfig)
		if err != nil {
			fmt.Printf("Import failed: %s\n", err)
			os.Exit(1)
		}
		var multiBar *cmdutils.MultiBar
		if !hideProgress {
			multiBar = cmdutils.NewMultiBar(importer)
			multiBar.Start()
		}
		stats, err := importer.Import(ctx, dryRun)
		if err != nil {
			if multiBar != nil {
				multiBar.Stop()
			}
			fmt.Printf("Import failed: %s\n", err)
			os.Exit(1)
		}
		if multiBar != nil {
			multiBar.Stop()
		}
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
		fmt.Printf("Import to branch %s finished successfully.\n", catalog.DefaultImportBranchName)
		fmt.Println()
		if withMerge {
			fmt.Printf("Merging import changes into lakefs://%s@%s/\n", repoName, repo.DefaultBranch)
			msg := fmt.Sprintf(onboard.CommitMsgTemplate, stats.CommitRef)
			commitLog, err := cataloger.Merge(ctx, repoName, catalog.DefaultImportBranchName, repo.DefaultBranch, CommitterName, msg, nil)
			if err != nil {
				fmt.Printf("Merge failed: %s\n", err)
				os.Exit(1)
			}
			fmt.Println("Merge was completed successfully.")
			fmt.Printf("To list imported objects, run:\n\t$ lakectl fs ls lakefs://%s@%s/\n", repoName, commitLog.Reference)
		} else {
			fmt.Printf("To list imported objects, run:\n\t$ lakectl fs ls lakefs://%s@%s/\n", repoName, stats.CommitRef)
			fmt.Printf("To merge the changes to your main branch, run:\n\t$ lakectl merge lakefs://%s@%s lakefs://%s@%s\n", repoName, catalog.DefaultImportBranchName, repoName, repo.DefaultBranch)
		}
	},
}

func getRepository(ctx context.Context, cataloger catalog.Cataloger, repoName string, dryRun, isRocks bool) (*catalog.Repository, error) {
	if dryRun {
		return &catalog.Repository{
			Name:          repoName,
			DefaultBranch: catalog.DefaultBranchName,
		}, nil
	}
	repo, err := cataloger.GetRepository(ctx, repoName)
	if err != nil {
		return nil, fmt.Errorf("read repository %s: %w", repoName, err)
	}
	if isRocks {
		// import branch is created on the fly for Rocks implementation.
		return repo, nil
	}

	// check we have import branch on this repo
	importBranchExists, err := cataloger.BranchExists(ctx, repoName, catalog.DefaultImportBranchName)
	if err != nil {
		return nil, fmt.Errorf("read branch (%s) information from repository %s: %w", catalog.DefaultImportBranchName, repoName, err)
	}
	if !importBranchExists {
		return nil, fmt.Errorf("import %w in repository: %s", catalog.ErrBranchNotFound, repoName)
	}
	return repo, nil
}

//nolint:gochecknoinits
func init() {
	rootCmd.AddCommand(importCmd)
	importCmd.Flags().Bool(DryRunFlagName, false, "Only read inventory and print stats, without making any changes")
	importCmd.Flags().StringP(ManifestURLFlagName, "m", "", fmt.Sprintf("S3 uri to the manifest.json to use for the import. Format: %s", ManifestURLFormat))
	_ = importCmd.MarkFlagRequired(ManifestURLFlagName)
	importCmd.Flags().Bool(WithMergeFlagName, false, "Merge imported data to the repository's main branch")
	importCmd.Flags().Bool(HideProgressFlagName, false, "Suppress progress bar")
	importCmd.Flags().StringP(PrefixesFileFlagName, "p", "", "File with a list of key prefixes. Imported object keys will be filtered according to these prefixes")
}
