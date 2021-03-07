package cmd

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"net/url"
	"os"
	"strings"

	"github.com/jedib0t/go-pretty/text"
	"github.com/spf13/cobra"
	"github.com/treeverse/lakefs/actions"
	"github.com/treeverse/lakefs/block/factory"
	"github.com/treeverse/lakefs/catalog"
	"github.com/treeverse/lakefs/cmdutils"
	"github.com/treeverse/lakefs/config"
	"github.com/treeverse/lakefs/db"
	"github.com/treeverse/lakefs/graveler"
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
	BaseCommitFlagName   = "commit"
	ManifestURLFormat    = "s3://example-bucket/inventory/YYYY-MM-DDT00-00Z/manifest.json"
	ImportCmdNumArgs     = 1
	CommitterName        = "lakefs"
)

var importCmd = &cobra.Command{
	Use:   "import <repository uri> --manifest <s3 uri to manifest.json>",
	Short: "Import data from S3 to a lakeFS repository",
	Long:  fmt.Sprintf("Import from an S3 inventory to lakeFS without copying the data. It will be added as a new commit in branch %s", onboard.DefaultImportBranchName),
	Args: cmdutils.ValidationChain(
		cobra.ExactArgs(ImportCmdNumArgs),
		cmdutils.FuncValidator(0, uri.ValidateRepoURI),
	),
	Run: func(cmd *cobra.Command, args []string) {
		os.Exit(runImport(cmd, args))
	},
}

var importBaseCmd = &cobra.Command{
	Use:    "import-base <repository uri> --manifest <s3 uri to manifest.json> --commit <base commit>",
	Short:  "Import data from S3 to a lakeFS repository on top of existing commit",
	Long:   "Creates a new commit with the imported data, on top of the given commit. Does not affect any branch",
	Hidden: true,
	Args: cmdutils.ValidationChain(
		cobra.ExactArgs(ImportCmdNumArgs),
		cmdutils.FuncValidator(0, uri.ValidateRepoURI),
	),
	Run: func(cmd *cobra.Command, args []string) {
		os.Exit(runImport(cmd, args))
	},
}

func runImport(cmd *cobra.Command, args []string) (statusCode int) {
	flags := cmd.Flags()
	dryRun, _ := flags.GetBool(DryRunFlagName)
	manifestURL, _ := flags.GetString(ManifestURLFlagName)
	withMerge, _ := flags.GetBool(WithMergeFlagName)
	hideProgress, _ := flags.GetBool(HideProgressFlagName)
	prefixFile, _ := flags.GetString(PrefixesFileFlagName)
	baseCommit, _ := flags.GetString(BaseCommitFlagName)

	ctx := cmd.Context()
	conf := config.NewConfig()
	err := db.ValidateSchemaUpToDate(ctx, conf.GetDatabaseParams())
	if errors.Is(err, db.ErrSchemaNotCompatible) {
		fmt.Println("Migration version mismatch, for more information see https://docs.lakefs.io/deploying/upgrade.html")
		return 1
	}
	if err != nil {
		fmt.Printf("%s\n", err)
		return 1
	}
	logger := logging.FromContext(ctx)
	dbPool := db.BuildDatabaseConnection(ctx, cfg.GetDatabaseParams())
	defer dbPool.Close()

	catalogCfg := catalog.Config{
		Config: cfg,
		DB:     dbPool,
	}
	cataloger, err := catalog.NewCataloger(ctx, catalogCfg)
	if err != nil {
		fmt.Printf("Failed to create cataloger: %s\n", err)
		return 1
	}
	defer func() { _ = cataloger.Close() }()

	// wire actions into entry catalog
	actionsService := actions.NewService(
		dbPool,
		catalog.NewActionsSource(cataloger),
		catalog.NewActionsOutputWriter(cataloger.BlockAdapter),
	)
	cataloger.SetHooksHandler(actionsService)

	u := uri.Must(uri.Parse(args[0]))
	blockStore, err := factory.BuildBlockAdapter(ctx, cfg)
	if err != nil {
		fmt.Printf("Failed to create block adapter: %s\n", err)
		return 1
	}
	if blockStore.BlockstoreType() != "s3" {
		fmt.Printf("Configuration uses unsupported block adapter: %s. Only s3 is supported.\n", blockStore.BlockstoreType())
		return 1
	}
	repoName := u.Repository
	parsedURL, err := url.Parse(manifestURL)
	if err != nil || parsedURL.Scheme != "s3" || !strings.HasSuffix(parsedURL.Path, "/manifest.json") {
		fmt.Printf("Invalid manifest url. expected format: %s\n", ManifestURLFormat)
		return 1
	}

	repo, err := getRepository(ctx, cataloger, repoName)
	if err != nil {
		fmt.Println("Error getting repository", err)
		return 1
	}

	if dryRun {
		fmt.Print("Starting import dry run. Will not perform any changes.\n\n")
	}
	var prefixes []string
	if prefixFile != "" {
		file, err := os.Open(prefixFile)
		if err != nil {
			fmt.Printf("Failed to read prefix filter: %s\n", err)
			return 1
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
			return 1
		}
		fmt.Printf("Filtering according to %d prefixes\n", len(prefixes))
	}

	importConfig := &onboard.Config{
		CommitUsername:     CommitterName,
		InventoryURL:       manifestURL,
		RepositoryID:       graveler.RepositoryID(repoName),
		DefaultBranchID:    graveler.BranchID(repo.DefaultBranch),
		InventoryGenerator: blockStore,
		Store:              cataloger.Store,
		KeyPrefixes:        prefixes,
		BaseCommit:         graveler.CommitID(baseCommit),
	}

	importer, err := onboard.CreateImporter(ctx, logger, importConfig)
	if err != nil {
		fmt.Printf("Import failed: %s\n", err)
		return 1
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
		return 1
	}
	if multiBar != nil {
		multiBar.Stop()
	}
	fmt.Println()
	fmt.Println(text.FgYellow.Sprint("Added or changed objects:"), stats.AddedOrChanged)

	if dryRun {
		fmt.Println("Dry run successful. No changes were made.")
		return 0
	}

	fmt.Print(text.FgYellow.Sprint("Commit ref:"), stats.CommitRef)
	fmt.Println()

	if baseCommit == "" {
		fmt.Printf("Import to branch %s finished successfully.\n", onboard.DefaultImportBranchName)
		fmt.Println()
	}

	if withMerge {
		fmt.Printf("Merging import changes into lakefs://%s@%s/\n", repoName, repo.DefaultBranch)
		msg := fmt.Sprintf(onboard.CommitMsgTemplate, stats.CommitRef)
		commitLog, err := cataloger.Merge(ctx, repoName, onboard.DefaultImportBranchName, repo.DefaultBranch, CommitterName, msg, nil)
		if err != nil {
			fmt.Printf("Merge failed: %s\n", err)
			return 1
		}
		fmt.Println("Merge was completed successfully.")
		fmt.Printf("To list imported objects, run:\n\t$ lakectl fs ls lakefs://%s@%s/\n", repoName, commitLog.Reference)
	} else {
		fmt.Printf("To list imported objects, run:\n\t$ lakectl fs ls lakefs://%s@%s/\n", repoName, stats.CommitRef)
		fmt.Printf("To merge the changes to your main branch, run:\n\t$ lakectl merge lakefs://%s@%s lakefs://%s@%s\n", repoName, stats.CommitRef, repoName, repo.DefaultBranch)
	}

	return 0
}

func getRepository(ctx context.Context, cataloger catalog.Interface, repoName string) (*catalog.Repository, error) {
	repo, err := cataloger.GetRepository(ctx, repoName)
	if err != nil {
		return nil, fmt.Errorf("read repository %s: %w", repoName, err)
	}

	// import branch is created on the fly
	return repo, nil
}

//nolint:gochecknoinits
func init() {
	manifestFlagMsg := fmt.Sprintf("S3 uri to the manifest.json to use for the import. Format: %s", ManifestURLFormat)
	const (
		hideMsg     = "Suppress progress bar"
		prefixesMsg = "File with a list of key prefixes. Imported object keys will be filtered according to these prefixes"
	)

	rootCmd.AddCommand(importCmd)
	importCmd.Flags().Bool(DryRunFlagName, false, "Only read inventory, print stats and write metarange. Commits nothing")
	importCmd.Flags().StringP(ManifestURLFlagName, "m", "", manifestFlagMsg)
	_ = importCmd.MarkFlagRequired(ManifestURLFlagName)
	importCmd.Flags().Bool(WithMergeFlagName, false, "Merge imported data to the repository's main branch")
	importCmd.Flags().Bool(HideProgressFlagName, false, hideMsg)
	importCmd.Flags().StringP(PrefixesFileFlagName, "p", "", prefixesMsg)

	rootCmd.AddCommand(importBaseCmd)
	importBaseCmd.Flags().StringP(ManifestURLFlagName, "m", "", manifestFlagMsg)
	_ = importBaseCmd.MarkFlagRequired(ManifestURLFlagName)
	importBaseCmd.Flags().Bool(HideProgressFlagName, false, hideMsg)
	importBaseCmd.Flags().StringP(PrefixesFileFlagName, "p", "", prefixesMsg)
	importBaseCmd.Flags().StringP(BaseCommitFlagName, "b", "", "Commit to apply to apply the import on top of")
	_ = importCmd.MarkFlagRequired(BaseCommitFlagName)
}
