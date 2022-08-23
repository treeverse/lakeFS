package cmd

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"net/url"
	"os"
	"strings"

	"github.com/jedib0t/go-pretty/v6/text"
	"github.com/spf13/cobra"
	"github.com/treeverse/lakefs/pkg/actions"
	"github.com/treeverse/lakefs/pkg/block/factory"
	"github.com/treeverse/lakefs/pkg/catalog"
	"github.com/treeverse/lakefs/pkg/cmdutils"
	"github.com/treeverse/lakefs/pkg/db"
	"github.com/treeverse/lakefs/pkg/graveler"
	"github.com/treeverse/lakefs/pkg/kv"
	"github.com/treeverse/lakefs/pkg/logging"
	"github.com/treeverse/lakefs/pkg/onboard"
	"github.com/treeverse/lakefs/pkg/stats"
	"github.com/treeverse/lakefs/pkg/uri"
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
	Args:  cobra.ExactArgs(ImportCmdNumArgs),
	Run: func(cmd *cobra.Command, args []string) {
		rc := runImport(cmd, args)
		os.Exit(rc)
	},
}

var importBaseCmd = &cobra.Command{
	Use:    "import-base <repository uri> --manifest <s3 uri to manifest.json> --commit <base commit>",
	Short:  "Import data from S3 to a lakeFS repository on top of existing commit",
	Long:   "Creates a new commit with the imported data, on top of the given commit. Does not affect any branch",
	Hidden: true,
	Args:   cobra.ExactArgs(ImportCmdNumArgs),
	Run: func(cmd *cobra.Command, args []string) {
		rc := runImport(cmd, args)
		os.Exit(rc)
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

	cfg := loadConfig()
	ctx := cmd.Context()
	logger := logging.FromContext(ctx)
	dbParams := cfg.GetDatabaseParams()
	var (
		idGen        actions.IDGenerator
		actionsStore actions.Store
		storeMessage *kv.StoreMessage
		dbPool       db.Database
	)
	if dbParams.KVEnabled {
		kvParams := cfg.GetKVParams()
		kvStore, err := kv.Open(ctx, kvParams)
		if err != nil {
			logger.WithError(err).Fatal("failed to open KV store")
		}
		defer kvStore.Close()
		storeMessage = &kv.StoreMessage{Store: kvStore}

		actionsStore = actions.NewActionsKVStore(*storeMessage)
		idGen = &actions.DecreasingIDGenerator{}
	} else {
		dbPool = db.BuildDatabaseConnection(ctx, dbParams)
		defer dbPool.Close()
		err := db.ValidateSchemaUpToDate(ctx, dbPool, dbParams)
		if errors.Is(err, db.ErrSchemaNotCompatible) {
			fmt.Println("Migration version mismatch, for more information see https://docs.lakefs.io/deploying-aws/upgrade.html")
			return 1
		}
		if err != nil {
			fmt.Printf("%s\n", err)
			return 1
		}

		actionsStore = actions.NewActionsDBStore(dbPool)
		idGen = &actions.IncreasingIDGenerator{}
	}

	u := uri.Must(uri.Parse(args[0]))
	if !u.IsRepository() {
		fmt.Printf("Invalid 'repository': %s\n", uri.ErrInvalidRefURI)
		return 1
	}

	blockStore, err := factory.BuildBlockAdapter(ctx, nil, cfg)
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

	bufferedCollector := stats.NewBufferedCollector(cfg.GetFixedInstallationID(), cfg)
	defer bufferedCollector.Close()
	bufferedCollector.SetRuntimeCollector(blockStore.RuntimeStats)

	c, err := catalog.New(ctx, catalog.Config{
		Config:  cfg,
		DB:      dbPool,
		KVStore: storeMessage,
	})
	if err != nil {
		fmt.Printf("Failed to create catalog: %s\n", err)
		return 1
	}
	defer func() { _ = c.Close() }()

	actionsService := actions.NewService(
		ctx,
		actionsStore,
		catalog.NewActionsSource(c),
		catalog.NewActionsOutputWriter(c.BlockAdapter),
		idGen,
		bufferedCollector,
		cfg.GetActionsEnabled(),
	)

	// wire actions into entry catalog
	c.SetHooksHandler(actionsService)
	defer actionsService.Stop()

	repo, err := getRepository(ctx, c, repoName)
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
		Store:              c.Store,
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
	st, err := importer.Import(ctx, dryRun)
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
	fmt.Println(text.FgYellow.Sprint("Added or changed objects:"), st.AddedOrChanged)

	if dryRun {
		fmt.Println("Dry run successful. No changes were made.")
		return 0
	}

	fmt.Print(text.FgYellow.Sprint("Commit ref:"), st.CommitRef)
	fmt.Println()

	if baseCommit == "" {
		fmt.Printf("Import to branch %s finished successfully.\n", onboard.DefaultImportBranchName)
		fmt.Println()
	}

	if withMerge {
		fmt.Printf("Merging import changes into lakefs://%s/%s/\n", repoName, repo.DefaultBranch)
		msg := fmt.Sprintf(onboard.CommitMsgTemplate, st.CommitRef)
		commitLog, err := c.Merge(ctx, repoName, onboard.DefaultImportBranchName, repo.DefaultBranch, CommitterName, msg, nil, "")
		if err != nil {
			fmt.Printf("Merge failed: %s\n", err)
			return 1
		}
		fmt.Println("Merge was completed successfully.")
		fmt.Printf("To list imported objects, run:\n\t$ lakectl fs ls lakefs://%s/%s/\n", repoName, commitLog)
	} else {
		fmt.Printf("To list imported objects, run:\n\t$ lakectl fs ls lakefs://%s/%s/\n", repoName, st.CommitRef)
		fmt.Printf("To merge the changes to your main branch, run:\n\t$ lakectl merge lakefs://%s/%s lakefs://%s/%s\n", repoName, st.CommitRef, repoName, repo.DefaultBranch)
	}

	return 0
}

func getRepository(ctx context.Context, c catalog.Interface, repoName string) (*catalog.Repository, error) {
	repo, err := c.GetRepository(ctx, repoName)
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
