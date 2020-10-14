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
	"github.com/treeverse/lakefs/block/factory"
	"github.com/treeverse/lakefs/catalog"
	"github.com/treeverse/lakefs/cmdutils"
	"github.com/treeverse/lakefs/config"
	"github.com/treeverse/lakefs/db"
	"github.com/treeverse/lakefs/logging"
	"github.com/treeverse/lakefs/onboard"
	"github.com/treeverse/lakefs/uri"
)

const (
	DryRunFlagName           = "dry-run"
	CreateRepoFlagName       = "create-repo"
	StorageNamespaceFlagName = "storage-namespace"
	WithMergeFlagName        = "with-merge"
	ManifestURLFlagName      = "manifest"
	ManifestURLFormat        = "s3://example-bucket/inventory/YYYY-MM-DDT00-00Z/manifest.json"
	ImportCmdNumArgs         = 1
	CommitterName            = "lakefs"
)

var (
	ErrRequired                = errors.New("required")
	ErrRepositoryAlreadyExists = errors.New("repository already exists")
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
		createRepo, _ := flags.GetBool(CreateRepoFlagName)
		storageNamespace, _ := flags.GetString(StorageNamespaceFlagName)

		if createRepo && storageNamespace == "" {
			fmt.Printf("when specifying the --create-repo flag, --storage-namespace must also be specified\n")
			os.Exit(1)
		}

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

		repo, err := prepareRepo(ctx, dryRun, cataloger, createRepo, repoName, storageNamespace)
		if err != nil {
			fmt.Printf("Error %s\n", err)
			if errors.Is(err, catalog.ErrBranchNotFound) {
				fmt.Println("Import will operate only on repositories created by import --create-repo")
			}
			os.Exit(1)
		}

		if dryRun {
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
		fmt.Println()
		if withMerge {
			fmt.Printf("Merging import changes into lakefs://%s@%s/\n", repoName, repo.DefaultBranch)
			msg := fmt.Sprintf(onboard.CommitMsgTemplate, stats.CommitRef)
			commitLog, err := cataloger.Merge(ctx, repoName, onboard.DefaultBranchName, repo.DefaultBranch, CommitterName, msg, nil)
			if err != nil {
				fmt.Printf("Merge failed: %s\n", err)
				os.Exit(1)
			}
			fmt.Println("Merge was completed successfully.")
			fmt.Printf("To list imported objects, run:\n\t$ lakectl fs ls lakefs://%s@%s/\n", repoName, commitLog.Reference)
		} else {
			fmt.Printf("To list imported objects, run:\n\t$ lakectl fs ls lakefs://%s@%s/\n", repoName, stats.CommitRef)
			fmt.Printf("To merge the changes to your main branch, run:\n\t$ lakectl merge lakefs://%s@%s lakefs://%s@%s\n", repoName, onboard.DefaultBranchName, repoName, repo.DefaultBranch)
		}
	},
}

func prepareRepo(ctx context.Context, dryRun bool, cataloger catalog.Cataloger, createRepo bool, repoName string, storageNamespace string) (*catalog.Repository, error) {
	if dryRun {
		return &catalog.Repository{
			Name:          repoName,
			DefaultBranch: catalog.DefaultBranchName,
		}, nil
	}
	var err error
	if createRepo {
		err = createRepository(ctx, cataloger, repoName, storageNamespace)
		if err != nil {
			return nil, err
		}
	}
	return getRepository(ctx, cataloger, repoName)
}

func getRepository(ctx context.Context, cataloger catalog.Cataloger, repoName string) (*catalog.Repository, error) {
	repo, err := cataloger.GetRepository(ctx, repoName)
	if err != nil {
		return nil, fmt.Errorf("read repository %s: %w", repoName, err)
	}
	// check we have import branch on this repo
	importBranchExists, err := cataloger.BranchExists(ctx, repoName, onboard.DefaultBranchName)
	if err != nil {
		return nil, fmt.Errorf("read branch (%s) information from repository %s: %w", onboard.DefaultBranchName, repoName, err)
	}
	if !importBranchExists {
		return nil, fmt.Errorf("import %w in repository: %s", catalog.ErrBranchNotFound, repoName)
	}
	return repo, nil
}

func createRepository(ctx context.Context, cataloger catalog.Cataloger, repoName, storageNamespace string) error {
	// make sure the repo does not exists
	_, err := cataloger.GetRepository(ctx, repoName)
	if err == nil {
		return fmt.Errorf("%w: %s", ErrRepositoryAlreadyExists, repoName)
	}
	// create repository with import branch
	_, err = cataloger.CreateRepository(ctx, repoName, storageNamespace, onboard.DefaultBranchName)
	if err != nil {
		return fmt.Errorf("create repository %s: %w", repoName, err)
	}
	// create default branch and make it default
	_, err = cataloger.CreateBranch(ctx, repoName, catalog.DefaultBranchName, onboard.DefaultBranchName)
	if err != nil {
		return fmt.Errorf("create default branch on repository %s: %w", repoName, err)
	}
	err = cataloger.SetDefaultBranch(ctx, repoName, catalog.DefaultBranchName)
	if err != nil {
		return fmt.Errorf("set default branch to %s on %s: %w", catalog.DefaultBranchName, repoName, err)
	}
	return nil
}

//nolint:gochecknoinits
func init() {
	rootCmd.AddCommand(importCmd)
	importCmd.Flags().Bool(DryRunFlagName, false, "Only read inventory and print stats, without making any changes")
	importCmd.Flags().StringP(ManifestURLFlagName, "m", "", fmt.Sprintf("S3 uri to the manifest.json to use for the import. Format: %s", ManifestURLFormat))
	_ = importCmd.MarkFlagRequired(ManifestURLFlagName)
	importCmd.Flags().Bool(WithMergeFlagName, false, "Merge imported data to the repository's main branch")
	importCmd.Flags().Bool(CreateRepoFlagName, false, "Create repository for initial import")
	importCmd.Flags().String(StorageNamespaceFlagName, "", "Storage namespace used for new repository")
}
