package cmd

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/vbauerster/mpb/v5"
	"github.com/vbauerster/mpb/v5/decor"

	"github.com/jedib0t/go-pretty/text"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/treeverse/lakefs/uri"

	"github.com/treeverse/lakefs/block/factory"
	"github.com/treeverse/lakefs/catalog"
	"github.com/treeverse/lakefs/config"
	"github.com/treeverse/lakefs/db"
	"github.com/treeverse/lakefs/logging"
	"github.com/treeverse/lakefs/onboard"
)

var ErrInvalid = errors.New("validation")

const (
	DryRunFlagName      = "dry-run"
	ManifestURLFlagName = "manifest"
	ManifestURLFormat   = "s3://example-bucket/inventory/YYYY-MM-DDT00-00Z/manifest.json"
	ImportCmdNumArgs    = 1
)

var importCmd = &cobra.Command{
	Use:   "import <repository uri> --manifest <s3 uri to manifest.json>",
	Short: "Import data from S3 to a lakeFS repository",
	Long:  "Import from an S3 inventory to lakeFS without copying the data.",
	Args: func(cmd *cobra.Command, args []string) error {
		if len(args) != ImportCmdNumArgs {
			return fmt.Errorf("%w - expected %d arguments", ErrInvalid, ImportCmdNumArgs)
		}
		err := uri.ValidateRepoURI(args[0])
		if err != nil {
			return fmt.Errorf("%w - %s is not a valid repo uri", ErrInvalid, args[0])
		}
		return nil
	},
	Run: func(cmd *cobra.Command, args []string) {
		dryRun, _ := cmd.Flags().GetBool(DryRunFlagName)
		manifestURL, _ := cmd.Flags().GetString(ManifestURLFlagName)
		ctx := context.Background()
		conf := config.NewConfig()
		err := db.ValidateSchemaUpToDate(conf.GetDatabaseParams())
		if err != nil {
			fmt.Printf("%v\n", err)
			os.Exit(1)
		}
		viper.Set("database.disable_auto_migrate", true)
		logger := logging.FromContext(ctx)
		dbPool := db.BuildDatabaseConnection(cfg.GetDatabaseParams())
		cataloger := catalog.NewCataloger(dbPool, catalog.WithParams(conf.GetCatalogerCatalogParams()))
		u := uri.Must(uri.Parse(args[0]))
		blockStore, err := factory.BuildBlockAdapter(cfg)
		if err != nil {
			fmt.Printf("failed to create block adapter: %v\n", err)
			os.Exit(1)
		}
		repoName := u.Repository
		parsedURL, err := url.Parse(manifestURL)
		if err != nil || parsedURL.Scheme != "s3" || !strings.HasSuffix(parsedURL.Path, "/manifest.json") {
			fmt.Printf("invalid manifest url. expected format: %s\n", ManifestURLFormat)
			os.Exit(1)
		}
		repo, err := cataloger.GetRepository(ctx, repoName)
		if err != nil {
			fmt.Printf("failed to read repository %s: %v\n", repoName, err)
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
			CommitUsername:     "lakefs",
			InventoryURL:       manifestURL,
			Repository:         repoName,
			InventoryGenerator: blockStore,
			Cataloger:          cataloger,
		}

		importer, err := onboard.CreateImporter(ctx, logger, importConfig)
		if err != nil {
			fmt.Printf("import failed: %v\n", err)
			os.Exit(1)
		}
		progressDone := manageProgress(importer)
		stats, err := importer.Import(ctx, dryRun)
		if err != nil {
			close(progressDone)
			fmt.Printf("import failed: %v\n", err)
			os.Exit(1)
		}
		close(progressDone)
		fmt.Print(text.FgYellow.Sprint("Added or changed objects: "), fmt.Sprintf("%d\n", *stats.AddedOrChanged))
		fmt.Print(text.FgYellow.Sprint("Deleted objects: "), fmt.Sprintf("%d\n", *stats.Deleted))
		if stats.PreviousInventoryURL != "" {
			fmt.Print(text.FgYellow.Sprint("Previously imported inventory: "), fmt.Sprintf("%s\n", stats.PreviousInventoryURL))
			fmt.Print(text.FgYellow.Sprint("Previous import date: "), fmt.Sprintf("%v\n\n", stats.PreviousImportDate))
		}
		if !dryRun {
			fmt.Print(text.FgYellow.Sprint("Commit ref: "), fmt.Sprintf("%s\n\n", stats.CommitRef))
			fmt.Printf("Import to branch %s finished successfully.\n", onboard.DefaultBranchName)
			fmt.Printf("To list imported objects, run:\n\t$ lakectl fs ls lakefs://%s@%s/\n", repoName, stats.CommitRef)
			fmt.Printf("To merge the changes to your main branch, run:\n\t$ lakectl merge lakefs://%s@%s lakefs://%s@%s\n", repoName, onboard.DefaultBranchName, repoName, repo.DefaultBranch)
		} else {
			fmt.Println("Dry run successful. No changes were made.")
		}
	},
}

func manageProgress(importer *onboard.Importer) chan bool {
	ticker := time.NewTicker(50 * time.Millisecond)
	multi := mpb.New(mpb.WithWidth(60))
	done := make(chan bool)
	bars := make(map[string]*mpb.Bar)

	go func() {
		for {
			select {
			case <-done:
				return
			case <-ticker.C:
				progress := importer.Progress()
				for _, p := range progress {
					b, ok := bars[p.Label]
					if !ok {
						total := p.Total
						labelLength := 40
						if total == -1 {
							total = p.Current + 1
							b = multi.AddSpinner(int64(total), mpb.SpinnerOnMiddle, mpb.SpinnerStyle([]string{"∙∙∙", "●∙∙", "∙●∙", "∙∙●", "∙∙∙"}),
								mpb.PrependDecorators(decor.Name(p.Label, decor.WC{W: labelLength, C: decor.DidentRight})),
								mpb.PrependDecorators(decor.CurrentNoUnit("%d ", decor.WC{W: 20})),
								mpb.PrependDecorators(decor.Name("[")),
								mpb.AppendDecorators(decor.Name("]")),
							)
						} else {
							b = multi.AddBar(int64(total), mpb.BarStyle(" =>- <"),
								mpb.PrependDecorators(decor.Name(p.Label, decor.WC{W: labelLength, C: decor.DidentRight})),
								mpb.PrependDecorators(decor.CountersNoUnit("%d / %d ", decor.WC{W: 20})),
								mpb.PrependDecorators(decor.Name("[")),
								mpb.AppendDecorators(decor.Name("]")),
							)
						}
						bars[p.Label] = b
					}
					if p.Total != -1 {
						b.SetTotal(int64(p.Total), false)
					} else {
						b.SetTotal(int64(p.Current+1), false)
					}
					b.SetCurrent(int64(p.Current))
				}
			}
		}
	}()
	return done
}

func prepareBranch(ctx context.Context, cataloger catalog.Cataloger, repo *catalog.Repository, branch string) error {
	repoName := repo.Name
	_, err := cataloger.GetBranchReference(ctx, repoName, branch)
	if errors.Is(err, db.ErrNotFound) {
		fmt.Printf("Branch %s does not exist, creating.\n\n", branch)
		_, err = cataloger.CreateBranch(ctx, repoName, branch, repo.DefaultBranch)
		if err != nil {
			return fmt.Errorf("failed to create branch %s in repo %s: %v", branch, repoName, err)
		}
	} else if err != nil {
		return fmt.Errorf("error when fetching branches for repo %s: %v", repoName, err)
	} else {
		fmt.Printf("Branch %s already exists, no need to create it.\n\n", branch)
	}
	return nil
}

//nolint:gochecknoinits
func init() {
	rootCmd.AddCommand(importCmd)
	importCmd.Flags().Bool(DryRunFlagName, false, "Only read inventory and print stats, without making any changes")
	importCmd.Flags().StringP(ManifestURLFlagName, "m", "", fmt.Sprintf("S3 uri to the manifest.json to use for the import. Format: %s", ManifestURLFormat))
	_ = importCmd.MarkFlagRequired(ManifestURLFlagName)
}
