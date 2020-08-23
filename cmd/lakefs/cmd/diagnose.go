package cmd

import (
	"context"
	"net/url"

	"github.com/spf13/cobra"
	"github.com/treeverse/lakefs/block/factory"
	"github.com/treeverse/lakefs/catalog"
	"github.com/treeverse/lakefs/db"
	"github.com/treeverse/lakefs/logging"
)

// diagnoseCmd implements the diagnose command
var diagnoseCmd = &cobra.Command{
	Use:   "diagnose",
	Short: "Diagnose underlying infrastructure configuration",
	Run: func(cmd *cobra.Command, args []string) {
		ctx := context.Background()
		logger := logging.Default().WithContext(ctx)
		dbPool := db.BuildDatabaseConnection(cfg.GetDatabaseParams())
		adapter, err := factory.BuildBlockAdapter(cfg, dbPool)
		if err != nil {
			logger.WithError(err).Fatal("Failed to create block adapter")
		}
		cataloger := catalog.NewCataloger(dbPool)

		numFailures := 0
		repos, _, err := cataloger.ListRepositories(ctx, -1, "")
		if err != nil {
			// Cannot advance last so fail everything
			logger.WithField("error", err).Fatal("Failed to list repositories")
		}
		for _, repo := range repos {
			parsedRepo, err := url.ParseRequestURI(repo.StorageNamespace)
			if err != nil {
				logger.WithFields(logging.Fields{
					"error": err,
					"repo":  repo,
				}).Error("Failed to parse repo to get bucket")
				numFailures += 1
				continue
			}
			bucket := parsedRepo.Host
			err = adapter.ValidateConfiguration(bucket)
			if err != nil {
				logger.WithFields(logging.Fields{
					"error":      err,
					"repository": repo,
				}).Error("Configuration error found in repository")
				numFailures += 1
				continue
			}
		}
		if numFailures > 0 {
			logger.Fatalf("Configuration issues found in %d repositories", numFailures)
		}
	},
	Hidden: true,
}

//nolint:gochecknoinits
func init() {
	rootCmd.AddCommand(diagnoseCmd)
}
