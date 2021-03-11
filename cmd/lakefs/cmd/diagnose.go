package cmd

import (
	"net/url"

	"github.com/spf13/cobra"
	"github.com/treeverse/lakefs/pkg/block/factory"
	"github.com/treeverse/lakefs/pkg/catalog"
	"github.com/treeverse/lakefs/pkg/db"
	"github.com/treeverse/lakefs/pkg/logging"
)

// diagnoseCmd implements the diagnose command
var diagnoseCmd = &cobra.Command{
	Use:   "diagnose",
	Short: "Diagnose underlying infrastructure configuration",
	Run: func(cmd *cobra.Command, args []string) {
		ctx := cmd.Context()
		logger := logging.Default().WithContext(ctx)
		dbPool := db.BuildDatabaseConnection(ctx, cfg.GetDatabaseParams())
		adapter, err := factory.BuildBlockAdapter(ctx, cfg)
		if err != nil {
			logger.WithError(err).Fatal("Failed to create block adapter")
		}
		c, err := catalog.New(ctx, catalog.Config{
			Config: cfg,
			DB:     dbPool,
		})
		if err != nil {
			logger.WithError(err).Fatal("Failed to create c")
		}
		defer func() { _ = c.Close() }()

		numFailures := 0
		repos, _, err := c.ListRepositories(ctx, -1, "")
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
			err = adapter.ValidateConfiguration(ctx, bucket)
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
