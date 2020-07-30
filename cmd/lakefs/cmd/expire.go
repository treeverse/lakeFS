package cmd

import (
	"context"
	"fmt"
	"net/url"

	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3control"
	"github.com/spf13/cobra"

	"github.com/treeverse/lakefs/catalog"
	"github.com/treeverse/lakefs/config"
	"github.com/treeverse/lakefs/logging"
	"github.com/treeverse/lakefs/retention"
)

// expireCmd implements the expiren command
var expireCmd = &cobra.Command{
	Use:   "expire",
	Short: "Apply configured retention policies to expire objects",
	Run: func(cmd *cobra.Command, args []string) {
		ctx := context.Background()
		logger := logging.FromContext(ctx)
		dbPool := cfg.BuildDatabaseConnection()
		cataloger := catalog.NewCataloger(dbPool)

		awsRetentionConfig := config.NewConfig().GetAwsS3RetentionConfig()

		repos, _, err := cataloger.ListRepositories(ctx, -1, "")
		if err != nil {
			logger.WithError(err).Fatal("cannot list repositories")
		}

		// TODO(ariels: fail on failure!
		awsCfg := cfg.GetAwsConfig()

		accountId, err := config.GetAccount(awsCfg)
		if err != nil {
			logger.WithError(err).Fatal("cannot get account ID")
		}

		expiryParams := retention.ExpireOnS3Params{
			AccountId: accountId,
			RoleArn:   awsRetentionConfig.RoleArn,
			ManifestUrlForBucket: func(x string) string {
				u, err := url.Parse(x)
				if err != nil {
					panic(fmt.Sprintf("failed to create URL from %s: %s", x, err))
				}
				return awsRetentionConfig.ManifestBaseUrl.ResolveReference(u).String()
			},
		}

		s3ControlSession := session.Must(session.NewSession(awsCfg))
		s3ControlSession.ClientConfig(s3control.ServiceName)
		s3ControlClient := s3control.New(s3ControlSession)

		s3Session := session.Must(session.NewSession(awsCfg))
		s3Session.ClientConfig(s3.ServiceName)
		s3Client := s3.New(s3Session)

		retentionService := retention.NewDBRetentionService(dbPool)

		// Expire by repositories.  No immediate technical reason, but administratively
		// it is easier to understand separated logs, and safer to expire one repository
		// at a time.
		numFailures := 0
		for _, repo := range repos {
			repoLogger := logger.WithFields(logging.Fields{
				"repository": repo.Name,
				"storage":    repo.StorageNamespace,
			})
			policy, err := retentionService.GetPolicy(repo.Name)
			if err != nil {
				repoLogger.WithError(err).Error("failed to get retention policy (skip repo)")
				continue
			}
			if policy == nil {
				repoLogger.Info("no retention policy for this repository - skip")
			}
			expiryRows, err := cataloger.QueryExpired(ctx, repo.Name, &policy.Policy)
			if err != nil {
				repoLogger.WithError(err).Error("failed to query for expired (skip repo)")
				continue
			}
			expiryReader, err := retention.WriteExpiryResultsToSeekableReader(ctx, expiryRows)
			if err != nil {
				repoLogger.WithError(err).Error("failed to write expiry results (skip repo)")
				continue
			}

			retention.ExpireOnS3(ctx, s3ControlClient, s3Client, cataloger, expiryReader, &expiryParams)
		}
		if numFailures > 0 {
			logger.Fatalf("Configuration issues found in %d repositories", numFailures)
		}
	},
	Hidden: true,
}

func init() {
	rootCmd.AddCommand(expireCmd)
	// No flags
}
