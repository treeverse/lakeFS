package application

import (
	"github.com/treeverse/lakefs/pkg/actions"
	"github.com/treeverse/lakefs/pkg/api"
	"github.com/treeverse/lakefs/pkg/catalog"
	"github.com/treeverse/lakefs/pkg/cloud"
	"github.com/treeverse/lakefs/pkg/gateway"
	"github.com/treeverse/lakefs/pkg/gateway/multiparts"
	"github.com/treeverse/lakefs/pkg/version"
	"net/http"
	"net/url"
)

func NewAPIHandler(cmd LakeFsCmdContext, databaseService *DatabaseService, authService *AuthService, blockStore *BlockStore, c *catalog.Catalog, cloudMetadataProvider cloud.MetadataProvider, actionsService *actions.Service, auditChecker *version.AuditChecker) http.Handler {
	return api.Serve(
		cmd.cfg,
		c,
		authService.authenticator,
		authService.dbAuthService,
		blockStore.blockAdapter,
		authService.authMetadataManager,
		databaseService.migrator,
		blockStore.bufferedCollector,
		cloudMetadataProvider,
		actionsService,
		auditChecker,
		cmd.logger.WithField("service", "api_gateway"),
		cmd.cfg.GetS3GatewayDomainNames(),
	)
}

func NewS3GatewayHandler(cmd LakeFsCmdContext, multipartsTracker multiparts.Tracker, c *catalog.Catalog, blockStore *BlockStore, authService *AuthService) http.Handler {
	cfg := cmd.cfg
	var err error
	s3Fallback := cfg.GetS3GatewayFallbackURL()
	var s3FallbackURL *url.URL
	if s3Fallback != "" {
		s3FallbackURL, err = url.Parse(s3Fallback)
		if err != nil {
			cmd.logger.WithError(err).Fatal("Failed to parse s3 fallback URL")
		}
	}
	return gateway.NewHandler(
		cfg.GetS3GatewayRegion(),
		c,
		multipartsTracker,
		blockStore.blockAdapter,
		authService.dbAuthService,
		cfg.GetS3GatewayDomainNames(),
		blockStore.bufferedCollector,
		s3FallbackURL,
		cfg.GetLoggingTraceRequestHeaders(),
	)
}
