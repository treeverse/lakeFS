package cmd

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/dlmiddlecote/sqlstats"
	"github.com/fsnotify/fsnotify"
	"github.com/go-ldap/ldap/v3"
	"github.com/golang-migrate/migrate/v4"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/treeverse/lakefs/pkg/actions"
	"github.com/treeverse/lakefs/pkg/api"
	"github.com/treeverse/lakefs/pkg/auth"
	"github.com/treeverse/lakefs/pkg/auth/crypt"
	"github.com/treeverse/lakefs/pkg/block"
	"github.com/treeverse/lakefs/pkg/block/factory"
	"github.com/treeverse/lakefs/pkg/catalog"
	"github.com/treeverse/lakefs/pkg/config"
	"github.com/treeverse/lakefs/pkg/db"
	"github.com/treeverse/lakefs/pkg/gateway"
	"github.com/treeverse/lakefs/pkg/gateway/multiparts"
	"github.com/treeverse/lakefs/pkg/gateway/sig"
	"github.com/treeverse/lakefs/pkg/gateway/simulator"
	"github.com/treeverse/lakefs/pkg/httputil"
	"github.com/treeverse/lakefs/pkg/logging"
	"github.com/treeverse/lakefs/pkg/stats"
	"github.com/treeverse/lakefs/pkg/version"
)

const (
	gracefulShutdownTimeout = 30 * time.Second

	mismatchedReposFlagName = "allow-mismatched-repos"
)

type Shutter interface {
	Shutdown(context.Context) error
}

func newLDAPAuthenticator(cfg *config.LDAP, service auth.Service) *auth.LDAPAuthenticator {
	const (
		connectionTimeout = 15 * time.Second
		requestTimeout    = 7 * time.Second
	)
	group := cfg.DefaultUserGroup
	if group == "" {
		group = auth.ViewersGroup
	}
	return &auth.LDAPAuthenticator{
		AuthService:       service,
		BindDN:            cfg.BindDN,
		BindPassword:      cfg.BindPassword,
		DefaultUserGroup:  group,
		UsernameAttribute: cfg.UsernameAttribute,
		MakeLDAPConn: func(_ context.Context) (*ldap.Conn, error) {
			c, err := ldap.DialURL(
				cfg.ServerEndpoint,
				ldap.DialWithDialer(&net.Dialer{Timeout: connectionTimeout}),
			)
			if err != nil {
				return nil, fmt.Errorf("dial %s: %w", cfg.ServerEndpoint, err)
			}
			c.SetTimeout(requestTimeout)
			// TODO(ariels): Support StartTLS (& other TLS configuration).
			return c, nil
		},
		BaseSearchRequest: ldap.SearchRequest{
			BaseDN:     cfg.UserBaseDN,
			Scope:      ldap.ScopeWholeSubtree,
			Filter:     cfg.UserFilter,
			Attributes: []string{cfg.UsernameAttribute},
		},
	}
}

// runCmd represents the run command
var runCmd = &cobra.Command{
	Use:   "run",
	Short: "Run lakeFS",
	Run: func(cmd *cobra.Command, args []string) {
		logger := logging.Default()
		cfg := loadConfig()
		viper.WatchConfig()
		viper.OnConfigChange(func(in fsnotify.Event) {
			lvl := viper.GetString(config.LoggingLevelKey)
			logger.WithField("toLevel", lvl).Info("Changing log level")
			logging.SetLevel(lvl)
		})

		ctx := cmd.Context()
		logger.WithField("version", version.Version).Info("lakeFS run")

		// validate service names and turn on the right flags
		dbParams := cfg.GetDatabaseParams()
		dbPool := db.BuildDatabaseConnection(ctx, dbParams)
		defer dbPool.Close()
		if err := db.ValidateSchemaUpToDate(ctx, dbPool, dbParams); errors.Is(err, db.ErrSchemaNotCompatible) {
			logger.WithError(err).Fatal("Migration version mismatch, for more information see https://docs.lakefs.io/deploying-aws/upgrade.html")
		} else if errors.Is(err, migrate.ErrNilVersion) {
			logger.Debug("No migration, setup required")
		} else if err != nil {
			logger.WithError(err).Warn("Failed on schema validation")
		}

		lockdbPool := db.BuildDatabaseConnection(ctx, dbParams)
		defer lockdbPool.Close()

		registerPrometheusCollector(dbPool)
		migrator := db.NewDatabaseMigrator(dbParams)

		c, err := catalog.New(ctx, catalog.Config{
			Config: cfg,
			DB:     dbPool,
			LockDB: lockdbPool,
		})
		if err != nil {
			logger.WithError(err).Fatal("failed to create catalog")
		}
		defer func() { _ = c.Close() }()

		multipartsTracker := multiparts.NewTracker(dbPool)

		// init authentication
		authService := auth.NewDBAuthService(
			dbPool,
			crypt.NewSecretStore(cfg.GetAuthEncryptionSecret()),
			cfg.GetAuthCacheConfig())
		var authenticator auth.Authenticator = auth.NewBuiltinAuthenticator(authService)
		var emailAuthenticator auth.Authenticator = auth.NewEmailAuthenticator(authService)
		ldapConfig := cfg.GetLDAPConfiguration()
		if ldapConfig != nil {
			ldapAuthenticator := newLDAPAuthenticator(ldapConfig, authService)
			authenticator = auth.NewChainAuthenticator(emailAuthenticator, authenticator, ldapAuthenticator)
		} else {
			authenticator = auth.NewChainAuthenticator(emailAuthenticator, authenticator)
		}
		authMetadataManager := auth.NewDBMetadataManager(version.Version, cfg.GetFixedInstallationID(), dbPool)
		cloudMetadataProvider := stats.BuildMetadataProvider(logger, cfg)
		blockstoreType := cfg.GetBlockstoreType()
		if blockstoreType == "local" || blockstoreType == "mem" {
			printLocalWarning(os.Stderr, blockstoreType)
			logger.WithField("adapter_type", blockstoreType).
				Error("Block adapter NOT SUPPORTED for production use")
		}
		metadata := stats.NewMetadata(ctx, logger, blockstoreType, authMetadataManager, cloudMetadataProvider)
		bufferedCollector := stats.NewBufferedCollector(metadata.InstallationID, cfg)
		// init block store
		blockStore, err := factory.BuildBlockAdapter(ctx, bufferedCollector, cfg)
		if err != nil {
			logger.WithError(err).Fatal("Failed to create block adapter")
		}
		bufferedCollector.SetRuntimeCollector(blockStore.RuntimeStats)
		// send metadata
		bufferedCollector.CollectMetadata(metadata)

		// wire actions
		actionsService := actions.NewService(
			ctx,
			dbPool,
			catalog.NewActionsSource(c),
			catalog.NewActionsOutputWriter(c.BlockAdapter),
			bufferedCollector,
			cfg.GetActionsEnabled(),
		)
		c.SetHooksHandler(actionsService)
		defer actionsService.Stop()

		auditChecker := version.NewDefaultAuditChecker(cfg.GetSecurityAuditCheckURL())
		defer auditChecker.Close()
		if version.Version != version.UnreleasedVersion {
			const maxSecondsToJitter = 12 * 60 * 60                                // 12h in seconds
			jitter := time.Duration(rand.Int63n(maxSecondsToJitter)) * time.Second //nolint:gosec
			interval := cfg.GetSecurityAuditCheckInterval() + jitter
			auditChecker.StartPeriodicCheck(ctx, interval, logger)
		}

		allowForeign, err := cmd.Flags().GetBool(mismatchedReposFlagName)
		if err != nil {
			fmt.Printf("%s: %s\n", mismatchedReposFlagName, err)
			os.Exit(1)
		}
		if !allowForeign {
			checkRepos(ctx, logger, authMetadataManager, blockStore, c)
		}

		// update health info with installation ID
		httputil.SetHealthHandlerInfo(metadata.InstallationID)

		// start API server
		done := make(chan bool, 1)
		quit := make(chan os.Signal, 1)
		signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

		apiHandler := api.Serve(
			cfg,
			c,
			authenticator,
			authService,
			blockStore,
			authMetadataManager,
			migrator,
			bufferedCollector,
			cloudMetadataProvider,
			actionsService,
			auditChecker,
			logger.WithField("service", "api_gateway"),
			cfg.GetS3GatewayDomainNames(),
		)

		// init gateway server
		s3Fallback := cfg.GetS3GatewayFallbackURL()
		var s3FallbackURL *url.URL
		if s3Fallback != "" {
			s3FallbackURL, err = url.Parse(s3Fallback)
			if err != nil {
				logger.WithError(err).Fatal("Failed to parse s3 fallback URL")
			}
		}
		s3gatewayHandler := gateway.NewHandler(
			cfg.GetS3GatewayRegion(),
			c,
			multipartsTracker,
			blockStore,
			authService,
			cfg.GetS3GatewayDomainNames(),
			bufferedCollector,
			s3FallbackURL,
			cfg.GetLoggingTraceRequestHeaders(),
		)
		ctx, cancelFn := context.WithCancel(cmd.Context())
		bufferedCollector.Run(ctx)
		defer bufferedCollector.Close()

		bufferedCollector.CollectEvent("global", "run")

		logging.Default().WithField("listen_address", cfg.GetListenAddress()).Info("starting HTTP server")
		server := &http.Server{
			Addr: cfg.GetListenAddress(),
			Handler: http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
				// If the request has the S3 GW domain (exact or subdomain) - or carries an AWS sig, serve S3GW
				if httputil.HostMatches(request, cfg.GetS3GatewayDomainNames()) ||
					httputil.HostSubdomainOf(request, cfg.GetS3GatewayDomainNames()) ||
					sig.IsAWSSignedRequest(request) {
					s3gatewayHandler.ServeHTTP(writer, request)
					return
				}

				// Otherwise, serve the API handler
				apiHandler.ServeHTTP(writer, request)
			}),
		}

		go func() {
			if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
				fmt.Printf("server failed to listen on %s: %v\n", cfg.GetListenAddress(), err)
				os.Exit(1)
			}
		}()

		go gracefulShutdown(cmd.Context(), quit, done, server)

		<-done
		cancelFn()
	},
}

// checkRepos iterates on all repos and validates that their settings are correct.
func checkRepos(ctx context.Context, logger logging.Logger, authMetadataManager *auth.DBMetadataManager, blockStore block.Adapter, c *catalog.Catalog) {
	initialized, err := authMetadataManager.IsInitialized(ctx)
	if err != nil {
		logger.WithError(err).Fatal("Failed to check if lakeFS is initialized")
	}
	if !initialized {
		logger.Debug("lakeFS isn't initialized, skipping mismatched adapter checks")
	} else {
		logger.
			WithField("adapter_type", blockStore.BlockstoreType()).
			Debug("lakeFS is initialized, checking repositories for mismatched adapter")
		hasMore := true
		next := ""

		for hasMore {
			var err error
			var repos []*catalog.Repository
			repos, hasMore, err = c.ListRepositories(ctx, -1, "", next)
			if err != nil {
				logger.WithError(err).Fatal("Checking existing repositories failed")
			}

			adapterStorageType := blockStore.BlockstoreType()
			for _, repo := range repos {
				nsURL, err := url.Parse(repo.StorageNamespace)
				if err != nil {
					logger.WithError(err).Fatalf("Failed to parse repository %s namespace '%s'", repo.Name, repo.StorageNamespace)
				}
				repoStorageType, err := block.GetStorageType(nsURL)
				if err != nil {
					logger.WithError(err).Fatalf("Failed to parse to parse storage type '%s'", nsURL)
				}

				checkForeignRepo(repoStorageType, logger, adapterStorageType, repo.Name)
				checkMetadataPrefix(ctx, repo, logger, blockStore, repoStorageType)

				next = repo.Name
			}
		}
	}
}

// checkMetadataPrefix checks for non-migrated repos of issue #2397 (https://github.com/treeverse/lakeFS/issues/2397)
func checkMetadataPrefix(ctx context.Context, repo *catalog.Repository, logger logging.Logger, adapter block.Adapter, repoStorageType block.StorageType) {
	if repoStorageType != block.StorageTypeGS &&
		repoStorageType != block.StorageTypeAzure {
		return
	}

	const dummyFile = "dummy"
	if _, err := adapter.Get(ctx, block.ObjectPointer{
		StorageNamespace: repo.StorageNamespace,
		Identifier:       dummyFile,
	}, -1); err != nil {
		logger.WithFields(logging.Fields{
			"path":              dummyFile,
			"storage_namespace": repo.StorageNamespace,
		}).Fatal("Can't find dummy file in storage namespace, did you run the migration? " +
			"(https://docs.lakefs.io/reference/upgrade.html#data-migration-for-version-v0500)")
	}
}

// checkForeignRepo checks whether a repo storage namespace matches the block adapter.
// A foreign repo is a repository which namespace doesn't match the current block adapter.
// A foreign repo might exists if the lakeFS instance configuration changed after a repository was
// already created. The behaviour of lakeFS for foreign repos is undefined and should be blocked.
func checkForeignRepo(repoStorageType block.StorageType, logger logging.Logger, adapterStorageType, repoName string) {
	if adapterStorageType != repoStorageType.BlockstoreType() {
		logger.Fatalf("Mismatched adapter detected. lakeFS started with adapter of type '%s', but repository '%s' is of type '%s'",
			adapterStorageType, repoName, repoStorageType.BlockstoreType())
	}
}

const runBanner = `

     ██╗      █████╗ ██╗  ██╗███████╗███████╗███████╗
     ██║     ██╔══██╗██║ ██╔╝██╔════╝██╔════╝██╔════╝
     ██║     ███████║█████╔╝ █████╗  █████╗  ███████╗
     ██║     ██╔══██║██╔═██╗ ██╔══╝  ██╔══╝  ╚════██║
     ███████╗██║  ██║██║  ██╗███████╗██║     ███████║
     ╚══════╝╚═╝  ╚═╝╚═╝  ╚═╝╚══════╝╚═╝     ╚══════╝

│
│ If you're running lakeFS locally for the first time,
│     complete the setup process at http://127.0.0.1:8000/setup
│

│
│ For more information on how to use lakeFS,
│     check out the docs at https://docs.lakefs.io/quickstart/repository
│

│
│ For support or any other question,
│     join our Slack channel https://docs.lakefs.io/slack
│

`

func printWelcome(w io.Writer) {
	_, _ = fmt.Fprint(w, runBanner)
	_, _ = fmt.Fprintf(w, "Version %s\n\n", version.Version)
}

var localWarningBanner = `
WARNING!

Using the "%s" block adapter.  This is suitable only for testing, but not
for production.
`

func printLocalWarning(w io.Writer, adapter string) {
	_, _ = fmt.Fprintf(w, localWarningBanner, adapter)
}

func registerPrometheusCollector(db sqlstats.StatsGetter) {
	collector := sqlstats.NewStatsCollector("lakefs", db)
	err := prometheus.Register(collector)
	if err != nil {
		logging.Default().WithError(err).Error("failed to register db stats collector")
	}
}

func gracefulShutdown(ctx context.Context, quit <-chan os.Signal, done chan<- bool, servers ...Shutter) {
	logger := logging.Default()
	logger.WithField("version", version.Version).Info("Up and running (^C to shutdown)...")

	printWelcome(os.Stderr)

	<-quit
	logger.Warn("shutting down...")

	ctx, cancel := context.WithTimeout(ctx, gracefulShutdownTimeout)
	defer cancel()

	for i, server := range servers {
		if err := server.Shutdown(ctx); err != nil {
			fmt.Printf("Error while shutting down service (%d): %s\n", i, err)
		}
	}
	simulator.ShutdownRecorder()
	close(done)
}

//nolint:gochecknoinits
func init() {
	rootCmd.AddCommand(runCmd)

	runCmd.Flags().BoolP(mismatchedReposFlagName, "m", false, "Allow repositories from other object store types")
	if err := runCmd.Flags().MarkHidden(mismatchedReposFlagName); err != nil {
		// (internal error)
		_, _ = fmt.Fprint(os.Stderr, err)
		os.Exit(internalErrorCode)
	}
}
