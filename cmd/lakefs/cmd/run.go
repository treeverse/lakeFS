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
	"strings"
	"syscall"
	"time"

	"github.com/coreos/go-oidc/v3/oidc"
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
	"github.com/treeverse/lakefs/pkg/auth/email"
	"github.com/treeverse/lakefs/pkg/block"
	"github.com/treeverse/lakefs/pkg/block/factory"
	"github.com/treeverse/lakefs/pkg/catalog"
	"github.com/treeverse/lakefs/pkg/config"
	"github.com/treeverse/lakefs/pkg/db"
	"github.com/treeverse/lakefs/pkg/gateway"
	"github.com/treeverse/lakefs/pkg/gateway/multiparts"
	"github.com/treeverse/lakefs/pkg/gateway/sig"
	"github.com/treeverse/lakefs/pkg/httputil"
	"github.com/treeverse/lakefs/pkg/kv"
	"github.com/treeverse/lakefs/pkg/kv/params"
	kvpg "github.com/treeverse/lakefs/pkg/kv/postgres"
	"github.com/treeverse/lakefs/pkg/logging"
	"github.com/treeverse/lakefs/pkg/stats"
	"github.com/treeverse/lakefs/pkg/templater"
	"github.com/treeverse/lakefs/pkg/version"
	"github.com/treeverse/lakefs/templates"
	"golang.org/x/oauth2"
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

		var (
			kvStore    kv.Store
			kvParams   params.KV
			dbPool     db.Database
			lockDBPool db.Database
			err        error
		)
		if dbParams.KVEnabled {
			kvParams = cfg.GetKVParams()
			kvStore, err = kv.Open(ctx, kvParams)
			if err != nil {
				logger.WithError(err).Fatal("Failed to open KV store")
			}
			defer kvStore.Close()

			// Check if migration required only on postgres
			migrationRequired := false
			if dbParams.Type == kvpg.DriverName && len(dbParams.ConnectionString) > 0 {
				dbPool = db.BuildDatabaseConnection(ctx, dbParams)
				defer dbPool.Close()
				_, _, err := db.MigrateVersion(ctx, dbPool, dbParams)
				if err == nil {
					migrationRequired = true
				} else if !errors.Is(err, migrate.ErrNilVersion) {
					logger.WithError(err).Fatal("Failed to get schema version")
				}
			}
			err = kv.ValidateSchemaVersion(ctx, kvStore, migrationRequired)
			if err != nil {
				logger.WithError(err).Fatal("Failure on schema validation")
			}
		} else {
			dbPool = db.BuildDatabaseConnection(ctx, dbParams)
			defer dbPool.Close()
			lockDBPool = db.BuildDatabaseConnection(ctx, dbParams)
			defer lockDBPool.Close()

			if err := db.ValidateSchemaUpToDate(ctx, dbPool, dbParams); errors.Is(err, db.ErrSchemaNotCompatible) {
				logger.WithError(err).Fatal("Migration version mismatch, for more information see https://docs.lakefs.io/deploying-aws/upgrade.html")
			} else if errors.Is(err, migrate.ErrNilVersion) {
				logger.Debug("No migration, setup required")
			} else if err != nil {
				logger.WithError(err).Warn("Failure on schema validation")
			}
		}
		registerPrometheusCollector(dbPool)

		var (
			multipartsTracker   multiparts.Tracker
			actionsStore        actions.Store
			authMetadataManager auth.MetadataManager
			storeMessage        *kv.StoreMessage
			migrator            db.Migrator
		)
		emailParams, _ := cfg.GetEmailParams()
		emailer, err := email.NewEmailer(emailParams)
		if err != nil {
			logger.WithError(err).Fatal("Emailer has not been properly configured, check the values in sender field")
		}

		var idGen actions.IDGenerator
		if dbParams.KVEnabled {
			migrator = kv.NewDatabaseMigrator(kvParams)
			storeMessage = &kv.StoreMessage{Store: kvStore}
			multipartsTracker = multiparts.NewTracker(*storeMessage)
			actionsStore = actions.NewActionsKVStore(*storeMessage)
			authMetadataManager = auth.NewKVMetadataManager(version.Version, cfg.GetFixedInstallationID(), cfg.GetDatabaseParams().Type, kvStore)
			idGen = &actions.DecreasingIDGenerator{}
		} else {
			migrator = db.NewDatabaseMigrator(dbParams)
			multipartsTracker = multiparts.NewDBTracker(dbPool)
			actionsStore = actions.NewActionsDBStore(dbPool)
			authMetadataManager = auth.NewDBMetadataManager(version.Version, cfg.GetFixedInstallationID(), dbPool)
			idGen = &actions.IncreasingIDGenerator{}
		}

		// initialize auth service
		var authService auth.Service
		switch {
		case cfg.IsAuthTypeAPI():
			var apiEmailer *email.Emailer
			if !cfg.GetAuthAPISupportsInvites() {
				// invites not supported by API - delegate it to emailer
				apiEmailer = emailer
			}
			authService, err = auth.NewAPIAuthService(
				cfg.GetAuthAPIEndpoint(),
				cfg.GetAuthAPIToken(),
				crypt.NewSecretStore(cfg.GetAuthEncryptionSecret()),
				cfg.GetAuthCacheConfig(), nil, apiEmailer)
			if err != nil {
				logger.WithError(err).Fatal("failed to create authentication service")
			}
		case dbParams.KVEnabled:
			authService = auth.NewKVAuthService(
				storeMessage,
				crypt.NewSecretStore(cfg.GetAuthEncryptionSecret()),
				emailer,
				cfg.GetAuthCacheConfig(),
				logger.WithField("service", "auth_service"),
			)
		default:
			authService = auth.NewDBAuthService(
				dbPool,
				crypt.NewSecretStore(cfg.GetAuthEncryptionSecret()),
				emailer,
				cfg.GetAuthCacheConfig(),
				logger.WithField("service", "auth_service"),
			)
		}

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

		c, err := catalog.New(ctx, catalog.Config{
			Config:  cfg,
			DB:      dbPool,
			LockDB:  lockDBPool,
			KVStore: storeMessage,
		})
		if err != nil {
			logger.WithError(err).Fatal("failed to create catalog")
		}
		defer func() { _ = c.Close() }()

		templater := templater.NewService(templates.Content, cfg, authService)

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
		defer actionsService.Stop()
		c.SetHooksHandler(actionsService)

		middlewareAuthenticator := auth.ChainAuthenticator{
			auth.NewBuiltinAuthenticator(authService),
		}
		ldapConfig := cfg.GetLDAPConfiguration()
		if ldapConfig != nil {
			middlewareAuthenticator = append(middlewareAuthenticator, newLDAPAuthenticator(ldapConfig, authService))
		}
		controllerAuthenticator := append(middlewareAuthenticator, auth.NewEmailAuthenticator(authService))

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

		oidcConfig := cfg.GetAuthOIDCConfiguration()
		var oauthConfig *oauth2.Config
		var oidcProvider *oidc.Provider
		if oidcConfig.Enabled {
			oidcProvider, err = oidc.NewProvider(
				cmd.Context(),
				oidcConfig.URL,
			)
			if err != nil {
				logger.WithError(err).Fatal("Failed to initialize OIDC provider")
			}
			oauthConfig = &oauth2.Config{
				ClientID:     oidcConfig.ClientID,
				ClientSecret: oidcConfig.ClientSecret,
				RedirectURL:  strings.TrimSuffix(oidcConfig.CallbackBaseURL, "/") + api.BaseURL + "/oidc/callback",
				Endpoint:     oidcProvider.Endpoint(),
				Scopes:       []string{oidc.ScopeOpenID, "profile"},
			}
		}
		apiHandler := api.Serve(
			cfg,
			c,
			middlewareAuthenticator,
			controllerAuthenticator,
			authService,
			blockStore,
			authMetadataManager,
			migrator,
			bufferedCollector,
			cloudMetadataProvider,
			actionsService,
			auditChecker,
			logger.WithField("service", "api_gateway"),
			emailer,
			templater,
			cfg.GetS3GatewayDomainNames(),
			cfg.GetUISnippets(),
			oidcProvider,
			oauthConfig,
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

		lakefsBaseURL := emailParams.LakefsBaseURL
		if lakefsBaseURL != "" {
			_, err := url.Parse(lakefsBaseURL)
			if err != nil {
				logger.WithError(err).Warn(fmt.Sprintf("Failed to parse lakefs base url for email, check the value in %s", config.LakefsEmailBaseURLKey))
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
			cfg.GetAuditLogLevel(),
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
func checkRepos(ctx context.Context, logger logging.Logger, authMetadataManager auth.MetadataManager, blockStore block.Adapter, c *catalog.Catalog) {
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
// A foreign repo might exist if the lakeFS instance configuration changed after a repository was
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

const localWarningBanner = `
WARNING!

Using the "%s" block adapter.  This is suitable only for testing, but not for production.
`

func printLocalWarning(w io.Writer, adapter string) {
	_, _ = fmt.Fprintf(w, localWarningBanner, adapter)
}

func registerPrometheusCollector(db sqlstats.StatsGetter) {
	if db == nil { // TODO (niro): WA for KV stats collector until https://github.com/treeverse/lakeFS/issues/3869 is resolved
		return
	}
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
