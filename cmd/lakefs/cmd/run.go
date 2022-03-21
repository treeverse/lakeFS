package cmd

import (
	"context"
	"errors"
	"fmt"
	"github.com/treeverse/lakefs/cmd/lakefs/application"
	"io"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/fsnotify/fsnotify"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/treeverse/lakefs/pkg/config"
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

func NewRootCmd() *cobra.Command {
	return &cobra.Command{
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
			lakeFsCmd := application.NewLakeFsCmdContext(ctx, cfg, logger)
			logger.WithField("version", version.Version).Info("lakeFS run")

			databaseService, err := application.NewDatabaseService(lakeFsCmd)
			if err != nil {
				logger.WithError(err).Fatal("Cannot initialize database pools")
			}
			defer databaseService.Close()
			err = databaseService.RegisterPrometheusCollector()
			if err != nil {
				logging.Default().WithError(err).Error("failed to register db stats collector")
			}
			c, err := databaseService.NewCatalog(lakeFsCmd)
			if err != nil {
				logger.WithError(err).Fatal("failed to create catalog")
			}
			defer func() { _ = c.Close() }()

			multipartsTracker := databaseService.NewMultipartTracker()

			// init authentication
			authService := databaseService.NewAuthService(*cfg, version.Version)
			cloudMetadataProvider := stats.BuildMetadataProvider(logger, cfg)
			blockStore, err := application.NewBlockStore(lakeFsCmd, authService, cloudMetadataProvider)
			if err != nil {
				logger.WithError(err).Fatal("Failed to create block adapter")
			}

			// wire actions
			actionsService := application.NewActionsService(lakeFsCmd, databaseService, c, blockStore)
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
				application.CheckRepos(lakeFsCmd, authService, blockStore, c)
			}

			// update health info with installation ID
			httputil.SetHealthHandlerInfo(blockStore.InstallationID())

			// start API server
			done := make(chan bool, 1)
			quit := make(chan os.Signal, 1)
			signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

			apiHandler := application.NewAPIHandler(lakeFsCmd, databaseService, authService, blockStore, c, cloudMetadataProvider, actionsService, auditChecker)
			// init gateway server
			s3gatewayHandler := application.NewS3GatewayHandler(lakeFsCmd, multipartsTracker, c, blockStore, authService)

			ctx, cancelFn := context.WithCancel(cmd.Context())
			go blockStore.RunCollector(ctx)

			blockStore.CollectRun()

			logging.Default().WithField("listen_address", cfg.GetListenAddress()).Info("starting HTTP server")
			server := application.NewLakeFsHTTPServer(cfg.GetListenAddress(), cfg.GetS3GatewayDomainNames(), s3gatewayHandler, apiHandler)
			go func() {
				if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
					fmt.Printf("server failed to listen on %s: %v\n", cfg.GetListenAddress(), err)
					os.Exit(1)
				}
			}()

			go gracefulShutdown(cmd.Context(), quit, done, server)

			<-done
			cancelFn()
			<-blockStore.CollectionChannel()
		},
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
	runCmd := NewRootCmd()
	rootCmd.AddCommand(runCmd)

	runCmd.Flags().BoolP(mismatchedReposFlagName, "m", false, "Allow repositories from other object store types")
	if err := runCmd.Flags().MarkHidden(mismatchedReposFlagName); err != nil {
		// (internal error)
		_, _ = fmt.Fprint(os.Stderr, err)
		os.Exit(internalErrorCode)
	}
}
