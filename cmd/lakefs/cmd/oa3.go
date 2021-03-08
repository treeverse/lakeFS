package cmd

import (
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/treeverse/lakefs/oa3"

	"github.com/spf13/cobra"
	"github.com/treeverse/lakefs/auth"
	"github.com/treeverse/lakefs/auth/crypt"
	"github.com/treeverse/lakefs/config"
	"github.com/treeverse/lakefs/db"
	"github.com/treeverse/lakefs/logging"
)

// runCmd represents the run command
var oa3Cmd = &cobra.Command{
	Use:   "oa3",
	Short: "Run lakeFS oa3 server",
	Run: func(cmd *cobra.Command, args []string) {
		logger := logging.Default()
		ctx := cmd.Context()
		logger.WithField("version", config.Version).Infof("lakeFS oa3")

		// validate service names and turn on the right flags
		dbParams := cfg.GetDatabaseParams()

		dbPool := db.BuildDatabaseConnection(ctx, dbParams)
		defer dbPool.Close()

		registerPrometheusCollector(dbPool)

		// init authentication
		authService := auth.NewDBAuthService(
			dbPool,
			crypt.NewSecretStore(cfg.GetAuthEncryptionSecret()),
			cfg.GetAuthCacheConfig())

		logging.Default().WithField("listen_address", cfg.GetListenAddress()).Info("starting HTTP server")
		server := &http.Server{
			Addr:    cfg.GetListenAddress(),
			Handler: oa3.NewServer(authService),
		}

		// start API server
		done := make(chan bool, 1)
		quit := make(chan os.Signal, 1)
		signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

		go func() {
			if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
				fmt.Printf("server failed to listen on %s: %v\n", cfg.GetListenAddress(), err)
				os.Exit(1)
			}
		}()

		go func() {
			<-quit // wait for handler
			_ = server.Shutdown(cmd.Context())
			close(done)
		}()

		<-done
	},
}

//nolint:gochecknoinits
func init() {
	rootCmd.AddCommand(oa3Cmd)
}
