package cmd

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"time"

	"github.com/spf13/cobra"
	"github.com/treeverse/lakefs/contrib/auth/acl"
	"github.com/treeverse/lakefs/pkg/auth/crypt"
	"github.com/treeverse/lakefs/pkg/kv"
	_ "github.com/treeverse/lakefs/pkg/kv/dynamodb"
	"github.com/treeverse/lakefs/pkg/kv/kvparams"
	_ "github.com/treeverse/lakefs/pkg/kv/local"
	_ "github.com/treeverse/lakefs/pkg/kv/mem"
	_ "github.com/treeverse/lakefs/pkg/kv/postgres"
	"github.com/treeverse/lakefs/pkg/logging"
)

const gracefulShutdownTimeout = 30 * time.Second

type Shutter interface {
	Shutdown(context.Context) error
}

// runCmd represents the run command
var runCmd = &cobra.Command{
	Use:   "run",
	Short: "run acl server",
	Args:  cobra.ExactArgs(0),
	Run: func(cmd *cobra.Command, args []string) {
		logger := logging.ContextUnavailable()
		cfg := loadConfig()
		kvParams, err := kvparams.NewConfig(&cfg.Database)
		if err != nil {
			logger.WithError(err).Fatal("Failed to get database params")
		}
		kvStore, err := kv.Open(cmd.Context(), kvParams)
		if err != nil {
			logger.WithError(err).Fatal("Failed to open KV store")
		}
		defer kvStore.Close()
		secretStore := crypt.NewSecretStore(cfg.AuthEncryptionSecret())
		authService := acl.NewAuthService(kvStore, secretStore, cfg.Cache)

		// Setup if needed
		if err = acl.SetupACLServer(cmd.Context(), authService); err != nil {
			logger.WithError(err).Fatal("failed to query auth service")
		}

		authAPIHandler := acl.Serve(authService, logger.WithField("service", "auth_api"))
		logger.WithField("listen_address", cfg.ListenAddress).Info("Starting Auth server")

		authAPIServer := &http.Server{
			Addr:              cfg.ListenAddress,
			ReadHeaderTimeout: time.Minute,
			Handler:           authAPIHandler,
		}
		go func() {
			if err := authAPIServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
				_, _ = fmt.Fprintf(os.Stderr, "Failed to listen on %s: %v\n", cfg.ListenAddress, err)
				os.Exit(1)
			}
		}()
		gracefulShutdown(cmd.Context(), authAPIServer)
	},
}

func gracefulShutdown(ctx context.Context, services ...Shutter) {
	<-ctx.Done()

	_, _ = fmt.Fprintf(os.Stderr, "Shutting down...\n")
	ctx, cancel := context.WithTimeout(ctx, gracefulShutdownTimeout)
	defer cancel()
	for i, service := range services {
		if err := service.Shutdown(ctx); err != nil {
			_, _ = fmt.Fprintf(os.Stderr, "Failed shutting down service [%d]: %s\n", i, err)
		}
	}
}

//nolint:gochecknoinits
func init() {
	rootCmd.AddCommand(runCmd)
}
