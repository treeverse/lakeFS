package cmd

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/hashicorp/go-multierror"
	"github.com/spf13/cobra"

	"github.com/treeverse/lakefs/pkg/auth"
	"github.com/treeverse/lakefs/pkg/auth/crypt"
	auth_migrate "github.com/treeverse/lakefs/pkg/auth/migrate"
	"github.com/treeverse/lakefs/pkg/auth/model"
	"github.com/treeverse/lakefs/pkg/kv"
	"github.com/treeverse/lakefs/pkg/logging"
)

// migrateCmd represents the migrate command
var migrateCmd = &cobra.Command{
	Use:   "migrate",
	Short: "Manage migrations",
}

var versionCmd = &cobra.Command{
	Use:   "version",
	Short: "Print current migration version and available version",
	Run: func(cmd *cobra.Command, args []string) {
		cfg := loadConfig()
		kvParams, err := cfg.DatabaseParams()
		if err != nil {
			_, _ = fmt.Fprintf(os.Stderr, "KV params: %s\n", err)
			os.Exit(1)
		}
		ctx := cmd.Context()
		kvStore, err := kv.Open(ctx, kvParams)
		if err != nil {
			_, _ = fmt.Fprintf(os.Stderr, "Failed to open KV store: %s\n", err)
			os.Exit(1)
		}
		defer kvStore.Close()

		version := mustValidateSchemaVersion(ctx, kvStore)
		fmt.Printf("Database schema version: %d\n", version)
	},
}

func mustValidateSchemaVersion(ctx context.Context, kvStore kv.Store) int {
	version, err := kv.ValidateSchemaVersion(ctx, kvStore)
	if errors.Is(err, kv.ErrNotFound) {
		fmt.Fprintf(os.Stderr, "No version information - KV not initialized.\n")
		os.Exit(1)
	}
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "Failed to get schema version: %s\n", err)
		os.Exit(1)
	}
	return version
}

func ReportACL(acl model.ACL) string {
	ret := string(acl.Permission) + " on "
	if acl.Repositories.All {
		ret += "[ALL repositories]"
	} else {
		ret += strings.Join(acl.Repositories.List, ", ")
	}
	return ret
}

var authCmd = &cobra.Command{
	Use:   "auth-acl",
	Short: "Apply RBAC-to-ACL migration",
	Run: func(cmd *cobra.Command, args []string) {
		logger := logging.FromContext(cmd.Context()).WithField("phase", "migrate")
		cfg := loadConfig()
		now := time.Now()
		kvParams, err := cfg.DatabaseParams()
		if err != nil {
			_, _ = fmt.Fprintf(os.Stderr, "KV params: %s\n", err)
			os.Exit(1)
		}
		ctx := cmd.Context()
		kvStore, err := kv.Open(ctx, kvParams)
		if err != nil {
			_, _ = fmt.Fprintf(os.Stderr, "Failed to open KV store: %s\n", err)
			os.Exit(1)
		}
		defer kvStore.Close()

		authService := auth.NewAuthService(
			kvStore,
			crypt.NewSecretStore(cfg.AuthEncryptionSecret()),
			nil,
			cfg.Auth.Cache,
			logger.WithField("service", "auth_service"),
		)
		reallyUpdate, _ := cmd.Flags().GetBool("yes")

		type Warning struct {
			GroupID string
			ACL     model.ACL
			Warn    error
		}
		var groupReports []Warning

		err = auth_migrate.RBACToACL(cmd.Context(), authService, reallyUpdate, now, func(groupID string, acl model.ACL, warn error) {
			groupReports = append(groupReports, Warning{GroupID: groupID, ACL: acl, Warn: warn})
		})
		if err != nil {
			_, _ = fmt.Fprintf(os.Stderr, "Failed to upgrade RBAC policies to ACLs: %s\n", err)
			os.Exit(1)
		}

		for _, w := range groupReports {
			fmt.Printf("GROUP %s\n\tACL: %s\n", w.GroupID, ReportACL(w.ACL))
			if w.Warn != nil {
				var multi *multierror.Error
				if errors.As(w.Warn, &multi) {
					multi.ErrorFormat = multierror.ErrorFormatFunc(func(es []error) string {
						points := make([]string, len(es))
						for i, err := range es {
							points[i] = fmt.Sprintf("* %s", err)
						}
						plural := "s"
						if len(es) == 1 {
							plural = ""
						}
						return fmt.Sprintf(
							"%d change%s:\n\t%s\n",
							len(points), plural, strings.Join(points, "\n\t"))
					})
				}
				fmt.Println(w.Warn)
			}
			fmt.Println()
		}
		if !reallyUpdate {
			fmt.Println("Updated nothing.  Re-run with --yes to update!")
		}
	},
}

var upCmd = &cobra.Command{
	Use:   "up",
	Short: "Apply all up migrations",
	Run: func(cmd *cobra.Command, args []string) {
		cfg := loadConfig()
		kvParams, err := cfg.DatabaseParams()
		if err != nil {
			_, _ = fmt.Fprintf(os.Stderr, "KV params: %s\n", err)
			os.Exit(1)
		}
		ctx := cmd.Context()
		kvStore, err := kv.Open(ctx, kvParams)
		if err != nil {
			_, _ = fmt.Fprintf(os.Stderr, "Failed to open KV store: %s\n", err)
			os.Exit(1)
		}
		defer kvStore.Close()

		_ = mustValidateSchemaVersion(ctx, kvStore)
		fmt.Printf("No migrations to apply.\n")
	},
}

var gotoCmd = &cobra.Command{
	Use:   "goto",
	Short: "Migrate to version V.",
	Run: func(cmd *cobra.Command, args []string) {
		cfg := loadConfig()
		kvParams, err := cfg.DatabaseParams()
		if err != nil {
			_, _ = fmt.Fprintf(os.Stderr, "KV params: %s\n", err)
			os.Exit(1)
		}
		ctx := cmd.Context()
		kvStore, err := kv.Open(ctx, kvParams)
		if err != nil {
			_, _ = fmt.Fprintf(os.Stderr, "Failed to open KV store: %s\n", err)
			os.Exit(1)
		}
		defer kvStore.Close()

		_ = mustValidateSchemaVersion(ctx, kvStore)
		fmt.Printf("No migrations to apply.\n")
	},
}

//nolint:gochecknoinits
func init() {
	rootCmd.AddCommand(migrateCmd)
	migrateCmd.AddCommand(versionCmd)
	migrateCmd.AddCommand(upCmd)
	migrateCmd.AddCommand(gotoCmd)
	migrateCmd.AddCommand(authCmd)
	_ = gotoCmd.Flags().Uint("version", 0, "version number")
	_ = gotoCmd.MarkFlagRequired("version")
	_ = gotoCmd.Flags().Bool("force", false, "force migrate")
	_ = authCmd.Flags().Bool("yes", false, "actually upgrade, otherwise merely print warnings for the pending upgrade")
}
