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
	"github.com/treeverse/lakefs/pkg/config"
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
	return string(acl.Permission) + " on [ALL repositories]"
}

func migrateToACL(ctx context.Context, kvStore kv.Store, cfg *config.Config, logger logging.Logger, reallyUpdate bool, updateTime time.Time, printMessages bool) bool {
	authService := auth.NewAuthService(
		kvStore,
		crypt.NewSecretStore(cfg.AuthEncryptionSecret()),
		nil,
		cfg.Auth.Cache,
		logger.WithField("service", "auth_service"),
	)

	type Warning struct {
		GroupID string
		ACL     model.ACL
		Warn    error
	}
	var groupReports []Warning

	usersWithPolicies, err := auth_migrate.RBACToACL(ctx, authService, reallyUpdate, updateTime, func(groupID string, acl model.ACL, warn error) {
		groupReports = append(groupReports, Warning{GroupID: groupID, ACL: acl, Warn: warn})
	})
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "Failed to upgrade RBAC policies to ACLs: %s\n", err)
		os.Exit(1)
	}
	hasWarnings := false
	for _, w := range groupReports {
		if printMessages {
			fmt.Printf("GROUP %s\n\tACL: %s\n", w.GroupID, ReportACL(w.ACL))
		}
		if w.Warn != nil {
			hasWarnings = true
			if printMessages {
				var multi *multierror.Error
				if errors.As(w.Warn, &multi) {
					multi.ErrorFormat = func(es []error) string {
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
					}
				}
				fmt.Println(w.Warn)
			}
		}
		fmt.Println()
	}
	for _, username := range usersWithPolicies {
		if printMessages {
			fmt.Printf("USER (%s)  detaching directly-attached policies\n", username)
		}
	}
	return hasWarnings || len(usersWithPolicies) == 0
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

		force, _ := cmd.Flags().GetBool("force")

		// -- This part contains the migration to ACL and will be removed in next version

		// skip migrate to ACL for users with External authorizations
		if cfg.IsAuthTypeAPI() {
			fmt.Println("skipping ACL migration - external Authorization")
		} else {
			now := time.Now()
			var doUpdate bool
			printMessages := true
			if force {
				doUpdate = true
			} else {
				doUpdate = migrateToACL(ctx, kvStore, cfg, logging.Default(), false, now, true)
				printMessages = false
			}
			if !doUpdate {
				fmt.Println("Updated nothing.  Re-run with --force to update!")
				return
			}

			migrateToACL(ctx, kvStore, cfg, logging.Default(), true, now, printMessages)
		}

		err = kv.SetDBSchemaVersion(ctx, kvStore, kv.ACLMigrateVersion)
		if err != nil {
			fmt.Println("migration succeeded - failed to upgrade version, to fix this re-run migration with --force")
		}
		// -- migrate to ACL ends here

		// TODO(Guys): return once Migrate to ACL is removed from code
		// _ = mustValidateSchemaVersion(ctx, kvStore)
		// fmt.Printf("No migrations to apply.\n")
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
	_ = gotoCmd.Flags().Uint("version", 0, "version number")
	_ = gotoCmd.MarkFlagRequired("version")
	_ = upCmd.Flags().Bool("force", false, "force migrate, otherwise, migration will fail on warnings ")
	_ = gotoCmd.Flags().Bool("force", false, "force migrate")
}
