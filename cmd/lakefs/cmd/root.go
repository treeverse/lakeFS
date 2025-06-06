package cmd

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path"
	"slices"
	"strings"
	"sync"

	"github.com/mitchellh/go-homedir"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	configfactory "github.com/treeverse/lakefs/modules/config/factory"
	"github.com/treeverse/lakefs/pkg/auth"
	"github.com/treeverse/lakefs/pkg/block"
	"github.com/treeverse/lakefs/pkg/cloud"
	"github.com/treeverse/lakefs/pkg/config"
	"github.com/treeverse/lakefs/pkg/kv/local"
	"github.com/treeverse/lakefs/pkg/kv/mem"
	"github.com/treeverse/lakefs/pkg/logging"
	"github.com/treeverse/lakefs/pkg/stats"
	"github.com/treeverse/lakefs/pkg/version"
)

var cfgFile string

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:     "lakefs",
	Short:   "lakeFS is a data lake management platform",
	Version: version.Version,
}

// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func GetRoot() *cobra.Command {
	return rootCmd
}

var initOnce sync.Once

//nolint:gochecknoinits
func init() {
	rootCmd.PersistentFlags().StringVarP(&cfgFile, "config", "c", "", "config file (default is $HOME/.lakefs.yaml)")
	rootCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
	rootCmd.PersistentFlags().Bool(config.UseLocalConfiguration, false, "Use lakeFS local default configuration")
	rootCmd.PersistentFlags().Bool(config.QuickstartConfiguration, false, "Use lakeFS quickstart configuration")
}

// TODO (niro): All this validation logic should be in the config package

func validateQuickstartEnv(cfg *config.BaseConfig) {
	if (cfg.Database.Type != local.DriverName && cfg.Database.Type != mem.DriverName) || cfg.Blockstore.Type != block.BlockstoreTypeLocal {
		_, _ = fmt.Fprint(os.Stderr, "\nFATAL: quickstart mode can only run with local settings\n")
		os.Exit(1)
	}

	if cfg.Installation.UserName != config.DefaultQuickstartUsername ||
		cfg.Installation.AccessKeyID != config.DefaultQuickstartKeyID ||
		cfg.Installation.SecretAccessKey != config.DefaultQuickstartSecretKey {
		_, _ = fmt.Fprint(os.Stderr, "\nFATAL: installation parameters must not be changed in quickstart mode\n")
		os.Exit(1)
	}
}

func useConfig(flagName string) bool {
	res, err := rootCmd.PersistentFlags().GetBool(flagName)
	if err != nil {
		fmt.Printf("%s: %s\n", flagName, err)
		os.Exit(1)
	}
	if res {
		printLocalWarning(os.Stderr, fmt.Sprintf("%s parameters configuration", flagName))
	}
	return res
}

func newConfig() (config.Config, error) {
	name := ""
	configurations := []string{config.QuickstartConfiguration, config.UseLocalConfiguration}
	if idx := slices.IndexFunc(configurations, useConfig); idx != -1 {
		name = configurations[idx]
	}

	cfg, err := configfactory.BuildConfig(name)
	if err != nil {
		return nil, err
	}

	if name == config.QuickstartConfiguration {
		validateQuickstartEnv(cfg.GetBaseConfig())
	}
	return cfg, nil
}

func LoadConfig() config.Config {
	log := logging.ContextUnavailable().WithField("phase", "startup")
	initOnce.Do(func() {
		initConfig(log)
	})
	// setup config used by the executed command
	cfg, err := newConfig()
	if err != nil {
		log.WithError(err).Fatal("Load config")
	} else {
		log.Info("Config loaded")
	}

	log.WithFields(config.MapLoggingFields(cfg)).Info("Config")
	if err != nil {
		fmt.Println("Failed to load config file", err)
		os.Exit(1)
	}
	return cfg
}

// initConfig reads in config file and ENV variables if set.
func initConfig(log logging.Logger) {
	if cfgFile != "" {
		log.WithField("file", cfgFile).Info("Configuration file")
		// Use config file from the flag.
		viper.SetConfigFile(cfgFile)
	} else {
		viper.SetConfigType("yaml")
		viper.SetConfigName("config")
		viper.AddConfigPath(".")
		viper.AddConfigPath(path.Join(getHomeDir(), ".lakefs"))
		viper.AddConfigPath("/etc/lakefs")
	}

	viper.SetEnvPrefix("LAKEFS")
	viper.SetEnvKeyReplacer(strings.NewReplacer(".", "_")) // support nested config
	// read in environment variables
	viper.AutomaticEnv()

	// read the configuration file
	err := viper.ReadInConfig()
	log = log.WithField("file", viper.ConfigFileUsed()) // should be called after SetConfigFile
	var errFileNotFound viper.ConfigFileNotFoundError
	if err != nil && !errors.As(err, &errFileNotFound) {
		log.WithError(err).Fatal("Failed to find a config file")
	}
	// fallback - try to load the previous supported $HOME/.lakefs.yaml
	//   if err is set it will be file-not-found based on the previous check
	if err != nil {
		fallbackCfgFile := path.Join(getHomeDir(), ".lakefs.yaml")
		if cfgFile != fallbackCfgFile {
			viper.SetConfigFile(fallbackCfgFile)
			log = log.WithField("file", viper.ConfigFileUsed()) // should be called after SetConfigFile
			err = viper.ReadInConfig()
			if err != nil && !os.IsNotExist(err) {
				log.WithError(err).Fatal("Failed to read config file")
			}
		}
	}
}

// getHomeDir find and return the home directory
func getHomeDir() string {
	home, err := homedir.Dir()
	if err != nil {
		fmt.Println("Get home directory -", err)
		os.Exit(1)
	}
	return home
}

// initStatsMetadata initializes and returns stats metadata with all required providers
func initStatsMetadata(ctx context.Context, logger logging.Logger, authMetadataManager auth.MetadataManager, storageConfig config.StorageConfig) *stats.Metadata {
	metadataProviders := []stats.MetadataProvider{
		authMetadataManager,
		cloud.NewMetadataProvider(),
		block.NewMetadataProvider(storageConfig),
	}
	return stats.NewMetadata(ctx, logger, metadataProviders)
}
