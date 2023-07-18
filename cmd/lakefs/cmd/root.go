package cmd

import (
	"errors"
	"fmt"
	"os"
	"path"
	"strings"
	"sync"

	"github.com/mitchellh/go-homedir"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/treeverse/lakefs/pkg/block"
	"github.com/treeverse/lakefs/pkg/config"
	"github.com/treeverse/lakefs/pkg/kv/local"
	"github.com/treeverse/lakefs/pkg/logging"
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

var initOnce sync.Once

//nolint:gochecknoinits
func init() {
	rootCmd.PersistentFlags().StringVarP(&cfgFile, "config", "c", "", "config file (default is $HOME/.lakefs.yaml)")
	rootCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
	rootCmd.PersistentFlags().Bool(config.UseLocalConfiguration, false, "Use lakeFS local default configuration")
	rootCmd.PersistentFlags().Bool(config.QuickstartConfiguration, false, "Use lakeFS quickstart configuration")
}

func validateQuickstartEnv(cfg *config.Config) {
	if cfg.Database.Type != local.DriverName || cfg.Blockstore.Type != block.BlockstoreTypeLocal {
		fmt.Printf("quickstart mode can only run with local settings\n")
		os.Exit(1)
	}

	if cfg.Installation.UserName != config.DefaultQuickstartUsername ||
		cfg.Installation.AccessKeyID != config.DefaultQuickstartKeyID ||
		cfg.Installation.SecretAccessKey != config.DefaultQuickstartSecretKey {
		fmt.Printf("installation parameters must not be changed in quickstart mode\n")
		os.Exit(1)
	}
	fmt.Printf("Access Key ID    : %s\n", config.DefaultQuickstartKeyID)
	fmt.Printf("Secret Access Key: %s\n", config.DefaultQuickstartSecretKey)
}

func useConfig(cfg string) bool {
	res, err := rootCmd.PersistentFlags().GetBool(cfg)
	if err != nil {
		fmt.Printf("%s: %s\n", cfg, err)
		os.Exit(1)
	}
	if res {
		printLocalWarning(os.Stderr, fmt.Sprintf("%s parameters configuration", cfg))
	}
	return res
}

func newConfig() (*config.Config, error) {
	quickStart := useConfig(config.QuickstartConfiguration)

	switch {
	case quickStart:
		cfg, err := config.NewConfig(config.QuickstartConfiguration)
		if err != nil {
			return nil, err
		}
		validateQuickstartEnv(cfg)
		return cfg, nil
	case useConfig(config.UseLocalConfiguration):
		return config.NewConfig(config.UseLocalConfiguration)
	default:
		return config.NewConfig("")
	}
}

func loadConfig() *config.Config {
	initOnce.Do(initConfig)
	cfg, err := newConfig()
	if err != nil {
		fmt.Println("Failed to load config file", err)
		os.Exit(1)
	}
	return cfg
}

// initConfig reads in config file and ENV variables if set.
func initConfig() {
	logger := logging.Default().WithField("phase", "startup")
	if cfgFile != "" {
		logger.WithField("file", cfgFile).Info("Configuration file")
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

	// read configuration file
	err := viper.ReadInConfig()
	logger = logger.WithField("file", viper.ConfigFileUsed()) // should be called after SetConfigFile
	var errFileNotFound viper.ConfigFileNotFoundError
	if err != nil && !errors.As(err, &errFileNotFound) {
		logger.WithError(err).Fatal("Failed to find a config file")
	}
	// fallback - try to load the previous supported $HOME/.lakefs.yaml
	//   if err is set it will be file-not-found based on previous check
	if err != nil {
		fallbackCfgFile := path.Join(getHomeDir(), ".lakefs.yaml")
		if cfgFile != fallbackCfgFile {
			viper.SetConfigFile(fallbackCfgFile)
			logger = logger.WithField("file", viper.ConfigFileUsed()) // should be called after SetConfigFile
			err = viper.ReadInConfig()
			if err != nil && !os.IsNotExist(err) {
				logger.WithError(err).Fatal("Failed to read config file")
			}
		}
	}

	// setup config used by the executed command
	cfg, err := newConfig()
	if err != nil {
		logger.WithError(err).Fatal("Load config")
	} else {
		logger.Info("Config loaded")
	}

	err = cfg.Validate()
	if err != nil {
		logger.WithError(err).Fatal("Invalid config")
	}

	logger.WithFields(config.MapLoggingFields(cfg)).Info("Config")
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
