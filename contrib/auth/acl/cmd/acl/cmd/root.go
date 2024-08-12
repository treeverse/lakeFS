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
	"github.com/treeverse/lakefs/contrib/auth/acl"
	"github.com/treeverse/lakefs/pkg/config"
	"github.com/treeverse/lakefs/pkg/logging"
)

var cfgFile string

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:     "acl",
	Short:   "acl is an acl auth server example for LakeFS",
	Version: "dev",
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
	rootCmd.PersistentFlags().StringVarP(&cfgFile, "config", "c", "", "config file (default is $HOME/.aclserver.yaml)")
}

func loadConfig() *acl.Config {
	initOnce.Do(initConfig)
	cfg, err := acl.NewConfig()
	if err != nil {
		fmt.Println("Failed to load config file", err)
		os.Exit(1)
	}
	return cfg
}

// initConfig reads in config file and ENV variables if set.
func initConfig() {
	logger := logging.ContextUnavailable().WithField("phase", "startup")
	if cfgFile != "" {
		logger.WithField("file", cfgFile).Info("Configuration file")
		// Use config file from the flag.
		viper.SetConfigFile(cfgFile)
	} else {
		viper.SetConfigType("yaml")
		viper.SetConfigName("config")
		viper.AddConfigPath(".")
		viper.AddConfigPath(path.Join(getHomeDir(), ".aclserver"))
		viper.AddConfigPath("/etc/aclserver")
	}

	viper.SetEnvPrefix("aclserver")
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
	// fallback - try to load the previous supported $HOME/.fluffy.yaml
	//   if err is set it will be file-not-found based on previous check
	if err != nil {
		fallbackCfgFile := path.Join(getHomeDir(), ".aclserver.yaml")
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
	cfg, err := acl.NewConfig()
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
