package cmd

import (
	"fmt"
	"os"
	"strings"

	homedir "github.com/mitchellh/go-homedir"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/treeverse/lakefs/pkg/config"
	"github.com/treeverse/lakefs/pkg/logging"
	"github.com/treeverse/lakefs/pkg/version"
)

var (
	cfgFile string
	cfg     *config.Config
)

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

//nolint:gochecknoinits
func init() {
	cobra.OnInitialize(initConfig)
	rootCmd.PersistentFlags().StringVarP(&cfgFile, "config", "c", "", "config file (default is $HOME/.lakefs.yaml)")
	rootCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}

// initConfig reads in config file and ENV variables if set.
func initConfig() {
	logger := logging.Default().WithField("phase", "startup")
	if cfgFile != "" {
		logger.WithField("file", cfgFile).Info("configuration file")
		// Use config file from the flag.
		viper.SetConfigFile(cfgFile)
	} else {
		logger.Info("search for configuration file .lakefs")
		// Find home directory.
		home, err := homedir.Dir()
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}

		// Search config in home directory with name ".lakefs" (without extension).
		viper.AddConfigPath(home)
		viper.SetConfigType("yaml")
		viper.SetConfigName(".lakefs")
	}

	viper.SetEnvPrefix("LAKEFS")
	viper.SetEnvKeyReplacer(strings.NewReplacer(".", "_")) // support nested config
	// read in environment variables
	viper.AutomaticEnv()

	// If a config file is found, read it in.
	err := viper.ReadInConfig()
	logger = logger.WithField("file", viper.ConfigFileUsed())

	if err == nil {
		logger.Info("loaded configuration from file")
	} else if _, ok := err.(viper.ConfigFileNotFoundError); !ok {
		logger.WithError(err).Fatal("failed to read config file")
	}

	// setup config used by the executed command
	cfg, err = config.NewConfig()
	if err != nil {
		logger.WithError(err).Fatal("load config")
	}
}
