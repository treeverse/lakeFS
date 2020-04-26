package cmd

import (
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/treeverse/lakefs/api"
	"github.com/treeverse/lakefs/config"

	"github.com/spf13/cobra"

	homedir "github.com/mitchellh/go-homedir"
	"github.com/spf13/viper"
)

const (
	DefaultConfigFileDirectory = "~/.lakefs"
	DefaultConfigFileBareName  = "config"
	DefaultConfigFilePath      = "~/.lakefs/config.yaml"

	ConfigAccessKeyId       = "credentials.access_key_id"
	ConfigSecretAccessKey   = "credentials.secret_access_key"
	ConfigServerEndpointUrl = "server.endpoint_url"
)

var cfgFile string

// rootCmd represents the base command when called without any sub-commands
var rootCmd = &cobra.Command{
	Use:   "lakectl",
	Short: "A cli tool to explore manage and work with lakeFS",
	Long: `lakeFS is data lake management solution, allowing Git-like semantics over common object stores

lakectl is a CLI tool allowing exploration and manipulation of a lakeFS environment`,
	PersistentPreRun: func(cmd *cobra.Command, args []string) {
		if noColorRequested {
			DisableColors()
		}
		if strings.EqualFold(cmd.Use, "config") && len(args) > 0 && strings.EqualFold(args[0], "init") {
			initConfig(false)
		} else {
			initConfig(true)
		}
	},
	Version: config.Version,
}

func getClient() api.Client {
	client, err := api.NewClient(
		viper.GetString(ConfigServerEndpointUrl),
		viper.GetString(ConfigAccessKeyId),
		viper.GetString(ConfigSecretAccessKey),
	)
	if err != nil {
		Die(fmt.Sprintf("could not initialize API client: %s", err), 1)
	}
	return client
}

// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute() {
	if noColorRequested {
		DisableColors()
	}
	err := rootCmd.Execute()
	if err != nil {
		log.Fatalln(err)
	}
}

func init() {
	// Here you will define your flags and configuration settings.
	// Cobra supports persistent flags, which, if defined here,
	// will be global for your application.
	rootCmd.PersistentFlags().StringVar(&cfgFile, "config", "", fmt.Sprintf("config file (default is %s)", DefaultConfigFilePath))
	rootCmd.PersistentFlags().BoolVar(&noColorRequested, "no-color", false, "use fancy output colors (ignored when not attached to an interactive terminal)")
}

// initConfig reads in config file and ENV variables if set.
func initConfig(readConf bool) {
	if cfgFile != "" {
		// Use config file from the flag.
		viper.SetConfigFile(cfgFile)
	} else {
		// Find home directory.
		cfgHome, err := homedir.Expand(DefaultConfigFileDirectory)
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}

		// Search config in home directory with name ".lakefs" (without extension).
		viper.AddConfigPath(cfgHome)
		viper.SetConfigName(DefaultConfigFileBareName)
	}

	viper.AutomaticEnv() // read in environment variables that match

	// if we're running "config init", don't read the config first
	if !readConf {
		return
	}

	// If a config file is found, read it in.
	if err := viper.ReadInConfig(); err != nil {
		if _, ok := err.(viper.ConfigFileNotFoundError); ok {
			// Config file not found; ignore error if desired
			Die(fmt.Sprintf("config file not found, please run \"lakectl config init\" to create one\n%s\n", err.Error()), 1)
		} else {
			// Config file was found but another error was produced
			Die(fmt.Sprintf("error reading configuration file: %v", err), 1)
		}
	}
}
