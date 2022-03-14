package cmd

import (
	"fmt"
	"net/url"
	"path/filepath"

	"github.com/manifoldco/promptui"
	"github.com/mitchellh/go-homedir"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/treeverse/lakefs/cmd/lakectl/cmd/config"
)

// configCmd represents the config command
var configCmd = &cobra.Command{
	Use:   "config",
	Short: "Create/update local lakeFS configuration",
	Run: func(cmd *cobra.Command, args []string) {
		if viper.ConfigFileUsed() == "" {
			// Find home directory.
			home, err := homedir.Dir()
			if err != nil {
				DieErr(err)
			}
			// Setup default config file
			viper.SetConfigFile(filepath.Join(home, ".lakectl.yaml"))
		}
		fmt.Printf("Config file %s will be used\n", viper.ConfigFileUsed())

		// get user input
		questions := []struct {
			Key    string
			Prompt *promptui.Prompt
		}{
			{Key: config.ConfigAccessKeyIDKey, Prompt: &promptui.Prompt{Label: "Access key ID"}},
			{Key: config.ConfigSecretAccessKey, Prompt: &promptui.Prompt{Label: "Secret access key"}},
			{Key: config.ConfigServerEndpointURLKey, Prompt: &promptui.Prompt{Label: "Server endpoint URL", Validate: func(rawurl string) error {
				_, err := url.ParseRequestURI(rawurl)
				return err
			}}},
		}
		for _, question := range questions {
			question.Prompt.Default = viper.GetString(question.Key)
			val, err := question.Prompt.Run()
			if err != nil {
				DieErr(err)
			}
			viper.Set(question.Key, val)
		}

		err := viper.SafeWriteConfig()
		if err != nil {
			err = viper.WriteConfig()
		}
		if err != nil {
			DieErr(err)
		}
	},
}

// configSetCmd represents the config set command
var configSetCmd = &cobra.Command{
	Use:   "set",
	Short: "Set local lakeFS configuration variables",
}

// configSetAccessKeyIdCmd represents the config set lakefs-access-key-id  command
var configSetAccessKeyIdCmd = &cobra.Command{
	Use:   "lakefs-access-key-id <access key id>",
	Short: "Create/update local lakeFS Access Key Id",
	Args:    cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		
		viper.Set(config.ConfigAccessKeyIDKey, args[0])

		err := viper.SafeWriteConfig()
		if err != nil {
			err = viper.WriteConfig()
		}
		if err != nil {
			DieErr(err)
		}
	},
}

// configSetSecretAccessKeyCmd represents the config set lakefs-secret-access-key command
var configSetSecretAccessKeyCmd = &cobra.Command{
	Use:   "lakefs-secret-access-key <secret access key>",
	Short: "Create/update local lakeFS Secret Access Key",
	Args:    cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		
		viper.Set(config.ConfigSecretAccessKey, args[0])

		err := viper.SafeWriteConfig()
		if err != nil {
			err = viper.WriteConfig()
		}
		if err != nil {
			DieErr(err)
		}
	},
}

// configSetEndpointUrlCmd represents the config set lakefs-endpoint command
var configSetEndpointUrlCmd = &cobra.Command{
	Use:   "lakefs-endpoint-url <endpoint url>",
	Short: "Create/update local lakeFS Endpoint",
	Args:    cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		
		_, err := url.ParseRequestURI(args[0])
		if err!=nil{
			DieErr(err)
		}
		viper.Set(config.ConfigServerEndpointURLKey, args[0])

		err = viper.SafeWriteConfig()
		if err != nil {
			err = viper.WriteConfig()
		}
		if err != nil {
			DieErr(err)
		}
	},
}

//nolint:gochecknoinits
func init() {
	rootCmd.AddCommand(configCmd)
	
	configCmd.AddCommand(configSetCmd)
	
	configSetCmd.AddCommand(configSetAccessKeyIdCmd)
	configSetCmd.AddCommand(configSetSecretAccessKeyCmd)
	configSetCmd.AddCommand(configSetEndpointUrlCmd)
}
