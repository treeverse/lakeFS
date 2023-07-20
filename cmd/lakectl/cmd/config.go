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
	"github.com/treeverse/lakefs/cmd/lakectl/cmd/utils"
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
				utils.DieErr(err)
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
			{Key: config.ConfigSecretAccessKey, Prompt: &promptui.Prompt{Label: "Secret access key", Mask: '*'}},
			{Key: config.ConfigServerEndpointURLKey, Prompt: &promptui.Prompt{Label: "Server endpoint URL", Validate: func(rawurl string) error {
				_, err := url.ParseRequestURI(rawurl)
				return err
			}}},
		}
		for _, question := range questions {
			question.Prompt.Default = viper.GetString(question.Key)
			val, err := question.Prompt.Run()
			if err != nil {
				utils.DieErr(err)
			}
			viper.Set(question.Key, val)
		}

		err := viper.SafeWriteConfig()
		if err != nil {
			err = viper.WriteConfig()
		}
		if err != nil {
			utils.DieErr(err)
		}
	},
}

//nolint:gochecknoinits
func init() {
	rootCmd.AddCommand(configCmd)
}
