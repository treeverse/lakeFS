package cmd

import (
	"errors"
	"fmt"
	"net/url"
	"path/filepath"

	"github.com/manifoldco/promptui"
	"github.com/mitchellh/go-homedir"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var ErrInvalidEndpoint = errors.New("invalid endpoint")

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
			{Key: "credentials.access_key_id", Prompt: &promptui.Prompt{Label: "Access key ID"}},
			{Key: "credentials.secret_access_key", Prompt: &promptui.Prompt{Label: "Secret access key", Mask: '*'}},
			{Key: "server.endpoint_url", Prompt: &promptui.Prompt{Label: "Server endpoint", Validate: func(rawURL string) error {
				u, err := url.ParseRequestURI(rawURL)
				if err != nil {
					return err
				}
				if u.Path != "" {
					return fmt.Errorf("%w: do not specify endpoint path", ErrInvalidEndpoint)
				}
				return nil
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

//nolint:gochecknoinits
func init() {
	rootCmd.AddCommand(configCmd)
}
