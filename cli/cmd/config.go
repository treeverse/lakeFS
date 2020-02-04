/*
Copyright © 2020 NAME HERE <EMAIL ADDRESS>

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package cmd

import (
	"os"

	"github.com/mitchellh/go-homedir"

	"github.com/spf13/viper"

	"github.com/spf13/cobra"
)

// configCmd represents the config command
var configCmd = &cobra.Command{
	Use:   "config",
	Short: "manage local lakefs configuration",
	Long: `A longer description that spans multiple lines and likely contains examples
and usage of using your command. For example:

Cobra is a CLI library for Go that empowers applications.
This application is a tool to generate the needed files
to quickly create a Cobra application.`,
	RunE: func(cmd *cobra.Command, args []string) error {
		// create directory if it doesn't exist
		configDir, _ := homedir.Expand(DefaultConfigFileDirectory)
		configFile, _ := homedir.Expand(DefaultConfigFilePath)
		err := os.MkdirAll(configDir, 0755)
		if err != nil {
			return err
		}

		f, err := os.OpenFile(configFile, os.O_WRONLY|os.O_CREATE, 0644)
		if err != nil {
			return err
		}
		err = f.Close()
		if err != nil {
			return err
		}

		// write config
		accessKeyId, err := prompt("access key id")
		if err != nil {
			return err
		}
		viper.SetDefault(ConfigAccessKeyId, accessKeyId)

		secretAccessKey, err := prompt("secret access key")
		if err != nil {
			return err
		}
		viper.SetDefault(ConfigSecretAccessKey, secretAccessKey)

		endpointUrl, err := promptUrl("endpoint URL")
		if err != nil {
			return err
		}
		viper.SetDefault(ConfigServerEndpointUrl, endpointUrl)

		err = viper.WriteConfigAs(configFile)
		if err != nil {
			return err
		}
		return nil

	},
}

func init() {
	rootCmd.AddCommand(configCmd)

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// configCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// configCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}
