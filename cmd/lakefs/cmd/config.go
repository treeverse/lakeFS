package cmd

import (
	"errors"
	"fmt"
	"os"

	"github.com/manifoldco/promptui"
	"github.com/mitchellh/go-homedir"

	"io/ioutil"

	"encoding/json"

	"github.com/spf13/cobra"
	"gopkg.in/yaml.v2"
)

// configuration model

type Database struct {
	ConnectionString string `yaml:"connection_string"`
}

type Gateways struct {
	S3 struct {
		DomainName string `yaml:"domain_name"`
		Region     string `yaml:"region"`
	} `yaml:"s3"`
}

type Configuration struct {
	DB         Database               `yaml:"database"`
	Blockstore map[string]interface{} `yaml:"blockstore"`
	GW         Gateways               `yaml:"gateways"`
}

// configCmd represents the config command
var configCmd = &cobra.Command{
	Use:   "config",
	Short: "User Interactive Config File generator",
	Long:  `long description here...`,
	Run: func(cmd *cobra.Command, args []string) {

		var config Configuration

		//loop for menu based interaction

		var isEdit string
		isEdit = "Yes"

		for {

			//Database
			//connection string
			prompt := promptui.Prompt{
				Label:    "Database->connection string",
				Validate: isEmptyInput,
			}

			connectionString, _ := prompt.Run()
			config.DB.ConnectionString = connectionString

			//Blockstore
			config.Blockstore = make(map[string]interface{})
			prompt = promptui.Prompt{
				Label:    "Blockstore->Type",
				Validate: isEmptyInput,
			}

			blockstoreType, _ := prompt.Run()
			config.Blockstore["type"] = blockstoreType

			if blockstoreType == "s3" {
				S3 := make(map[string]string)

				//s3 region
				prompt = promptui.Prompt{
					Label:    "Blockstore->s3->region",
					Validate: isEmptyInput,
				}

				blockstoreRegion, _ := prompt.Run()
				S3["region"] = blockstoreRegion

				//s3 credentials file
				prompt := promptui.Prompt{
					Label:    "Blockstore->s3->Credentials File",
					Validate: isEmptyInput,
				}
				credFile, _ := prompt.Run()
				S3["credentials_file"] = credFile

				//s3 profile
				prompt = promptui.Prompt{
					Label:    "Blockstore->s3->profile",
					Validate: isEmptyInput,
				}
				prof, _ := prompt.Run()
				S3["profile"] = prof

				config.Blockstore["s3"] = S3

			} else {

				Loc := make(map[string]string)

				//path
				prompt := promptui.Prompt{
					Label:    "Blockstore->Local->Path",
					Validate: isEmptyInput,
				}
				localPath, _ := prompt.Run()
				Loc["path"] = localPath

				config.Blockstore["local"] = Loc

			}

			//Gateways

			//domain_name
			prompt = promptui.Prompt{
				Label:    "Gateways->Domain Name",
				Validate: isEmptyInput,
			}

			domainName, _ := prompt.Run()
			config.GW.S3.DomainName = domainName

			//Region
			prompt = promptui.Prompt{
				Label:    "Gateways->Region",
				Validate: isEmptyInput,
			}

			s3Region, _ := prompt.Run()
			config.GW.S3.Region = s3Region

			//printing the config

			fmt.Println("------------config---------------")
			PrintConfig(config)

			selectPrompt := promptui.Select{
				Label: "Want to edit?",
				Items: []string{"Yes", "No"},
			}

			_, isEdit, _ = selectPrompt.Run()

			//if not wants to edit then exit loop
			if isEdit == "No" {
				break
			}

		}
		//menu loop ends

		//config file path
		homeDir, _ := homedir.Dir()
		prompt := promptui.Prompt{
			Label: "Save config file to (default-" + homeDir + "/.lakefs.yaml)",
		}

		fileDir, _ := prompt.Run()

		if len(fileDir) == 0 {
			fileDir = homeDir + "/.lakefs.yaml"
		}

		err := saveConfig(config, fileDir)

		if err != nil {
			fmt.Println("failed to save the file:", err)
			os.Exit(1)
		} else {
			fmt.Println("Config File Saved Successfully at", fileDir)
		}

	},
}

func init() {
	rootCmd.AddCommand(configCmd)
}

//utility functions

//function to save config file to provided destination
func saveConfig(c Configuration, filename string) error {

	bytes, err := yaml.Marshal(c)

	if err != nil {
		fmt.Println("Error occured while saving file:", err)
		os.Exit(1)
	}

	return ioutil.WriteFile(filename, bytes, 0644)

}

//PrintConfig function to pretty print the configuration structure
func PrintConfig(c Configuration) {

	configData, _ := json.MarshalIndent(c, "", " ")
	fmt.Printf("configuration:\n%s", string(configData))

}

// input validation functions

// to check if the input is empty
func isEmptyInput(input string) error {
	if len(input) < 1 {
		return errors.New("this field must have an input")
	}
	return nil
}
