package cmd

import (
	"fmt"
	"strings"

	"github.com/spf13/cobra"
	"github.com/treeverse/lakefs/pkg/api"
)

// doctorCmd represents the doctor command
var doctorCmd = &cobra.Command{
	Use:   "doctor",
	Short: "Run a basic diagnosis of the LakeFS configuration",
	Run: func(cmd *cobra.Command, args []string) {
		client := getClient()
		rsp, err := client.ListRepositoriesWithResponse(cmd.Context(), &api.ListRepositoriesParams{})

		if err != nil {
			if strings.Contains(err.Error(), "unsupported protocol scheme") {
				fmt.Println("It looks like you have a problem with your '.lakectl.yaml' file. \nProbably your endpoint url is wrong.")
			} else {
				fmt.Println(err.Error())
			}
		} else {
			if rsp != nil {
				if rsp.JSON200 != nil {
					fmt.Println("LakeFS doctor could not find any configuration issues")
					return
				}
				if rsp.JSON401 != nil {
					fmt.Println(rsp.JSON401.Message)
					fmt.Println("It looks like you have a problem with your '.lakectl.yaml' file. \nIt is possible that the 'access_key_id' or 'secret_access_key' you supplied are wrong.")
				} else {
					// In case we get the "not found" HTML page (the status is 200 and not 404 in this case)
					if rsp.HTTPResponse != nil && rsp.HTTPResponse.StatusCode == 200 {
						fmt.Println("It looks like you have a problem with your '.lakectl.yaml' file. \nProbably your endpoint url is wrong.")
					} else {
						if rsp.JSONDefault != nil {
							fmt.Println("It looks like you have a problem with your '.lakectl.yaml' file.")
							fmt.Println(rsp.JSONDefault.Message)
						} else {
							fmt.Println("It looks like you have a problem with your '.lakectl.yaml' file.")
						}
					}
				}
			}

		}

		accessKeyID := cfg.Values.Credentials.AccessKeyID
		if !IsValidAccessKeyID(accessKeyID) {
			fmt.Println("access_key_id value looks suspicious...")
		}

		secretAccessKey := cfg.Values.Credentials.SecretAccessKey
		if !IsValidSecretAccessKey(secretAccessKey) {
			fmt.Println("secret_access_key value looks suspicious...")
		}

		serverEndpoint := cfg.Values.Server.EndpointURL
		if !IsValidEndpointURI(serverEndpoint) {
			fmt.Println("Suspicious URI format for server.endpoint_url:", serverEndpoint)
		}
	},
}

//nolint:gochecknoinits
func init() {
	rootCmd.AddCommand(doctorCmd)
}
