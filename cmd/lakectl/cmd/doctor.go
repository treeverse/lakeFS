package cmd

import (
	"context"
	"errors"
	"net/url"
	"strings"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/treeverse/lakefs/pkg/api"
)

type ErrCredential struct {
	Message string
	Details string
}

func (e *ErrCredential) Error() string {
	return (e.Message + "\n" + e.Details + "\n")
}

type ErrWrongEndpointURI struct {
	Message string
	Details string
}

func (e *ErrWrongEndpointURI) Error() string {
	return (e.Message + "\n" + e.Details + "\n")
}

type ErrUnknownConfig struct {
	Message string
	Details string
}

func (e *ErrUnknownConfig) Error() string {
	return (e.Message + "\n" + e.Details + "\n")
}

type SuccessMessage struct {
	Message string
}

var configErrorTemplate = `{{ .Message |red }}
{{  .Details | red }}
`
var successMessageTemplate = `{{ .Message | green}}
`

// doctorCmd represents the doctor command
var doctorCmd = &cobra.Command{
	Use:   "doctor",
	Short: "Run a basic diagnosis of the LakeFS configuration",
	Run: func(cmd *cobra.Command, args []string) {
		err := ListRepositoriesAndAnalyze(cmd.Context())
		if err == nil {
			Write(successMessageTemplate, &SuccessMessage{"Valid configuration"})
			return
		}

		Write(configErrorTemplate, err)

		accessKeyID := cfg.Values.Credentials.AccessKeyID
		if !IsValidAccessKeyID(accessKeyID) {
			Fmt("access_key_id value looks suspicious: %v\n", accessKeyID)
		}

		secretAccessKey := cfg.Values.Credentials.SecretAccessKey
		if !IsValidSecretAccessKey(secretAccessKey) {
			Fmt("secret_access_key value looks suspicious...\n")
		}

		serverEndpoint := cfg.Values.Server.EndpointURL
		if !strings.HasSuffix(serverEndpoint, api.BaseURL) {
			Fmt("Suspicious URI format for server.endpoint_url: %v, doesn't end with: `%v`.\n", serverEndpoint, api.BaseURL)
		}
	},
}

func ListRepositoriesAndAnalyze(ctx context.Context) error {
	configFileName := viper.GetViper().ConfigFileUsed()
	msgOnErrUnknownConfig := "It looks like you have a problem with your `" + configFileName + "` file."
	msgOnErrWrongEndpointURI := "It looks like endpoint url is wrong."
	msgOnErrCredential := "It seems like the `access_key_id` or `secret_access_key` you supplied are wrong."

	serverEndpoint := cfg.Values.Server.EndpointURL
	_, err := url.Parse(serverEndpoint)
	if err != nil {
		return &ErrWrongEndpointURI{msgOnErrWrongEndpointURI, err.Error()}
	}
	client := getClient()
	resp, err := client.ListRepositoriesWithResponse(ctx, &api.ListRepositoriesParams{})

	switch {
	case err != nil:
		urlErr := &url.Error{}
		if errors.As(err, &urlErr) {
			return &ErrWrongEndpointURI{msgOnErrWrongEndpointURI, err.Error()}
		}
		return &ErrUnknownConfig{msgOnErrUnknownConfig, err.Error()}
	case resp == nil:
		break
	case resp.JSON200 != nil:
		return nil
	case resp.JSON401 != nil:
		return &ErrCredential{msgOnErrCredential, resp.JSON401.Message}
	// In case we get the "not found" HTML page (the status is 200 and not 404 in this case)
	case resp.HTTPResponse != nil && resp.HTTPResponse.StatusCode == 200:
		return &ErrWrongEndpointURI{msgOnErrWrongEndpointURI, ""}
	case resp.JSONDefault != nil:
		return &ErrUnknownConfig{msgOnErrUnknownConfig, resp.JSONDefault.Message}
	}
	return &ErrUnknownConfig{msgOnErrUnknownConfig, "An unknown error accourd while trying to analyzing LakeCtl configuration."}
}

//nolint:gochecknoinits
func init() {
	rootCmd.AddCommand(doctorCmd)
}
