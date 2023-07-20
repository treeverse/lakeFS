package cmd

import (
	"context"
	"errors"
	"net/url"
	"strings"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/treeverse/lakefs/cmd/lakectl/cmd/utils"
	"github.com/treeverse/lakefs/pkg/api"
)

type Detailed interface {
	GetDetails() string
}

type CredentialsError struct {
	Message string
	Details string
}

func (e *CredentialsError) Error() string { return e.Message }

func (e *CredentialsError) GetDetails() string {
	return e.Message + "\n" + e.Details
}

type WrongEndpointURIError struct {
	Message string
	Details string
}

func (e *WrongEndpointURIError) Error() string { return e.Message }

func (e *WrongEndpointURIError) GetDetails() string {
	return e.Message + "\n" + e.Details
}

type UnknownConfigError struct {
	Message string
	Details string
}

func (e *UnknownConfigError) Error() string { return e.Message }

func (e *UnknownConfigError) GetDetails() string {
	return e.Message + "\n" + e.Details
}

type UserMessage struct {
	Message string
}

var detailedErrorTemplate = `{{ .Message |red }}
{{  .Details  }}
`

var errorTemplate = `{{ .Message |red }}
`

var successMessageTemplate = `{{ .Message | green}}
`

var analyzingMessageTemplate = `{{ .Message }}
`

// doctorCmd represents the doctor command
var doctorCmd = &cobra.Command{
	Use:   "doctor",
	Short: "Run a basic diagnosis of the LakeFS configuration",
	Run: func(cmd *cobra.Command, args []string) {
		err := ListRepositoriesAndAnalyze(cmd.Context())
		if err == nil {
			utils.Write(successMessageTemplate, &UserMessage{Message: "Valid configuration"})
			return
		}

		utils.WriteIfVerbose(analyzingMessageTemplate, &UserMessage{Message: "Got error while trying to run a sanity command.\nTrying to analyze error."})
		if detailedErr, ok := err.(Detailed); ok {
			utils.Write(detailedErrorTemplate, detailedErr)
		} else {
			utils.Write(errorTemplate, err)
		}

		utils.WriteIfVerbose(analyzingMessageTemplate, &UserMessage{Message: "Trying to validate access key format."})
		accessKeyID := cfg.Values.Credentials.AccessKeyID
		if !utils.IsValidAccessKeyID(accessKeyID) {
			utils.Write(analyzingMessageTemplate, &UserMessage{Message: "access_key_id value looks suspicious: " + accessKeyID})
		} else {
			utils.WriteIfVerbose(analyzingMessageTemplate, &UserMessage{Message: "Couldn't find a problem with access key format."})
		}

		utils.WriteIfVerbose(analyzingMessageTemplate, &UserMessage{Message: "Trying to validate secret access key format."})
		secretAccessKey := cfg.Values.Credentials.SecretAccessKey
		if !utils.IsValidSecretAccessKey(secretAccessKey) {
			utils.Write(analyzingMessageTemplate, &UserMessage{Message: "secret_access_key value looks suspicious..."})
		} else {
			utils.WriteIfVerbose(analyzingMessageTemplate, &UserMessage{Message: "Couldn't find a problem with secret access key format."})
		}

		utils.WriteIfVerbose(analyzingMessageTemplate, &UserMessage{Message: "Trying to validate endpoint URL format."})
		serverEndpoint := cfg.Values.Server.EndpointURL
		if !strings.HasSuffix(serverEndpoint, api.BaseURL) {
			utils.Write(analyzingMessageTemplate, &UserMessage{Message: "Suspicious URI format for server.endpoint_url: " + serverEndpoint})
		} else {
			utils.WriteIfVerbose(analyzingMessageTemplate, &UserMessage{Message: "Couldn't find a problem with endpoint URL format."})
		}
	},
}

func ListRepositoriesAndAnalyze(ctx context.Context) error {
	configFileName := viper.GetViper().ConfigFileUsed()
	msgOnErrUnknownConfig := "It looks like you have a problem with your `" + configFileName + "` file."
	msgOnErrWrongEndpointURI := "It looks like endpoint url is wrong."
	msgOnErrCredential := "It seems like the `access_key_id` or `secret_access_key` you supplied are wrong." //nolint: gosec

	utils.WriteIfVerbose(analyzingMessageTemplate, &UserMessage{Message: "Trying to get endpoint URL and parse it as a URL format."})
	// getClient might die on url.Parse error, so check it first.
	serverEndpoint := cfg.Values.Server.EndpointURL
	_, err := url.Parse(serverEndpoint)
	if err != nil {
		return &WrongEndpointURIError{Message: msgOnErrWrongEndpointURI, Details: err.Error()}
	}
	client := getClient()
	utils.WriteIfVerbose(analyzingMessageTemplate, &UserMessage{Message: "Trying to run a sanity command using current configuration."})
	resp, err := client.ListRepositoriesWithResponse(ctx, &api.ListRepositoriesParams{})

	switch {
	case err != nil:
		var urlErr *url.Error
		if errors.As(err, &urlErr) {
			return &WrongEndpointURIError{Message: msgOnErrWrongEndpointURI, Details: err.Error()}
		}
		return &UnknownConfigError{Message: msgOnErrUnknownConfig, Details: err.Error()}
	case resp == nil:
		break
	case resp.JSON200 != nil:
		return nil
	case resp.JSON401 != nil:
		return &CredentialsError{Message: msgOnErrCredential, Details: resp.JSON401.Message}
	case resp.JSONDefault != nil:
		return &UnknownConfigError{Message: msgOnErrUnknownConfig, Details: resp.JSONDefault.Message}
	case resp.HTTPResponse != nil && resp.JSON200 == nil:
		// In case we get the HTML page
		return &WrongEndpointURIError{Message: msgOnErrWrongEndpointURI}
	}
	return &UnknownConfigError{
		Message: msgOnErrUnknownConfig,
		Details: "An unknown error occurred while trying to analyze LakeCtl configuration.",
	}
}

//nolint:gochecknoinits
func init() {
	rootCmd.AddCommand(doctorCmd)
}
