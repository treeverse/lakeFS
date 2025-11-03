package cmd

import (
	"fmt"
	"net/http"
	"net/url"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/spf13/cobra"
	"github.com/treeverse/lakefs/pkg/api/apigen"
)

const (
	// TODO(ariels): Underline the link?
	webLoginTemplate = `Opening {{.RedirectURL|bold|blue}} where you should log in.

If that URL does not open automatically, please open it manually and log in.
`
	loggedInTemplate = `Logged in as {{.Username}}.`
)

type webLoginParams struct {
	RedirectURL string
}

var loginCmd = &cobra.Command{
	Use:     "login",
	Short:   "Use a web browser to log into lakeFS",
	Long:    "Connect to lakeFS using a web browser.",
	Example: "lakectl login",
	Run: func(cmd *cobra.Command, _ []string) {
		serverURL, err := url.Parse(cfg.Server.EndpointURL.String())
		if err != nil {
			DieErr(fmt.Errorf("Get server URL %s: %w", cfg.Server.EndpointURL, err))
		}

		client := getClient()
		tokenRedirect, err := client.GetTokenRedirectWithResponse(cmd.Context())
		// TODO(ariels): Change back to http.StatusSeeOther after fixing lakeFS server!
		DieOnErrorOrUnexpectedStatusCode(tokenRedirect, err, http.StatusOK)
		header := tokenRedirect.HTTPResponse.Header
		relativeLocation, err := url.Parse(header.Get("location"))
		if err != nil {
			DieErr(fmt.Errorf("Parse relative redirect URL %s: %w", header.Get("location"), err))
		}
		mailbox := header.Get("x-lakefs-mailbox")

		redirectURL := serverURL.ResolveReference(relativeLocation)

		Write(webLoginTemplate, webLoginParams{RedirectURL: redirectURL.String()})
		// TODO(ariels): Open redirectURL (xdg-open on Linux, something on MacOS, maybe
		//     a library does it?)

		loginToken, err := backoff.RetryWithData(
			func() (*apigen.AuthenticationToken, error) {
				resp, err := client.GetTokenFromMailboxWithResponse(cmd.Context(), mailbox)
				if err != nil {
					return nil, err
				}
				return resp.JSON200, nil
			},
			// Initial backoff is rapid, in case user is already logged in.  Then
			// slow down considerably so user can log in!
			backoff.NewExponentialBackOff(backoff.WithInitialInterval(80*time.Millisecond),
				backoff.WithMultiplier(1.5),
				backoff.WithMaxInterval(time.Second),
				backoff.WithMaxElapsedTime(20*time.Second),
			),
		)
		if err != nil {
			DieErr(fmt.Errorf("get login token: %w", err))
		}

		// BUG(ariels): Remove!
		fmt.Printf("TOKEN: %s\nExpires: %d\n", loginToken.Token, *loginToken.TokenExpiration)

		cache := getTokenCacheOnce()
		err = cache.SaveToken(loginToken)
		if err != nil {
			DieErr(fmt.Errorf("save login token: %w", err))
		}
	},
}

//nolint:gochecknoinits
func init() {
	rootCmd.AddCommand(loginCmd)
}
