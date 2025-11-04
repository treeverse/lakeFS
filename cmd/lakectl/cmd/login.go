package cmd

import (
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/skratchdot/open-golang/open"
	"github.com/spf13/cobra"
	"github.com/treeverse/lakefs/pkg/api/apigen"
)

const (
	// TODO(ariels): Underline the link?
	webLoginTemplate = `Opening {{.RedirectURL | blue | underline}} where you should log in.
If it does not open automatically, please try to open it manually and log in.
`
	loggedInTemplate = `
[{{.Time | green}}] {{"Logged in." | green | bold}}
`
)

type webLoginParams struct {
	RedirectURL string
}

var (
	errTryAgain                     = errors.New("HTTP request failed; retry")
	errFailedToGetToken             = errors.New("failed to get token")
	errFailedToGetTokenDontTryAgain = backoff.Permanent(errFailedToGetToken)
)

var loginCmd = &cobra.Command{
	Use:     "login",
	Short:   "Use a web browser to log into lakeFS",
	Long:    "Connect to lakeFS using a web browser.",
	Example: "lakectl login",
	Run: func(cmd *cobra.Command, _ []string) {
		serverURL, err := url.Parse(cfg.Server.EndpointURL.String())
		if err != nil {
			DieErr(fmt.Errorf("get server URL %s: %w", cfg.Server.EndpointURL, err))
		}

		client := getClient()
		tokenRedirect, err := client.GetTokenRedirectWithResponse(cmd.Context())
		// TODO(ariels): Change back to http.StatusSeeOther after fixing lakeFS server!
		DieOnErrorOrUnexpectedStatusCode(tokenRedirect, err, http.StatusOK)
		header := tokenRedirect.HTTPResponse.Header
		relativeLocation, err := url.Parse(header.Get("location"))
		if err != nil {
			DieErr(fmt.Errorf("parse relative redirect URL %s: %w", header.Get("location"), err))
		}
		mailbox := header.Get("x-lakefs-mailbox")

		redirectURL := serverURL.ResolveReference(relativeLocation)

		Write(webLoginTemplate, webLoginParams{RedirectURL: redirectURL.String()})
		err = open.Run(redirectURL.String())
		if err != nil {
			Warning(fmt.Sprintf("Failed to open URL: %s", err.Error()))
			// Keep going, user can manually use the URL.
		}

		loginToken, err := backoff.RetryWithData(
			func() (*apigen.AuthenticationToken, error) {
				resp, err := client.GetTokenFromMailboxWithResponse(cmd.Context(), mailbox)
				if err != nil {
					return nil, err
				}
				if resp.JSON404 != nil {
					return nil, errTryAgain
				}
				if resp.JSON200 == nil {
					return nil, errFailedToGetTokenDontTryAgain
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

		if loginToken == nil {
			Die("nil login token", 1)
		}

		cache := getTokenCacheOnce()
		err = cache.SaveToken(loginToken)
		if err != nil {
			DieErr(fmt.Errorf("save login token: %w", err))
		}

		Write(loggedInTemplate, struct{ Time string }{Time: time.Now().Format(time.DateTime)})
	},
}

//nolint:gochecknoinits
func init() {
	rootCmd.AddCommand(loginCmd)
}
