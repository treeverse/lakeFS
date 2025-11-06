package cmd

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"slices"
	"time"

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
	loginRetryStatuses = slices.Concat(lakectlDefaultRetryStatuses,
		[]int{http.StatusNotFound},
	)
)

func loginRetryPolicy(ctx context.Context, resp *http.Response, err error) (bool, error) {
	return CheckRetry(ctx, resp, err, loginRetryStatuses)
}

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

		client := getClient(apigen.WithHTTPClient(getHTTPClient(loginRetryPolicy)))
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

		// client will retry; use this to wait for login.
		//
		// TODO(ariels): The timeouts on some lakectl configurations may be too low for
		// convenient login.  Consider using a RetryClient based on a different
		// configuration here..
		resp, err := client.GetTokenFromMailboxWithResponse(cmd.Context(), mailbox)

		DieOnErrorOrUnexpectedStatusCode(resp, err, http.StatusOK)

		loginToken := resp.JSON200
		if loginToken == nil {
			Die("No login token", 1)
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
