package cmd

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"slices"
	"time"

	"github.com/manifoldco/promptui"
	"github.com/skratchdot/open-golang/open"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/treeverse/lakefs/pkg/api/apigen"
	"github.com/treeverse/lakefs/pkg/config"
	"github.com/treeverse/lakefs/pkg/httputil"
)

const (
	warningNotificationTmpl = `{{ . | yellow }}`
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

	errNoProtocol = errors.New(`missing protocol, try e.g. "https://..."`)
	errNoHost     = errors.New(`missing host, e.g. "https://honey-badger.lakefscloud.us-east-1.io/"`)
)

func loginRetryPolicy(ctx context.Context, resp *http.Response, err error) (bool, error) {
	return CheckRetry(ctx, resp, err, loginRetryStatuses)
}

func validateURL(maybeURL string) error {
	u, err := url.Parse(maybeURL)
	if err != nil {
		return err
	}
	switch {
	case u.Scheme == "":
		return errNoProtocol
	case u.Host == "":
		return errNoHost
	default:
		return nil
	}
}

func isIPAddress(hostname string) bool {
	return net.ParseIP(hostname) != nil
}

func readLoginServerURL() (string, error) {
	var (
		ok        = false
		serverURL *url.URL
		err       error
	)
	Write(warningNotificationTmpl, "No .lakectl.yaml found.  On lakeFS Enterprise, enter the server URL to log in.\n")
	for !ok {
		prompt := promptui.Prompt{
			Label:    "lakeFS server URL",
			Validate: validateURL,
		}
		serverURLString, err := prompt.Run()
		if err != nil {
			return "", err
		}
		serverURL, err = url.Parse(serverURLString)
		if err != nil { // Unlikely, validateURL should have done this!
			return "", err
		}

		host := serverURL.Hostname()
		if isIPAddress(host) {
			prompt = promptui.Prompt{
				IsConfirm: true,
				Default:   "n",
				Label:     "Numeric IP addresses will not work with OIDC; are you sure? ",
			}
			_, err := prompt.Run()
			if err != nil && !errors.Is(err, promptui.ErrAbort) {
				return "", err
			}
			ok = !errors.Is(err, promptui.ErrAbort)
		} else {
			ok = true
		}
	}
	return serverURL.String(), err
}

func configureLogin(serverURL string) error {
	viper.Set("server.endpoint_url", serverURL)
	return viper.SafeWriteConfig()
}

var loginCmd = &cobra.Command{
	Use:     "login",
	Short:   "Use a web browser to log into lakeFS",
	Long:    "Connect to lakeFS using a web browser.",
	Example: "lakectl login",
	Run: func(cmd *cobra.Command, _ []string) {
		var err error

		writeConfigFile := false
		serverEndpointURL := string(cfg.Server.EndpointURL)
		if serverEndpointURL == "" {
			serverEndpointURL, err = readLoginServerURL()
			if err != nil {
				DieFmt("No server endpoint URL: %s", err)
			}
			Write("Read URL {{. | yellow}}\n", serverEndpointURL)
			cfg.Server.EndpointURL = config.OnlyString(serverEndpointURL)
			writeConfigFile = true
		}
		serverURL, err := url.Parse(serverEndpointURL)
		if err != nil {
			DieErr(fmt.Errorf("get server URL %s: %w", cfg.Server.EndpointURL, err))
		}

		httpClient := getHTTPClientWithRetryConfig(loginRetryPolicy, cfg.Server.LoginRetries)
		client := getClient(apigen.WithHTTPClient(httpClient))
		tokenRedirect, err := client.GetTokenRedirectWithResponse(cmd.Context())
		// TODO(ariels): Change back to http.StatusSeeOther after fixing lakeFS server!
		DieOnErrorOrUnexpectedStatusCode(tokenRedirect, err, http.StatusOK)
		header := tokenRedirect.HTTPResponse.Header
		relativeLocation, err := url.Parse(header.Get("location"))
		if err != nil {
			DieErr(fmt.Errorf("parse relative redirect URL %s: %w", header.Get("location"), err))
		}
		mailbox := header.Get(httputil.LoginMailboxHeaderName)

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
		// configuration here.
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

		if writeConfigFile {
			err = configureLogin(serverEndpointURL)
			if err != nil {
				Warning(fmt.Sprintf("Failed to save login configuration: %s.", err))
			}
		}
	},
}

//nolint:gochecknoinits
func init() {
	rootCmd.AddCommand(loginCmd)
}
