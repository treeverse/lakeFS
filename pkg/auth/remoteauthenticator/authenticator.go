package remoteauthenticator

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"

	"github.com/treeverse/lakefs/pkg/auth"
	"github.com/treeverse/lakefs/pkg/auth/model"
	"github.com/treeverse/lakefs/pkg/config"
	"github.com/treeverse/lakefs/pkg/logging"
)

const RemoteAuthSource = "remote_authenticator"

// AuthenticationRequest is the request object that will be sent to the remote authenticator service as JSON payload in a POST request
type AuthenticationRequest struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

// AuthenticationResponse is the expected response from the remote authenticator service
type AuthenticationResponse struct {
	// ExternalUserIdentifier is optional, if returned then the user will be used as the official username in lakeFS
	ExternalUserIdentifier *string `json:"external_user_identifier,omitempty"`
}

// RemoteAuthenticator client
type RemoteAuthenticator struct {
	AuthService auth.Service
	Logger      logging.Logger
	Config      *config.RemoteAuthenticator
	serviceURL  string
	httpClient  *http.Client
}

func NewRemoteAuthenticator(conf *config.RemoteAuthenticator, authService auth.Service, logger logging.Logger) (auth.Authenticator, error) {
	serviceURL, err := url.JoinPath(conf.BaseURL, conf.AuthEndpoint)
	if err != nil {
		return nil, err
	}
	logger.WithField("auth_url", serviceURL).Info("initializing remote authenticator")

	return &RemoteAuthenticator{
		Logger:      logger,
		Config:      conf,
		serviceURL:  serviceURL,
		AuthService: authService,
	}, nil
}

func (ra *RemoteAuthenticator) client() *http.Client {
	if ra.httpClient == nil {
		ra.httpClient = http.DefaultClient
		ra.httpClient.Timeout = ra.Config.RequestTimeout
	}
	return ra.httpClient
}

func (ra *RemoteAuthenticator) doRequest(ctx context.Context, username, password string, log logging.Logger) ([]byte, error) {
	payload, err := json.Marshal(&AuthenticationRequest{Username: username, Password: password})

	if err != nil {
		return nil, fmt.Errorf("failed marshaling request body: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, ra.serviceURL, bytes.NewReader(payload))

	if err != nil {
		return nil, fmt.Errorf("failed creating request to remote authenticator: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	log = log.WithField("url", req.URL.String())

	client := ra.client()

	log.Debug("starting http request to remote authenticator")

	resp, err := client.Do(req)

	if err != nil {
		return nil, fmt.Errorf("failed sending request to remote authenticator: %w", err)
	}

	log = log.WithField("status_code", resp.StatusCode)

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("bad status code %d: %w", resp.StatusCode, auth.ErrUnexpectedStatusCode)
	}

	log.Debug("got response from remote authenticator")

	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed reading response body: %w", err)
	}
	return body, nil
}

func (ra *RemoteAuthenticator) AuthenticateUser(ctx context.Context, username, password string) (string, error) {
	log := ra.Logger.WithContext(ctx).WithField("input_username", username)

	data, err := ra.doRequest(ctx, username, password, log)

	if err != nil {
		return "", fmt.Errorf("doing http request %s: %w", username, err)
	}

	var res AuthenticationResponse

	if err := json.Unmarshal(data, &res); err != nil {
		return "", fmt.Errorf("unmarshaling authenticator response %s: %w", username, err)
	}

	dbUsername := username

	// if the external authentication service provided an external user identifier, use it as the username

	if res.ExternalUserIdentifier != nil && *res.ExternalUserIdentifier != "" {
		log = ra.Logger.WithField("external_user_identifier", *res.ExternalUserIdentifier)
		dbUsername = *res.ExternalUserIdentifier
	}

	user, err := ra.AuthService.GetUser(ctx, dbUsername)

	if err == nil {
		log.WithField("user", fmt.Sprintf("%+v", user)).Debug("Got existing user")
		return user.Username, nil
	}
	if !errors.Is(err, auth.ErrNotFound) {
		return "", fmt.Errorf("get user %s: %w", dbUsername, err)
	}

	log.Info("could not get user; creating them")

	newUser := &model.User{
		CreatedAt:    time.Now().UTC(),
		Username:     dbUsername,
		FriendlyName: &username,
		Source:       RemoteAuthSource,
	}

	_, err = ra.AuthService.CreateUser(ctx, newUser)
	if err != nil {
		return "", fmt.Errorf("create backing user for remote auth user %s: %w", newUser.Username, err)
	}
	_, err = ra.AuthService.CreateCredentials(ctx, newUser.Username)
	if err != nil {
		return "", fmt.Errorf("create credentials for remote auth user %s: %w", newUser.Username, err)
	}

	err = ra.AuthService.AddUserToGroup(ctx, newUser.Username, ra.Config.DefaultUserGroup)
	if err != nil {
		return "", fmt.Errorf("add newly created remote auth user %s to %s: %w", newUser.Username, ra.Config.DefaultUserGroup, err)
	}
	return newUser.Username, nil
}

func (ra *RemoteAuthenticator) String() string {
	return RemoteAuthSource
}
