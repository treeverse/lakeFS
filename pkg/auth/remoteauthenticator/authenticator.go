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

	"github.com/go-openapi/swag"
	"github.com/treeverse/lakefs/pkg/auth"
	"github.com/treeverse/lakefs/pkg/auth/model"
	"github.com/treeverse/lakefs/pkg/logging"
)

const RemoteAuthSource = "remote_authenticator"

// RemoteAuthenticatorConfig holds remote authentication configuration.
type RemoteAuthenticatorConfig struct {
	// Enabled if set true will enable remote authentication
	Enabled bool
	// BaseURL is the base URL of the remote authentication service (e.g. https://my-auth.example.com)
	BaseURL string
	// AuthEndpoint is the endpoint to authenticate users (e.g. /auth)
	AuthEndpoint string
	// DefaultUserGroup is the default group for the users authenticated by the remote service
	DefaultUserGroup string
	// RequestTimeout timeout for remote authentication requests
	RequestTimeout time.Duration
}

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
	Config      RemoteAuthenticatorConfig
	serviceURL  string
	client      *http.Client
}

func NewRemoteAuthenticator(conf RemoteAuthenticatorConfig, authService auth.Service, logger logging.Logger) (auth.Authenticator, error) {

	if conf.BaseURL == "" {
		return nil, errors.New("remote authenticator base URL is required")
	}

	serviceURL, err := url.JoinPath(conf.BaseURL, conf.AuthEndpoint)
	if err != nil {
		return nil, err
	}
	httpClient := &http.Client{Timeout: conf.RequestTimeout}

	log := logger.WithField("service_name", "remote_authenticator")

	log.WithFields(logging.Fields{
		"auth_url":        serviceURL,
		"request_timeout": httpClient.Timeout,
	}).Info("initializing remote authenticator")

	return &RemoteAuthenticator{
		Logger:      log,
		Config:      conf,
		serviceURL:  serviceURL,
		AuthService: authService,
		client:      httpClient,
	}, nil
}

func (ra *RemoteAuthenticator) doRequest(ctx context.Context, log logging.Logger, username, password string) (*AuthenticationResponse, error) {
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

	log.Trace("starting http request to remote authenticator")

	resp, err := ra.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed sending request to remote authenticator: %w", err)
	}
	defer resp.Body.Close()

	log = log.WithField("status_code", resp.StatusCode)

	if resp.StatusCode < http.StatusOK || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("bad status code %d: %w", resp.StatusCode, auth.ErrUnexpectedStatusCode)
	}

	log.Debug("got response from remote authenticator")

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed reading response body: %w", err)
	}

	var res AuthenticationResponse
	if err := json.Unmarshal(body, &res); err != nil {
		return nil, fmt.Errorf("unmarshaling authenticator response %s: %w", username, err)
	}

	return &res, nil
}

func (ra *RemoteAuthenticator) AuthenticateUser(ctx context.Context, username, password string) (string, error) {
	log := ra.Logger.WithContext(ctx).WithField("input_username", username)

	res, err := ra.doRequest(ctx, log, username, password)
	if err != nil {
		return "", err
	}

	dbUsername := username

	// if the external authentication service provided an external user identifier, use it as the username
	externalUserIdentifier := swag.StringValue(res.ExternalUserIdentifier)
	if externalUserIdentifier != "" {
		log = log.WithField("external_user_identifier", externalUserIdentifier)
		dbUsername = externalUserIdentifier
	}

	user, err := ra.AuthService.GetUser(ctx, dbUsername)
	if err == nil {
		log.WithField("user", fmt.Sprintf("%+v", user)).Debug("Got existing user")
		return user.Username, nil
	}
	if !errors.Is(err, auth.ErrNotFound) {
		return "", fmt.Errorf("get user %s: %w", dbUsername, err)
	}

	log.Info("first time remote authenticated user, creating them")

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

	err = ra.AuthService.AddUserToGroup(ctx, newUser.Username, ra.Config.DefaultUserGroup)
	if err != nil {
		return "", fmt.Errorf("add newly created remote auth user %s to %s: %w", newUser.Username, ra.Config.DefaultUserGroup, err)
	}
	return newUser.Username, nil
}

func (ra *RemoteAuthenticator) String() string {
	return RemoteAuthSource
}
