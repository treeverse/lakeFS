package remoteauthenticator

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/go-openapi/swag"
	"github.com/treeverse/lakefs/pkg/auth"
	"github.com/treeverse/lakefs/pkg/logging"
)

const remoteAuthSource = "remote_authenticator"

var ErrBadConfig = errors.New("invalid configuration")

// AuthenticatorConfig holds authentication configuration.
type AuthenticatorConfig struct {
	// Enabled if set true will enable authenticator
	Enabled bool
	// Endpoint URL of the remote authentication service (e.g. https://my-auth.example.com/auth)
	Endpoint string
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

// Authenticator client
type Authenticator struct {
	AuthService auth.Service
	Logger      logging.Logger
	Config      AuthenticatorConfig
	client      *http.Client
}

func NewAuthenticator(conf AuthenticatorConfig, authService auth.Service, logger logging.Logger) (*Authenticator, error) {
	if conf.Endpoint == "" {
		return nil, fmt.Errorf("endpoint is empty: %w", ErrBadConfig)
	}

	httpClient := &http.Client{Timeout: conf.RequestTimeout}

	log := logger.WithField("service_name", remoteAuthSource)

	log.WithFields(logging.Fields{
		"auth_url":        conf.Endpoint,
		"request_timeout": httpClient.Timeout,
	}).Info("initializing remote authenticator")

	return &Authenticator{
		Logger:      log,
		Config:      conf,
		AuthService: authService,
		client:      httpClient,
	}, nil
}

func (ra *Authenticator) doRequest(ctx context.Context, log logging.Logger, username, password string) (*AuthenticationResponse, error) {
	payload, err := json.Marshal(&AuthenticationRequest{Username: username, Password: password})
	if err != nil {
		return nil, fmt.Errorf("failed marshaling request body: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, ra.Config.Endpoint, bytes.NewReader(payload))
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
	defer func() { _ = resp.Body.Close() }()

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

func (ra *Authenticator) AuthenticateUser(ctx context.Context, username, password string) (string, error) {
	log := ra.Logger.WithContext(ctx).WithField("input_username", username)

	res, err := ra.doRequest(ctx, log, username, password)
	if err != nil {
		return "", err
	}

	dbUsername := username
	// if the external authentication service provided an external user identifier, use it as the username
	externalUserIdentifier := swag.StringValue(res.ExternalUserIdentifier)
	if externalUserIdentifier != "" {
		dbUsername = externalUserIdentifier
	}
	user, err := auth.GetOrCreateUser(ctx, log, ra.AuthService, dbUsername, username, ra.Config.DefaultUserGroup, remoteAuthSource)
	if err != nil {
		return "", fmt.Errorf("get or create user: %w", err)
	}
	return user.Username, nil
}

func (ra *Authenticator) String() string {
	return remoteAuthSource
}
