package remote_authenticator

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"path"
	"time"

	"github.com/treeverse/lakefs/pkg/auth"
	"github.com/treeverse/lakefs/pkg/auth/model"
	"github.com/treeverse/lakefs/pkg/logging"
)

const RemoteAuthSource = "remote_authenticator"

// Request object that will be sent to the remote authenticator service as JSON payload in a POST request
type AuthenticationRequest struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

// Expected response from the remote authenticator service
type AuthenticationResponse struct {
	// optional, if returned then the user will be used as the official username in lakeFS
	ExternalUserIdentifier string `json:"external_user_identifier"`
}

type AuthenticatorConfig struct {
	// BaseURL is the base URL of the remote authentication service (e.g. https://my-auth.com)
	BaseURL string
	// AuthEndpoint is the endpoint to authenticate users (e.g. /auth)
	AuthEndpoint string
	// DefaultUserGroup is the default group to add users to in LakeFS permissions context
	DefaultUserGroup string
}

// RemoteAuthenticator client
type RemoteAuthenticator struct {
	AuthService auth.Service
	Logger      logging.Logger
	Config      AuthenticatorConfig
	authURL     string
	c           *http.Client
}

func NewRemoteAuthenticator(conf AuthenticatorConfig, logger logging.Logger) auth.Authenticator {
	authUrl := path.Join(conf.BaseURL, conf.AuthEndpoint)
	logger.Debugf("initializing remote authenticator with auth url %s", authUrl)
	return &RemoteAuthenticator{
		Logger:  logger,
		Config:  conf,
		authURL: authUrl,
	}
}

func (ra *RemoteAuthenticator) client() *http.Client {
	if ra.c == nil {
		ra.c = http.DefaultClient
	}
	return ra.c
}

func (ra *RemoteAuthenticator) newRequest(ctx context.Context, username, password string) (*http.Request, error) {
	payload, err := json.Marshal(&AuthenticationRequest{Username: username, Password: password})

	if err != nil {
		return nil, err
	}

	req, err := http.NewRequestWithContext(ctx, "POST", ra.authURL, bytes.NewBuffer(payload))
	return req, err
}

func (ra *RemoteAuthenticator) doRequest(logger logging.Logger, req *http.Request) ([]byte, error) {
	client := ra.client()
	log := logger.WithField("url", req.URL.String())
	log.Debug("sending request to remote authenticator")

	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)

	if err != nil {
		log.WithError(err).Debug("failed sending request to remote authenticator")
		return nil, err
	}
	log = log.WithField("status_code", resp.StatusCode)
	log.Debug("got response from remote authenticator")

	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		log.WithError(err).Debug("failed reading response body")
		return nil, err
	}
	return body, nil
}

func (ra *RemoteAuthenticator) AuthenticateUser(ctx context.Context, username, password string) (string, error) {
	log := ra.Logger.WithField("input_username", username)

	req, err := ra.newRequest(ctx, username, password)

	if err != nil {
		log.WithError(err).Error("failed creating request")
		return "", auth.ErrInvalidRequest
	}

	data, err := ra.doRequest(log, req)

	if err != nil {
		return "", fmt.Errorf("doing http request %s: %w", username, err)
	}

	var res *AuthenticationResponse

	if err := json.Unmarshal(data, res); err != nil {
		return "", fmt.Errorf("unmarshaling authenticator response %s: %w", username, err)
	}

	dbUsername := username

	// if the external authentication service provided an external user identifier, use it as the username
	log = ra.Logger.WithField("external_user_identifier", res.ExternalUserIdentifier)
	if res.ExternalUserIdentifier != "" {
		dbUsername = res.ExternalUserIdentifier
	}

	user, err := ra.AuthService.GetUser(ctx, dbUsername)

	if err == nil {
		log.WithField("user", fmt.Sprintf("%+v", user)).Debug("Got existing user")
		return user.Username, nil
	}
	if !errors.Is(err, auth.ErrNotFound) {
		log.WithError(err).Info("Could not get user; createing them")
	}

	newUser := &model.User{
		CreatedAt:    time.Now(),
		Username:     dbUsername,
		FriendlyName: &username,
		Source:       RemoteAuthSource,
	}

	_, err = ra.AuthService.CreateUser(ctx, newUser)
	if err != nil {
		return "", fmt.Errorf("create backing user for remote auth user %s: %w", dbUsername, err)
	}
	_, err = ra.AuthService.CreateCredentials(ctx, dbUsername)
	if err != nil {
		return "", fmt.Errorf("create credentials for remote auth user %s: %w", dbUsername, err)
	}

	err = ra.AuthService.AddUserToGroup(ctx, dbUsername, ra.Config.DefaultUserGroup)
	if err != nil {
		return "", fmt.Errorf("add newly created remote auth user %s to %s: %w", dbUsername, ra.Config.DefaultUserGroup, err)
	}
	return newUser.Username, nil
}

func (la *RemoteAuthenticator) String() string {
	return RemoteAuthSource
}
