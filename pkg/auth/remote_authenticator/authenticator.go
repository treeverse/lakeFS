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

type AuthenticationRequest struct {
	Username string `json:"username"`
	Password string `json:"password"`
}
type AuthenticationResponse struct {
	SystemUsername string `json:"system_username"`
}

type AuthenticatorConfig struct {
	BaseURL      string
	AuthEndpoint string
}

// RemoteAuthenticator
type RemoteAuthenticator struct {
	AuthService auth.Service
	Logger      logging.Logger
	Config      AuthenticatorConfig
	authURL     string
	c           *http.Client
}

func (ra *RemoteAuthenticator) client() *http.Client {
	if ra.c == nil {
		ra.c = &http.Client{}
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

func (ra *RemoteAuthenticator) doRequest(req *http.Request) ([]byte, error) {
	client := ra.client()
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		ra.Logger.WithError(err)
		return nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		ra.Logger.WithError(err)
		return nil, err
	}
	return body, nil
}

func NewRemoteAuthenticator(conf AuthenticatorConfig, logger logging.Logger) auth.Authenticator {
	return &RemoteAuthenticator{
		Logger:  logger,
		Config:  conf,
		authURL: path.Join(conf.BaseURL, conf.AuthEndpoint),
	}
}

func (ra *RemoteAuthenticator) AuthenticateUser(ctx context.Context, username, password string) (string, error) {
	req, err := ra.newRequest(ctx, username, password)
	// todo(isan) add bad request error
	if err != nil {
		ra.Logger.WithError(err)
		return "", err
	}

	data, err := ra.doRequest(req)

	if err != nil {
		return "", err
	}

	var res *AuthenticationResponse

	// todo(isan) add bad response error
	if err := json.Unmarshal(data, res); err != nil {
		return "", err
	}

	dbUsername := username

	if res.SystemUsername != "" {
		dbUsername = res.SystemUsername
	}

	user, err := ra.AuthService.GetUser(ctx, dbUsername)
	if err == nil {
		ra.Logger.WithField("user", fmt.Sprintf("%+v", user)).Debug("Got existing user")
		return user.Username, nil
	}
	if !errors.Is(err, ErrNotFound) {
		ra.Logger.WithError(err).Info("Could not get user; create them")
	}

	newUser := &model.User{
		CreatedAt:    time.Now(),
		Username:     dbUsername,
		FriendlyName: &username,
		Source:       "remote_authenticator",
	}

	return "", nil
}

func (la *RemoteAuthenticator) String() string {
	return "Remote authenticator"
}
