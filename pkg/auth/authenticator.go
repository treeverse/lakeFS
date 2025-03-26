package auth

import (
	"context"
	"crypto/subtle"
	"errors"
	"fmt"
	"time"

	"github.com/hashicorp/go-multierror"
	"github.com/treeverse/lakefs/pkg/auth/model"
	"github.com/treeverse/lakefs/pkg/logging"
)

// Authenticator authenticates users returning an identifier for the user.
// (Currently it handles only username+password single-step authentication.
// This interface will need to change significantly in order to support
// challenge-response protocols.)
type Authenticator interface {
	// AuthenticateUser authenticates a user matching username and
	// password and returns their ID.
	AuthenticateUser(ctx context.Context, username, password string) (string, error)
}

// Credentialler fetches S3-style credentials for access keys.
type Credentialler interface {
	GetCredentials(ctx context.Context, accessKeyID string) (*model.Credential, error)
}

// ChainAuthenticator authenticates users by trying each Authenticator in
// order, returning the last error in case all fail.
type ChainAuthenticator []Authenticator

func (ca ChainAuthenticator) AuthenticateUser(ctx context.Context, username, password string) (string, error) {
	var merr *multierror.Error
	logger := logging.FromContext(ctx).WithField("username", username)
	for _, a := range ca {
		id, err := a.AuthenticateUser(ctx, username, password)
		if err == nil {
			return id, nil
		}
		// TODO(ariels): Add authenticator ID here.
		merr = multierror.Append(merr, fmt.Errorf("%s: %w", a, err))
	}
	logger.WithError(merr).Info("Failed to authenticate user")
	return InvalidUserID, merr
}

// BuiltinAuthenticator authenticates users by their access key IDs and
// passwords stored in the auth service.
type BuiltinAuthenticator struct {
	creds Credentialler
}

func NewBuiltinAuthenticator(service Service) *BuiltinAuthenticator {
	return &BuiltinAuthenticator{creds: service}
}

func (ba *BuiltinAuthenticator) AuthenticateUser(ctx context.Context, username, password string) (string, error) {
	// Look user up in DB.  username is really the access key ID.
	cred, err := ba.creds.GetCredentials(ctx, username)
	if err != nil {
		return InvalidUserID, err
	}
	if subtle.ConstantTimeCompare([]byte(password), []byte(cred.SecretAccessKey)) != 1 {
		return InvalidUserID, ErrInvalidSecretAccessKey
	}
	return cred.Username, nil
}

func (ba *BuiltinAuthenticator) String() string {
	return "built in authenticator"
}

// GetOrCreateUser searches for the user by username, and if not found, creates a new user with the given username and
// external user identifier.  It returns the user.
// This function is meant to be used by Authenticator implementations in the ChainAuthenticator.
func GetOrCreateUser(ctx context.Context, authService Service, friendlyName, identifier, defaultUserGroup, source string) (*model.User, error) {
	log := logging.FromContext(ctx).WithField("input_username", friendlyName)
	dbUsername := friendlyName

	// if a user identifier is available, use it as the db username
	if identifier != "" {
		log = log.WithField("external_user_identifier", identifier)
		dbUsername = identifier
	}

	user, err := authService.GetUser(ctx, dbUsername)
	if err == nil {
		log.WithField("user", fmt.Sprintf("%+v", user)).Debug("Got existing user")
		return user, nil
	}
	if !errors.Is(err, ErrNotFound) {
		return nil, fmt.Errorf("get user %s: %w", dbUsername, err)
	}

	log.Info("first time remote authenticated user, creating them")

	newUser := &model.User{
		CreatedAt:    time.Now().UTC(),
		Username:     dbUsername,
		FriendlyName: &friendlyName,
		Source:       source,
	}

	_, err = authService.CreateUser(ctx, newUser)
	if err != nil {
		return nil, fmt.Errorf("create backing user for remote auth user %s: %w", newUser.Username, err)
	}

	err = authService.AddUserToGroup(ctx, newUser.Username, defaultUserGroup)
	if err != nil {
		return nil, fmt.Errorf("add newly created remote auth user %s to %s: %w", newUser.Username, defaultUserGroup, err)
	}
	return newUser, nil
}
