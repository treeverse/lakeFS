package auth

import (
	"context"
	"crypto/subtle"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/go-ldap/ldap/v3"
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
	AuthenticateUser(ctx context.Context, username, password string) (int, error)
}

// Credentialler fetches S3-style credentials for access keys.
type Credentialler interface {
	GetCredentials(ctx context.Context, accessKeyID string) (*model.Credential, error)
}

// NewChainAuthenticator returns an Authenticator that authenticates users
// by trying each auth in order.
func NewChainAuthenticator(auth ...Authenticator) Authenticator {
	return ChainAuthenticator(auth)
}

// ChainAuthenticator authenticates users by trying each Authenticator in
// order, returning the last error in case all fail.
type ChainAuthenticator []Authenticator

func (ca ChainAuthenticator) AuthenticateUser(ctx context.Context, username, password string) (int, error) {
	var (
		err error
		id  int
	)
	logger := logging.FromContext(ctx).WithField("username", username)
	for _, a := range ca {
		id, err = a.AuthenticateUser(ctx, username, password)
		if err == nil {
			return id, nil
		}
		// TODO(ariels): Add authenticator ID here.
		logger.WithError(err).Info("Failed to authenticate user")
	}
	return InvalidUserID, err
}

type EmailAuthenticator struct {
	AuthService Service
}

func NewEmailAuthenticator(service Service) *EmailAuthenticator {
	return &EmailAuthenticator{AuthService: service}
}

func (e EmailAuthenticator) AuthenticateUser(ctx context.Context, username, password string) (int, error) {
	user, err := e.AuthService.GetUserByEmail(ctx, username)
	if err != nil {
		return InvalidUserID, err
	}

	if err := user.Authenticate(password); err != nil {
		return InvalidUserID, err
	}

	return user.ID, nil
}

// BuiltinAuthenticator authenticates users by their access key IDs and
// passwords stored in the auth service.
type BuiltinAuthenticator struct {
	creds Credentialler
}

func NewBuiltinAuthenticator(service Service) *BuiltinAuthenticator {
	return &BuiltinAuthenticator{creds: service}
}

func (ba *BuiltinAuthenticator) AuthenticateUser(ctx context.Context, username, password string) (int, error) {
	// Look user up in DB.  username is really the access key ID.
	cred, err := ba.creds.GetCredentials(ctx, username)
	if err != nil {
		return InvalidUserID, err
	}
	if subtle.ConstantTimeCompare([]byte(password), []byte(cred.SecretAccessKey)) != 1 {
		return InvalidUserID, ErrInvalidSecretAccessKey
	}
	return cred.UserID, nil
}

// LDAPAuthenticator authenticates users on an LDAP server.  It currently
// supports only simple authentication.
type LDAPAuthenticator struct {
	AuthService Service

	MakeLDAPConn      func(ctx context.Context) (*ldap.Conn, error)
	BindDN            string
	BindPassword      string
	BaseSearchRequest ldap.SearchRequest
	UsernameAttribute string
	DefaultUserGroup  string

	// control is bound to the operator (BindDN) and is used to query
	// LDAP about users.
	control *ldap.Conn
}

func (la *LDAPAuthenticator) getControlConnection(ctx context.Context) (*ldap.Conn, error) {
	// LDAP connections are "closing" even after they've closed.
	if la.control != nil && !la.control.IsClosing() {
		return la.control, nil
	}
	control, err := la.MakeLDAPConn(ctx)
	if err != nil {
		return nil, fmt.Errorf("open control connection: %w", err)
	}
	if la.BindPassword == "" {
		err = control.UnauthenticatedBind(la.BindDN)
	} else {
		err = control.Bind(la.BindDN, la.BindPassword)
	}
	if err != nil {
		return nil, fmt.Errorf("bind control connection to %s: %w", la.BindDN, err)
	}
	la.control = control
	return la.control, nil
}

func (la *LDAPAuthenticator) resetControlConnection() {
	go la.control.Close() // Don't wait for dead connection to shut itself down
	la.control = nil
}

// inBrackets returns filter (which should already be properly escaped)
// enclosed in brackets if it does not start with open brackets.
func inBrackets(filter string) string {
	filter = strings.TrimLeft(filter, " \t\n\r")
	if filter == "" {
		return filter
	}
	if filter[0] == '(' {
		return filter
	}
	return fmt.Sprintf("(%s)", filter)
}

func (la *LDAPAuthenticator) AuthenticateUser(ctx context.Context, username, password string) (int, error) {
	// There may be multiple authenticators.  Log everything to allow debugging.
	logger := logging.FromContext(ctx).WithField("username", username)
	controlConn, err := la.getControlConnection(ctx)
	if err != nil {
		logger.WithError(err).Error("Failed to bind LDAP control connection")
		return InvalidUserID, fmt.Errorf("LDAP bind control: %w", err)
	}

	searchRequest := la.BaseSearchRequest
	searchRequest.Filter = fmt.Sprintf("(&(%s=%s)%s)", la.UsernameAttribute, ldap.EscapeFilter(username), inBrackets(searchRequest.Filter))

	res, err := controlConn.SearchWithPaging(&searchRequest, 2)
	if err != nil {
		logger.WithError(err).Error("Failed to search for DN by username")
		if errors.Is(err, os.ErrDeadlineExceeded) {
			la.resetControlConnection()
		}
		return InvalidUserID, fmt.Errorf("LDAP find user %s: %w", username, err)
	}
	if logger.IsTracing() {
		for _, e := range res.Entries {
			logger.WithField("entry", fmt.Sprintf("%+v", e)).Trace("LDAP entry")
		}
	}
	if len(res.Entries) == 0 {
		logger.WithError(err).Debug("No users found")
		return InvalidUserID, fmt.Errorf("LDAP find user %s: %w", username, ErrNotFound)
	}
	if len(res.Entries) > 1 {
		logger.WithError(err).Error("Too many users found")
		return InvalidUserID, fmt.Errorf("LDAP find user %s: %w", username, ErrNonUnique)
	}

	dn := res.Entries[0].DN

	// TODO(ariels): This should be on the audit log.
	logger = logger.WithField("dn", dn)
	logger.Trace("Authenticate user")

	userConn, err := la.MakeLDAPConn(ctx)
	if err != nil {
		logger.WithError(err).Error("Open per-user LDAP connection")
		return InvalidUserID, fmt.Errorf("LDAP connect for user auth: %w", err) //nolint:stylecheck
	}
	defer userConn.Close()
	err = userConn.Bind(dn, password)
	if err != nil {
		logger.WithError(err).Debug("Failed to bind user")
		return InvalidUserID, fmt.Errorf("user auth failed: %w", err)
	}

	// TODO(ariels): Should users be stored by their DNs or by their
	//     usernames?  (Also below in user passed to CreateUser).
	user, err := la.AuthService.GetUser(ctx, dn)
	if err == nil {
		logger.WithField("user", fmt.Sprintf("%+v", user)).Debug("Got existing user")
		return user.ID, nil
	}

	if !errors.Is(err, ErrNotFound) {
		logger.WithError(err).Info("Could not get user; create them")
	}

	user = &model.User{
		CreatedAt:    time.Now(),
		Username:     dn,
		FriendlyName: &username,
		Source:       "ldap",
	}
	id, err := la.AuthService.CreateUser(ctx, user)
	if err != nil {
		return InvalidUserID, fmt.Errorf("create backing user for LDAP user %s: %w", dn, err)
	}
	_, err = la.AuthService.CreateCredentials(ctx, dn)
	if err != nil {
		return InvalidUserID, fmt.Errorf("create credentials for LDAP user %s: %w", dn, err)
	}

	err = la.AuthService.AddUserToGroup(ctx, dn, la.DefaultUserGroup)
	if err != nil {
		return InvalidUserID, fmt.Errorf("add newly created LDAP user %s to %s: %w", dn, la.DefaultUserGroup, err)
	}
	return id, nil
}
