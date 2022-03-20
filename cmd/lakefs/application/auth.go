package application

import (
	"context"
	"fmt"
	"github.com/go-ldap/ldap/v3"
	"github.com/treeverse/lakefs/pkg/auth"
	"github.com/treeverse/lakefs/pkg/auth/crypt"
	"github.com/treeverse/lakefs/pkg/config"
	"net"
	"time"
)

type AuthService struct {
	authenticator       auth.Authenticator
	dbAuthService       *auth.DBAuthService
	authMetadataManager *auth.DBMetadataManager
}

func newLDAPAuthenticator(cfg *config.LDAP, service auth.Service) *auth.LDAPAuthenticator {
	const (
		connectionTimeout = 15 * time.Second
		requestTimeout    = 7 * time.Second
	)
	group := cfg.DefaultUserGroup
	if group == "" {
		group = auth.ViewersGroup
	}
	return &auth.LDAPAuthenticator{
		AuthService:       service,
		BindDN:            cfg.BindDN,
		BindPassword:      cfg.BindPassword,
		DefaultUserGroup:  group,
		UsernameAttribute: cfg.UsernameAttribute,
		MakeLDAPConn: func(_ context.Context) (*ldap.Conn, error) {
			c, err := ldap.DialURL(
				cfg.ServerEndpoint,
				ldap.DialWithDialer(&net.Dialer{Timeout: connectionTimeout}),
			)
			if err != nil {
				return nil, fmt.Errorf("dial %s: %w", cfg.ServerEndpoint, err)
			}
			c.SetTimeout(requestTimeout)
			// TODO(ariels): Support StartTLS (& other TLS configuration).
			return c, nil
		},
		BaseSearchRequest: ldap.SearchRequest{
			BaseDN:     cfg.UserBaseDN,
			Scope:      ldap.ScopeWholeSubtree,
			Filter:     cfg.UserFilter,
			Attributes: []string{cfg.UsernameAttribute},
		},
	}
}

func (databaseService DatabaseService) NewAuthService(cfg config.Config, version string) *AuthService {
	dbAuthService := auth.NewDBAuthService(
		databaseService.dbPool,
		crypt.NewSecretStore(cfg.GetAuthEncryptionSecret()),
		cfg.GetAuthCacheConfig())
	var authenticator auth.Authenticator = auth.NewBuiltinAuthenticator(dbAuthService)
	ldapConfig := cfg.GetLDAPConfiguration()
	if ldapConfig != nil {
		ldapAuthenticator := newLDAPAuthenticator(ldapConfig, dbAuthService)
		authenticator = auth.NewChainAuthenticator(authenticator, ldapAuthenticator)
	}
	authMetadataManager := auth.NewDBMetadataManager(version, cfg.GetFixedInstallationID(), databaseService.dbPool)
	return &AuthService{
		authenticator,
		dbAuthService,
		authMetadataManager,
	}
}
