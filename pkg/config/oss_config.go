package config

import (
	"fmt"
	"strings"

	"github.com/treeverse/lakefs/pkg/logging"
)

type ConfigImpl struct {
	BaseConfig `mapstructure:",squash"`
	Auth       Auth `mapstructure:"auth"`
	UI         UI   `mapstructure:"ui"`
}

func (c *ConfigImpl) AuthConfig() AuthConfig {
	return &c.Auth
}

func (c *ConfigImpl) UIConfig() UIConfig {
	return &c.UI
}

func (c *ConfigImpl) Validate() error {
	missingKeys := ValidateMissingRequiredKeys(c, "mapstructure", "squash")
	if len(missingKeys) > 0 {
		return fmt.Errorf("%w: %v", ErrMissingRequiredKeys, missingKeys)
	}
	return ValidateBlockstore(&c.Blockstore)
}

func BuildConfig(cfgType string) (Config, error) {
	c := &ConfigImpl{}
	_, err := NewConfig(cfgType, c)
	if err != nil {
		return nil, err
	}

	// Perform required validations
	if err = c.Validate(); err != nil {
		return nil, err
	}
	c.warnDeprecatedKeys()

	err = c.ValidateDomainNames()
	if err != nil {
		return nil, err
	}

	return c, nil
}

// warnDeprecatedKeys logs every configured key whose value is no longer used.
func (c *ConfigImpl) warnDeprecatedKeys() {
	const enterpriseHint = " Single sign-on and role-based access control are available in lakeFS Enterprise."
	a := &c.Auth
	deprecated := []struct {
		set bool
		key string
	}{
		{c.Logging.TraceRequestHeadersDeprecated, "logging.trace_request_headers"},
		{a.APIDeprecated.Endpoint != "", "auth.api.endpoint"},
		{a.APIDeprecated.Token != "", "auth.api.token"},
		{a.APIDeprecated.SupportsInvites, "auth.api.supports_invites"},
		{a.APIDeprecated.HealthCheckTimeout != 0, "auth.api.health_check_timeout"},
		{a.APIDeprecated.SkipHealthCheck, "auth.api.skip_health_check"},
		{a.AuthenticationAPIDeprecated.Endpoint != "", "auth.authentication_api.endpoint"},
		{a.AuthenticationAPIDeprecated.ExternalPrincipalsEnabled, "auth.authentication_api.external_principals_enabled"},
		{a.RemoteAuthenticatorDeprecated.Enabled, "auth.remote_authenticator.enabled"},
		{a.RemoteAuthenticatorDeprecated.Endpoint != "", "auth.remote_authenticator.endpoint"},
		{a.RemoteAuthenticatorDeprecated.DefaultUserGroup != "", "auth.remote_authenticator.default_user_group"},
		{a.RemoteAuthenticatorDeprecated.RequestTimeout != 0, "auth.remote_authenticator.request_timeout"},
		{len(a.OIDCDeprecated.ValidateIDTokenClaims) > 0, "auth.oidc.validate_id_token_claims"},
		{len(a.OIDCDeprecated.DefaultInitialGroups) > 0, "auth.oidc.default_initial_groups"},
		{a.OIDCDeprecated.InitialGroupsClaimName != "", "auth.oidc.initial_groups_claim_name"},
		{a.OIDCDeprecated.FriendlyNameClaimName != "", "auth.oidc.friendly_name_claim_name"},
		{a.OIDCDeprecated.PersistFriendlyName, "auth.oidc.persist_friendly_name"},
		{len(a.CookieAuthVerificationDeprecated.ValidateIDTokenClaims) > 0, "auth.cookie_auth_verification.validate_id_token_claims"},
		{len(a.CookieAuthVerificationDeprecated.DefaultInitialGroups) > 0, "auth.cookie_auth_verification.default_initial_groups"},
		{a.CookieAuthVerificationDeprecated.InitialGroupsClaimName != "", "auth.cookie_auth_verification.initial_groups_claim_name"},
		{a.CookieAuthVerificationDeprecated.FriendlyNameClaimName != "", "auth.cookie_auth_verification.friendly_name_claim_name"},
		{a.CookieAuthVerificationDeprecated.ExternalUserIDClaimName != "", "auth.cookie_auth_verification.external_user_id_claim_name"},
		{a.CookieAuthVerificationDeprecated.AuthSource != "", "auth.cookie_auth_verification.auth_source"},
		{a.CookieAuthVerificationDeprecated.PersistFriendlyName, "auth.cookie_auth_verification.persist_friendly_name"},
		{a.LoginMaxDurationDeprecated != 0, "auth.login_max_duration"},
		{a.RBACDeprecated != "", "auth.ui_config.rbac"},
		{a.LoginURLDeprecated != "", "auth.ui_config.login_url"},
		{a.FallbackLoginURLDeprecated != nil, "auth.ui_config.fallback_login_url"},
		{a.FallbackLoginLabelDeprecated != nil, "auth.ui_config.fallback_login_label"},
		{len(a.LoginCookieNamesDeprecated) > 0, "auth.ui_config.login_cookie_names"},
		{a.LogoutURLDeprecated != "", "auth.ui_config.logout_url"},
		{a.UseLoginPlaceholdersDeprecated, "auth.ui_config.use_login_placeholders"},
	}
	for _, d := range deprecated {
		if !d.set {
			continue
		}
		msg := d.key + " is deprecated. Value is no longer used."
		if strings.HasPrefix(d.key, "auth.") {
			msg += enterpriseHint
		}
		logging.ContextUnavailable().Warn(msg)
	}
}
