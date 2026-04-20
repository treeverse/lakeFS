package jwtidp

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/go-openapi/swag"
	"github.com/jmespath/go-jmespath"
	"github.com/treeverse/lakefs/pkg/auth"
	"github.com/treeverse/lakefs/pkg/auth/model"
	"github.com/treeverse/lakefs/pkg/config"
	"github.com/treeverse/lakefs/pkg/logging"
)

const (
	defaultExternalUserIDClaimRef = "/sub"
	// authSource is written to model.User.Source on provisioning so
	// admins can tell via the user list that a user came from the JWT
	// IdP flow. Matches the hardcoded labels used by the OIDC
	// ("oidc") and remote_authenticator flows.
	authSource = "jwt"
)

// Config drives claim-to-user mapping for a verified JWT. ExternalUserIDClaimRef
// and FriendlyNameClaimRef are RFC 6901 JSON pointers. InitialGroups is a
// pre-compiled JMESPath expression evaluated against the full claim map.
type Config struct {
	// ExternalUserIDClaimRef identifies the subject across logins.
	// Defaults to "/sub".
	ExternalUserIDClaimRef string
	FriendlyNameClaimRef   string
	// InitialGroups is evaluated against the claim map on first
	// provisioning. Nil skips initial group assignment. Expression
	// results are coerced to []string; anything else leaves the user
	// with no initial groups.
	InitialGroups       *jmespath.JMESPath
	PersistFriendlyName bool
}

// NewResolverConfig projects the user-mapping fields of config.JWT
// onto a resolver Config, compiling InitialGroupsPath up-front so
// bad expressions fail at startup rather than on first login.
func NewResolverConfig(cfg *config.JWT) (*Config, error) {
	if cfg == nil {
		return nil, nil
	}
	out := &Config{
		ExternalUserIDClaimRef: cfg.ExternalUserIDClaimRef,
		FriendlyNameClaimRef:   cfg.FriendlyNameClaimRef,
		PersistFriendlyName:    cfg.PersistFriendlyName,
	}
	if cfg.InitialGroupsPath != "" {
		prog, err := jmespath.Compile(cfg.InitialGroupsPath)
		if err != nil {
			return nil, fmt.Errorf("%w: compile initial_groups_path: %w", ErrConfig, err)
		}
		out.InitialGroups = prog
	}
	return out, nil
}

// ResolveUser maps verified JWT claims to a lakeFS user, provisioning
// the user and assigning initial groups on first login.
func ResolveUser(ctx context.Context, logger logging.Logger, svc auth.Service, cfg *Config, claims Claims) (*model.User, error) {
	if cfg == nil {
		return nil, fmt.Errorf("%w: nil resolver config", ErrConfig)
	}
	idRef := cfg.ExternalUserIDClaimRef
	if idRef == "" {
		idRef = defaultExternalUserIDClaimRef
	}

	externalID, ok, err := StringAt(claims.Raw, idRef)
	if err != nil {
		return nil, fmt.Errorf("extract external user id: %w", err)
	}
	if !ok || externalID == "" {
		return nil, fmt.Errorf("%w: external user id claim %q", ErrClaimMissing, idRef)
	}

	friendlyName, _, err := StringAt(claims.Raw, cfg.FriendlyNameClaimRef)
	if err != nil {
		logger.WithError(err).WithField("claim_ref", cfg.FriendlyNameClaimRef).Warn("failed to extract friendly name claim")
		friendlyName = ""
	}
	log := logger.WithFields(logging.Fields{
		"external_id":   externalID,
		"friendly_name": friendlyName,
	})

	user, err := svc.GetUserByExternalID(ctx, externalID)
	if err == nil {
		return enhanceFriendlyName(ctx, logger, svc, user, friendlyName, cfg.PersistFriendlyName), nil
	}
	if !errors.Is(err, auth.ErrNotFound) {
		log.WithError(err).Error("failed to look up user by external id")
		return nil, fmt.Errorf("get user by external id: %w", err)
	}

	log.Info("provisioning user from JWT claims")
	u := model.User{
		CreatedAt:  time.Now().UTC(),
		Source:     authSource,
		Username:   externalID,
		ExternalID: &externalID,
	}
	if cfg.PersistFriendlyName {
		u.FriendlyName = &friendlyName
	}
	if _, err := svc.CreateUser(ctx, &u); err != nil {
		if !errors.Is(err, auth.ErrAlreadyExists) {
			log.WithError(err).Error("failed to create user")
			return nil, fmt.Errorf("create user: %w", err)
		}
		existing, err := svc.GetUserByExternalID(ctx, externalID)
		if err != nil {
			log.WithError(err).Error("failed to re-fetch user after create race")
			return nil, fmt.Errorf("get user by external id: %w", err)
		}
		return enhanceFriendlyName(ctx, logger, svc, existing, friendlyName, cfg.PersistFriendlyName), nil
	}

	for _, g := range evalInitialGroups(log, cfg.InitialGroups, claims.Raw) {
		if err := svc.AddUserToGroup(ctx, u.Username, g); err != nil {
			log.WithError(err).WithField("group", g).Error("failed to add user to group")
		}
	}

	return enhanceFriendlyName(ctx, logger, svc, &u, friendlyName, cfg.PersistFriendlyName), nil
}

// evalInitialGroups runs the compiled JMESPath against the claim map
// and coerces the result to []string. Nil, empty, or ill-typed
// results yield no groups; the user must bake a default into the
// expression (e.g. `... || ['Viewers']`) if they want one.
func evalInitialGroups(log logging.Logger, prog *jmespath.JMESPath, claims map[string]any) []string {
	if prog == nil {
		return nil
	}
	result, err := prog.Search(claims)
	if err != nil {
		log.WithError(err).Warn("initial_groups_path evaluation failed")
		return nil
	}
	switch v := result.(type) {
	case nil:
		return nil
	case string:
		if v == "" {
			return nil
		}
		return []string{v}
	case []any:
		out := make([]string, 0, len(v))
		for _, item := range v {
			s, ok := item.(string)
			if !ok {
				log.WithField("element_type", fmt.Sprintf("%T", item)).Warn("initial_groups_path returned non-string element; dropping all")
				return nil
			}
			out = append(out, s)
		}
		return out
	default:
		log.WithField("result_type", fmt.Sprintf("%T", v)).Warn("initial_groups_path returned unsupported type")
		return nil
	}
}

func enhanceFriendlyName(ctx context.Context, logger logging.Logger, svc auth.Service, user *model.User, friendlyName string, persist bool) *model.User {
	if user == nil || friendlyName == "" {
		return user
	}
	if swag.StringValue(user.FriendlyName) == friendlyName {
		return user
	}
	user.FriendlyName = swag.String(friendlyName)
	if persist {
		if err := svc.UpdateUserFriendlyName(ctx, user.Username, friendlyName); err != nil {
			logger.WithError(err).WithFields(logging.Fields{
				"user":          user.Username,
				"friendly_name": friendlyName,
			}).Warn("failed to persist friendly name")
		}
	}
	return user
}
