package migrations

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/hashicorp/go-multierror"
	"github.com/treeverse/lakefs/pkg/auth"
	"github.com/treeverse/lakefs/pkg/auth/acl"
	"github.com/treeverse/lakefs/pkg/auth/crypt"
	"github.com/treeverse/lakefs/pkg/auth/model"
	authparams "github.com/treeverse/lakefs/pkg/auth/params"
	"github.com/treeverse/lakefs/pkg/auth/wildcard"
	"github.com/treeverse/lakefs/pkg/config"
	"github.com/treeverse/lakefs/pkg/kv"
	"github.com/treeverse/lakefs/pkg/logging"
	"github.com/treeverse/lakefs/pkg/permissions"
)

const (
	maxGroups        = 1000
	pageSize         = 1000
	maxGroupPolicies = 1000
)

var (
	// ErrTooMany is returned when this migration does not support a
	// particular number of resources.  It should not occur on any
	// reasonably-sized installation.
	ErrTooMany         = errors.New("too many")
	ErrTooManyPolicies = fmt.Errorf("%w policies", ErrTooMany)
	ErrTooManyGroups   = fmt.Errorf("%w groups", ErrTooMany)
	ErrNotAllowed      = fmt.Errorf("not allowed")
	ErrAlreadyHasACL   = errors.New("already has ACL")
	ErrAddedActions    = errors.New("added actions")
	ErrEmpty           = errors.New("empty")
	ErrPolicyExists    = errors.New("policy exists")
	ErrHasWarnings     = errors.New("has warnings")

	// allPermissions lists all permissions, from most restrictive to
	// most permissive.  It includes "" for some edge cases.
	allPermissions = []model.ACLPermission{"", acl.ACLRead, acl.ACLWrite, acl.ACLSuper, acl.ACLAdmin}
)

func MigrateToACL(ctx context.Context, kvStore kv.Store, cfg *config.Config, logger logging.Logger, version int, force bool) error {
	if cfg.IsAuthTypeAPI() {
		fmt.Println("skipping ACL migration - external Authorization")
		return updateKVSchemaVersion(ctx, kvStore, kv.ACLNoReposMigrateVersion)
	}

	// handle migrate within ACLs
	if version == kv.ACLMigrateVersion {
		if force {
			return updateKVSchemaVersion(ctx, kvStore, kv.ACLNoReposMigrateVersion)
		} else {
			return fmt.Errorf("migrating from previous version of ACL will leave repository level groups until the group is re-edited - please run migrate again with --force flag or contact services@treeverse.io: %w", kv.ErrMigrationVersion)
		}
	}

	type Warning struct {
		GroupID string
		ACL     model.ACL
		Warn    error
	}
	var (
		groupReports      []Warning
		usersWithPolicies []string
	)
	updateTime := time.Now()
	authService := auth.NewAuthService(
		kvStore,
		crypt.NewSecretStore(cfg.AuthEncryptionSecret()),
		nil,
		authparams.ServiceCache(cfg.Auth.Cache),
		logger.WithField("service", "auth_service"),
	)
	usersWithPolicies, err := rbacToACL(ctx, authService, false, updateTime, func(groupID string, acl model.ACL, warn error) {
		groupReports = append(groupReports, Warning{GroupID: groupID, ACL: acl, Warn: warn})
	})
	if err != nil {
		return fmt.Errorf("failed to upgrade RBAC policies to ACLs: %w", err)
	}

	hasWarnings := false
	for _, w := range groupReports {
		fmt.Printf("GROUP %s\n\tACL: %s\n", w.GroupID, reportACL(w.ACL))
		if w.Warn != nil {
			hasWarnings = true
			var multi *multierror.Error
			if errors.As(w.Warn, &multi) {
				multi.ErrorFormat = func(es []error) string {
					points := make([]string, len(es))
					for i, err := range es {
						points[i] = fmt.Sprintf("* %s", err)
					}
					plural := "s"
					if len(es) == 1 {
						plural = ""
					}
					return fmt.Sprintf(
						"%d change%s:\n\t%s\n",
						len(points), plural, strings.Join(points, "\n\t"))
				}
			}
			fmt.Println(w.Warn)
		}
		fmt.Println()
	}
	for _, username := range usersWithPolicies {
		fmt.Printf("USER (%s)  detaching directly-attached policies\n", username)
	}

	if hasWarnings && !force {
		return fmt.Errorf("warnings found. Please fix or run using --force flag: %w", ErrHasWarnings)
	}

	_, err = rbacToACL(ctx, authService, true, updateTime, func(groupID string, acl model.ACL, warn error) {
		groupReports = append(groupReports, Warning{GroupID: groupID, ACL: acl, Warn: warn})
	})
	if err != nil {
		return fmt.Errorf("failed to upgrade RBAC policies to ACLs: %w", err)
	}

	return updateKVSchemaVersion(ctx, kvStore, kv.ACLNoReposMigrateVersion)
}

func reportACL(acl model.ACL) string {
	return string(acl.Permission) + " on [ALL repositories]"
}

// checkPolicyACLName fails if policy name is named as an ACL policy (start
// with ACLPolicyPrefix) but is not an ACL policy.
func checkPolicyACLName(ctx context.Context, svc auth.Service, name string) error {
	if !acl.IsACLPolicyName(name) {
		return nil
	}

	_, err := svc.GetGroup(ctx, name)
	switch {
	case errors.Is(err, auth.ErrNotFound):
		return nil
	case err == nil:
		return fmt.Errorf("%s: %w", name, ErrPolicyExists)
	default:
		return fmt.Errorf("check policy name %s: %w", name, err)
	}
}

// rbacToACL translates all groups on svc to use ACLs instead of RBAC
// policies.  It updates svc only if doUpdate.  It calls messageFunc to
// report increased permissions.
// returns a list of users with directly attached policies
func rbacToACL(ctx context.Context, svc auth.Service, doUpdate bool, creationTime time.Time, messageFunc func(string, model.ACL, error)) ([]string, error) {
	mig := NewACLsMigrator(svc, doUpdate)

	groups, _, err := svc.ListGroups(ctx, &model.PaginationParams{Amount: maxGroups + 1})
	if err != nil {
		return nil, fmt.Errorf("list groups: %w", err)
	}
	if len(groups) > maxGroups {
		return nil, fmt.Errorf("%w (got %d)", ErrTooManyGroups, len(groups))
	}
	for _, group := range groups {
		var warnings error

		policies, _, err := svc.ListGroupPolicies(ctx, group.DisplayName, &model.PaginationParams{Amount: maxGroupPolicies + 1})
		if err != nil {
			return nil, fmt.Errorf("list group %+v policies: %w", group, err)
		}
		if len(policies) > maxGroupPolicies {
			return nil, fmt.Errorf("group %+v: %w (got %d)", group, ErrTooManyPolicies, len(policies))
		}
		newACL, warn, err := mig.NewACLForPolicies(ctx, policies)
		if err != nil {
			return nil, fmt.Errorf("create ACL for group %+v: %w", group, err)
		}
		if warn != nil {
			warnings = multierror.Append(warnings, warn)
		}

		log := logging.FromContext(ctx)
		log.WithFields(logging.Fields{
			"group": group.DisplayName,
			"acl":   fmt.Sprintf("%+v", newACL),
		}).Info("Computed ACL")

		aclPolicyName := acl.ACLPolicyName(group.DisplayName)
		err = checkPolicyACLName(ctx, svc, aclPolicyName)
		if err != nil {
			warnings = multierror.Append(warnings, warn)
		}
		policyExists := errors.Is(err, ErrPolicyExists)
		if doUpdate {
			err = acl.WriteGroupACL(ctx, svc, group.DisplayName, *newACL, creationTime, policyExists)
			if errors.Is(err, auth.ErrAlreadyExists) {
				warnings = multierror.Append(warnings, err)
			} else if err != nil {
				return nil, err
			}
		}

		messageFunc(group.DisplayName, *newACL, warnings)
	}
	var usersWithPolicies []string
	hasMoreUser := true
	afterUser := ""
	for hasMoreUser {
		// get membership groups to user
		users, userPaginator, err := svc.ListUsers(ctx, &model.PaginationParams{
			After:  afterUser,
			Amount: pageSize,
		})
		if err != nil {
			return nil, err
		}
		if len(users) == 0 { // avoid infinite loop when no users
			break
		}
		for _, user := range users {
			// get policies attracted to user
			hasMoreUserPolicy := true
			afterUserPolicy := ""
			for hasMoreUserPolicy {
				userPolicies, userPoliciesPaginator, err := svc.ListUserPolicies(ctx, user.Username, &model.PaginationParams{
					After:  afterUserPolicy,
					Amount: pageSize,
				})
				if err != nil {
					return nil, fmt.Errorf("list user policies: %w", err)
				}
				if len(userPolicies) > 0 {
					usersWithPolicies = append(usersWithPolicies, user.Username)
				}
				if !doUpdate {
					break
				}
				for _, policy := range userPolicies {
					if err := svc.DetachPolicyFromUser(ctx, policy.DisplayName, user.Username); err != nil {
						return nil, fmt.Errorf("failed detaching policy %s from user %s: %w", policy.DisplayName, user.Username, err)
					}
				}
				afterUserPolicy = userPoliciesPaginator.NextPageToken
				hasMoreUserPolicy = userPoliciesPaginator.NextPageToken != ""
			}
			afterUser = userPaginator.NextPageToken
			hasMoreUser = userPaginator.NextPageToken != ""
		}
	}
	return usersWithPolicies, nil
}

// ACLsMigrator migrates from policies to ACLs.
type ACLsMigrator struct {
	svc      auth.Service
	doUpdate bool

	Actions map[model.ACLPermission]map[string]struct{}
}

func makeSet(allEls ...[]string) map[string]struct{} {
	ret := make(map[string]struct{}, 0)
	for _, els := range allEls {
		for _, el := range els {
			ret[el] = struct{}{}
		}
	}
	return ret
}

// NewACLsMigrator returns an ACLsMigrator.  That ACLsMigrator will only
// check (change nothing) if doUpdate is false.
func NewACLsMigrator(svc auth.Service, doUpdate bool) *ACLsMigrator {
	manageOwnCredentials := auth.GetActionsForPolicyTypeOrDie("AuthManageOwnCredentials")
	ciRead := auth.GetActionsForPolicyTypeOrDie("RepoManagementRead")
	return &ACLsMigrator{
		svc:      svc,
		doUpdate: doUpdate,
		Actions: map[model.ACLPermission]map[string]struct{}{
			acl.ACLAdmin: makeSet(auth.GetActionsForPolicyTypeOrDie("AllAccess")),
			acl.ACLSuper: makeSet(auth.GetActionsForPolicyTypeOrDie("FSFullAccess"), manageOwnCredentials, ciRead),
			acl.ACLWrite: makeSet(auth.GetActionsForPolicyTypeOrDie("FSReadWrite"), manageOwnCredentials, ciRead),
			acl.ACLRead:  makeSet(auth.GetActionsForPolicyTypeOrDie("FSRead"), manageOwnCredentials),
		},
	}
}

// NewACLForPolicies converts policies of group name to an ACL.  warn
// summarizes all losses in converting policies to ACL.  err holds an error
// if conversion failed.
func (mig *ACLsMigrator) NewACLForPolicies(ctx context.Context, policies []*model.Policy) (acl *model.ACL, warn error, err error) {
	warn = nil
	acl = new(model.ACL)

	allAllowedActions := make(map[string]struct{})
	for _, p := range policies {
		if p.ACL.Permission != "" {
			warn = multierror.Append(warn, fmt.Errorf("policy %s: %w", p.DisplayName, ErrAlreadyHasACL))
		}

		for _, s := range p.Statement {
			if s.Effect != model.StatementEffectAllow {
				warn = multierror.Append(warn, fmt.Errorf("ignore policy %s statement %+v: %w", p.DisplayName, s, ErrNotAllowed))
			}
			sp, err := mig.ComputePermission(ctx, s.Action)
			if err != nil {
				return nil, warn, fmt.Errorf("convert policy %s statement %+v: %w", p.DisplayName, s, err)
			}
			for _, allowedAction := range expandMatchingActions(s.Action) {
				allAllowedActions[allowedAction] = struct{}{}
			}

			if BroaderPermission(sp, acl.Permission) {
				acl.Permission = sp
			}
		}
	}
	addedActions := mig.ComputeAddedActions(acl.Permission, allAllowedActions)
	if len(addedActions) > 0 {
		warn = multierror.Append(warn, fmt.Errorf("%w: %s", ErrAddedActions, strings.Join(addedActions, ", ")))
	}
	return acl, warn, err
}

func expandMatchingActions(patterns []string) []string {
	ret := make([]string, 0, len(patterns))
	for _, action := range permissions.Actions {
		for _, pattern := range patterns {
			if wildcard.Match(pattern, action) {
				ret = append(ret, action)
			}
		}
	}
	return ret
}

func someActionMatches(action string, availableActions map[string]struct{}) bool {
	for availableAction := range availableActions {
		if wildcard.Match(availableAction, action) {
			return true
		}
	}
	return false
}

func (mig *ACLsMigrator) GetMinPermission(action string) model.ACLPermission {
	if !strings.ContainsAny(action, "*?") {
		for _, permission := range allPermissions {
			if someActionMatches(action, mig.Actions[permission]) {
				return permission
			}
		}
		return ""
	}

	// Try a wildcard match against all known actions: find the least
	// permissions that allows all actions that the action pattern
	// matches.
	for _, permission := range allPermissions {
		// This loop is reasonably efficient only for small numbers
		// of ACL permissions.
		actionsForPermission := mig.Actions[permission]
		permissionOK := true
		for _, a := range permissions.Actions {
			if !wildcard.Match(action, a) {
				// a does not include action.
				continue
			}
			if someActionMatches(a, actionsForPermission) {
				// a is allowed at permission.
				continue
			}
			permissionOK = false
			break
		}
		if permissionOK {
			return permission
		}
	}
	panic(fmt.Sprintf("Unknown action %s", action))
}

// ComputePermission returns ACL permission for actions and the actions that
// applying that permission will add to it.
func (mig *ACLsMigrator) ComputePermission(ctx context.Context, actions []string) (model.ACLPermission, error) {
	log := logging.FromContext(ctx)
	permission := model.ACLPermission("")
	for _, action := range actions {
		p := mig.GetMinPermission(action)
		if BroaderPermission(p, permission) {
			log.WithFields(logging.Fields{
				"action":          action,
				"permission":      p,
				"prev_permission": permission,
			}).Debug("Increase permission")
			permission = p
		} else {
			log.WithFields(logging.Fields{
				"action":     action,
				"permission": p,
			}).Trace("Permission")
		}
	}
	if permission == model.ACLPermission("") {
		return permission, fmt.Errorf("%w actions", ErrEmpty)
	}

	return permission, nil
}

// ComputeAddedActions returns the list of actions that permission allows
// that are not in alreadyAllowedActions.
func (mig *ACLsMigrator) ComputeAddedActions(permission model.ACLPermission, alreadyAllowedActions map[string]struct{}) []string {
	var allAllowedActions map[string]struct{}
	switch permission {
	case acl.ACLRead:
		allAllowedActions = mig.Actions[acl.ACLRead]
	case acl.ACLWrite:
		allAllowedActions = mig.Actions[acl.ACLWrite]
	case acl.ACLSuper:
		allAllowedActions = mig.Actions[acl.ACLSuper]
	case acl.ACLAdmin:
	default:
		allAllowedActions = mig.Actions[acl.ACLAdmin]
	}
	addedActions := make(map[string]struct{}, len(allAllowedActions))
	for _, action := range permissions.Actions {
		if someActionMatches(action, allAllowedActions) && !someActionMatches(action, alreadyAllowedActions) {
			addedActions[action] = struct{}{}
		}
	}
	addedActionsSlice := make([]string, 0, len(addedActions))
	for action := range addedActions {
		addedActionsSlice = append(addedActionsSlice, action)
	}
	return addedActionsSlice
}

// BroaderPermission returns true if a offers strictly more permissions than b. Unknown ACLPermission will panic.
func BroaderPermission(a, b model.ACLPermission) bool {
	switch a {
	case "":
		return false
	case acl.ACLRead:
		return b == ""
	case acl.ACLWrite:
		return b == "" || b == acl.ACLRead
	case acl.ACLSuper:
		return b == "" || b == acl.ACLRead || b == acl.ACLWrite
	case acl.ACLAdmin:
		return b == "" || b == acl.ACLRead || b == acl.ACLWrite || b == acl.ACLSuper
	}
	panic(fmt.Sprintf("impossible comparison %s and %s", a, b))
}
