package migrate

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/hashicorp/go-multierror"

	"github.com/treeverse/lakefs/pkg/auth"
	"github.com/treeverse/lakefs/pkg/auth/acl"
	"github.com/treeverse/lakefs/pkg/auth/model"
	"github.com/treeverse/lakefs/pkg/auth/wildcard"
	"github.com/treeverse/lakefs/pkg/logging"
	"github.com/treeverse/lakefs/pkg/permissions"
)

const (
	maxGroups        = 1000
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
	ErrWidened         = errors.New("resource widened")

	allPermissions = []model.ACLPermission{"", acl.ACLRead, acl.ACLWrite, acl.ACLSuper, acl.ACLAdmin}
)

// RBACToACL translates all groups on svc to use ACLs instead of RBAC
// policies.  It updates svc only if doUpdate.  It calls warn to report
// increased permissions.
func RBACToACL(ctx context.Context, svc auth.Service, doUpdate bool, warnFunc func(error)) error {
	mig := NewACLsMigrator(svc, doUpdate)

	groups, _, err := svc.ListGroups(ctx, &model.PaginationParams{Amount: maxGroups + 1})
	if err != nil {
		return fmt.Errorf("list groups: %w", err)
	}
	if len(groups) > maxGroups {
		return fmt.Errorf("%w (got %d)", ErrTooManyGroups, len(groups))
	}
	for _, group := range groups {
		policies, _, err := svc.ListGroupPolicies(ctx, group.DisplayName, &model.PaginationParams{Amount: maxGroupPolicies + 1})
		if err != nil {
			return fmt.Errorf("list group %+v policies: %w", group, err)
		}
		if len(policies) > maxGroupPolicies {
			return fmt.Errorf("group %+v: %w (got %d)", group, ErrTooManyPolicies, len(policies))
		}
		acl, warn, err := mig.NewACLForPolicies(ctx, policies)
		if err != nil {
			return fmt.Errorf("create ACL for group %+v: %w", group, err)
		}
		if warn != nil {
			warnFunc(warn)
		}

		log := logging.FromContext(ctx)
		log.WithFields(logging.Fields{
			"group": group.DisplayName,
			"acl":   fmt.Sprintf("%+v", acl),
		}).Info("Computed ACL")
	}

	if doUpdate {
		panic("BUG(ariels): CREATE ACL & UPDATE GROUP!")
	}
	return nil
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
	repositories := make(map[string]struct{}, 0)
	allRepositories := false

	for _, p := range policies {
		if p.ACL.Permission != "" {
			return nil, warn, fmt.Errorf("policy %s: %w", p.DisplayName, ErrAlreadyHasACL)
		}

		for _, s := range p.Statement {
			if s.Effect != model.StatementEffectAllow {
				warn = multierror.Append(warn, fmt.Errorf("ignore policy %s statement %+v: %w", p.DisplayName, s, ErrNotAllowed))
			}
			sp, addedActions, err := mig.ComputePermission(ctx, s.Action)
			if err != nil {
				return nil, warn, fmt.Errorf("convert policy %s statement %+v: %w", p.DisplayName, s, err)
			}

			if len(addedActions) > 0 {
				w := fmt.Errorf("%w: %s", ErrAddedActions, strings.Join(addedActions, ", "))
				warn = multierror.Append(warn, w)
			}
			if BroaderPermission(sp, acl.Permission) {
				acl.Permission = sp
			}

			added, all, w := mig.GetRepositories(s.Resource)
			if w != nil {
				warn = multierror.Append(warn, w)
			}
			if all {
				allRepositories = true
			}
			if !allRepositories {
				for _, r := range added {
					repositories[r] = struct{}{}
				}
			}
		}
	}

	if allRepositories {
		acl.Repositories.All = true
	} else {
		rs := make([]string, len(repositories))
		for r := range repositories {
			rs = append(rs, r)
		}
		acl.Repositories.List = rs
	}
	return acl, warn, err
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
func (mig *ACLsMigrator) ComputePermission(ctx context.Context, actions []string) (model.ACLPermission, []string, error) {
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
		return permission, nil, fmt.Errorf("%w actions", ErrEmpty)
	}

	var (
		extraActions      []string
		allAllowedActions map[string]struct{}
	)
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
	for allowedAction := range allAllowedActions {
		// action _added_ if none of actions match it
		matched := false
		for _, a := range actions {
			if wildcard.Match(a, allowedAction) {
				matched = true
				break
			}
		}
		if !matched {
			extraActions = append(extraActions, allowedAction)
		}
	}
	return permission, extraActions, nil
}

// GetRepository returns the repositories to which resource refers, rounding
// up.
//
//   - It ignores all ARNs except "arn:lakefs:fs:::repository/.
//   - If an explicit repository is provided, it returns [repo], false, nil.
//   - Otherwise, if _any_ wildcards appears in the repository, it returns no
//     repo, true ("all"), and possibly a warning about widening resource.
func (mig *ACLsMigrator) GetRepositories(resource string) ([]string, bool, error) {
	const arnPrefix = "arn:lakefs:fs:::"

	suffix := resource

	if suffix == "*" {
		return nil, true, nil
	}

	if !strings.HasPrefix(suffix, arnPrefix) {
		return nil, false, nil
	}
	suffix = strings.TrimPrefix(suffix, arnPrefix)

	if !strings.HasPrefix(suffix, "repository/") {
		return nil, true, fmt.Errorf("%w: %s: no repository", ErrWidened, suffix)
	}
	suffix = strings.TrimPrefix(suffix, "repository/")

	repo := suffix
	index := strings.Index(suffix, "/")
	if index != -1 {
		repo = suffix[:index]
	}

	var widened []string

	if idx := strings.Index(suffix, "/objects/"); idx != -1 {
		widened = append(widened, fmt.Sprintf("path limitation \"%s\" dropped", suffix[idx:]))
	}

	var (
		repos []string
		all   bool
	)

	if !strings.ContainsAny(repo, "*?") {
		repos = []string{repo}
	} else {
		all = true

		if repo != "*" {
			widened = append(widened, "all repositories allowed")
		}
	}
	if widened == nil {
		return repos, all, nil
	}
	warn := fmt.Errorf("%w: %s: %s", ErrWidened, suffix, strings.Join(widened, " and "))
	return repos, all, warn
}

// BroaderPermission returns true if a offers strictly more permissions that b.
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
