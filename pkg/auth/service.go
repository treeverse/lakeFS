package auth

//go:generate go run github.com/treeverse/lakefs/tools/wrapgen --package auth --output ./service_wrapper.gen.go --interface Service ./service.go

// Must run goimports after wrapgen: it adds unused imports.
//go:generate go run golang.org/x/tools/cmd/goimports@latest -w ./service_wrapper.gen.go

import (
	"context"
	"fmt"
	"strings"

	"github.com/treeverse/lakefs/pkg/auth/crypt"
	"github.com/treeverse/lakefs/pkg/auth/model"
	"github.com/treeverse/lakefs/pkg/auth/wildcard"
	"github.com/treeverse/lakefs/pkg/logging"
	"github.com/treeverse/lakefs/pkg/permissions"
)

const (
	contextKeyConditionContext contextKey = "condition_context"
)

type AuthorizationRequest struct {
	Username            string
	RequiredPermissions permissions.Node
	ClientIP            string // IP address of the client making the request
}

type AuthorizationResponse struct {
	Allowed bool
	Error   error
}

type MissingPermissions struct {
	// Denied is a list of actions the user was denied for the attempt.
	Denied []string
	// Unauthorized is a list of actions the user did not have for the attempt.
	Unauthorized []string
}

// CheckResult - the final result for the authorization is accepted only if it's CheckAllow
type CheckResult int

const (
	UserNotAllowed = "not allowed"
	InvalidUserID  = ""
	MaxPage        = 1000
	// CheckAllow Permission allowed
	CheckAllow CheckResult = iota
	// CheckNeutral Permission neither allowed nor denied
	CheckNeutral
	// CheckDeny Permission denied
	CheckDeny
)

type GatewayService interface {
	GetCredentials(_ context.Context, accessKey string) (*model.Credential, error)
	GetUser(ctx context.Context, username string) (*model.User, error)
	Authorize(_ context.Context, req *AuthorizationRequest) (*AuthorizationResponse, error)
	ListEffectivePolicies(ctx context.Context, username string, params *model.PaginationParams) ([]*model.Policy, *model.Paginator, error)
}

type Authorizer interface {
	// Authorize checks 'req' containing user and required permissions. An error returns in case we fail perform the request.
	// AuthorizationResponse holds if the request allowed and Error in case we fail with additional reason as ErrInsufficientPermissions.
	Authorize(ctx context.Context, req *AuthorizationRequest) (*AuthorizationResponse, error)
}

type CredentialsCreator interface {
	CreateCredentials(ctx context.Context, username string) (*model.Credential, error)
}

type Service interface {
	SecretStore() crypt.SecretStore
	Cache() Cache

	// users
	CreateUser(ctx context.Context, user *model.User) (string, error)
	DeleteUser(ctx context.Context, username string) error
	GetUserByID(ctx context.Context, userID string) (*model.User, error)
	GetUser(ctx context.Context, username string) (*model.User, error)
	GetUserByEmail(ctx context.Context, email string) (*model.User, error)
	ListUsers(ctx context.Context, params *model.PaginationParams) ([]*model.User, *model.Paginator, error)

	// groups
	CreateGroup(ctx context.Context, group *model.Group) (*model.Group, error)
	DeleteGroup(ctx context.Context, groupID string) error
	GetGroup(ctx context.Context, groupID string) (*model.Group, error)
	ListGroups(ctx context.Context, params *model.PaginationParams) ([]*model.Group, *model.Paginator, error)

	// group<->user memberships
	AddUserToGroup(ctx context.Context, username, groupID string) error
	RemoveUserFromGroup(ctx context.Context, username, groupID string) error
	ListUserGroups(ctx context.Context, username string, params *model.PaginationParams) ([]*model.Group, *model.Paginator, error)
	ListGroupUsers(ctx context.Context, groupID string, params *model.PaginationParams) ([]*model.User, *model.Paginator, error)

	// policies
	WritePolicy(ctx context.Context, policy *model.Policy, update bool) error
	GetPolicy(ctx context.Context, policyDisplayName string) (*model.Policy, error)
	DeletePolicy(ctx context.Context, policyDisplayName string) error
	ListPolicies(ctx context.Context, params *model.PaginationParams) ([]*model.Policy, *model.Paginator, error)

	// credentials
	CredentialsCreator
	AddCredentials(ctx context.Context, username, accessKeyID, secretAccessKey string) (*model.Credential, error)
	DeleteCredentials(ctx context.Context, username, accessKeyID string) error
	GetCredentialsForUser(ctx context.Context, username, accessKeyID string) (*model.Credential, error)
	GetCredentials(ctx context.Context, accessKeyID string) (*model.Credential, error)
	ListUserCredentials(ctx context.Context, username string, params *model.PaginationParams) ([]*model.Credential, *model.Paginator, error)

	// policy<->user attachments
	AttachPolicyToUser(ctx context.Context, policyDisplayName, username string) error
	DetachPolicyFromUser(ctx context.Context, policyDisplayName, username string) error
	ListUserPolicies(ctx context.Context, username string, params *model.PaginationParams) ([]*model.Policy, *model.Paginator, error)
	ListEffectivePolicies(ctx context.Context, username string, params *model.PaginationParams) ([]*model.Policy, *model.Paginator, error)

	// policy<->group attachments
	AttachPolicyToGroup(ctx context.Context, policyDisplayName, groupID string) error
	DetachPolicyFromGroup(ctx context.Context, policyDisplayName, groupID string) error
	ListGroupPolicies(ctx context.Context, groupID string, params *model.PaginationParams) ([]*model.Policy, *model.Paginator, error)

	Authorizer

	ClaimTokenIDOnce(ctx context.Context, tokenID string, expiresAt int64) error
}

func (n *MissingPermissions) String() string {
	if len(n.Denied) != 0 {
		return fmt.Sprintf("denied permission to %s", strings.Join(n.Denied, ","))
	}
	if len(n.Unauthorized) != 0 {
		return fmt.Sprintf("not allowed to %s", strings.Join(n.Unauthorized, ","))
	}
	return UserNotAllowed
}

func CheckPermissions(ctx context.Context, node permissions.Node, username string, policies []*model.Policy, permAudit *MissingPermissions) CheckResult {
	// Create condition context for condition evaluation
	conditionCtx := &ConditionContext{
		Fields: make(map[string]string),
	}
	log := logging.FromContext(ctx)
	if reqContext, ok := ctx.Value(contextKeyConditionContext).(*ConditionContext); ok {
		conditionCtx = reqContext
		if len(conditionCtx.Fields) > 0 {
			log = log.WithField("fields", conditionCtx.Fields)
		}
	}

	allowed := CheckNeutral
	switch node.Type {
	case permissions.NodeTypeNode:
		hasPermission := false
		// check whether the permission is allowed, denied or natural (not allowed and not denied)
		for _, policy := range policies {
			for _, stmt := range policy.Statement {
				// Evaluate conditions first
				if len(stmt.Condition) > 0 {
					conditionPassed, err := EvaluateConditions(stmt.Condition, conditionCtx)
					if err != nil {
						log.WithError(err).Warn("Failed to evaluate conditions")
						return CheckDeny
					}
					if !conditionPassed {
						// Conditions didn't pass, skip this statement
						continue
					}
				}

				resources, err := ParsePolicyResourceAsList(stmt.Resource)
				if err != nil {
					log.Error(err)
					return CheckDeny
				}
				for _, resource := range resources {
					resource = interpolateUser(resource, username)
					if !ArnMatch(resource, node.Permission.Resource) {
						continue
					}

					for _, action := range stmt.Action {
						if !wildcard.Match(action, node.Permission.Action) {
							continue // not a matching action
						}

						if stmt.Effect == model.StatementEffectDeny {
							// this is a "Deny" and it takes precedence
							permAudit.Denied = append(permAudit.Denied, action)
							return CheckDeny
						}
						hasPermission = true
						allowed = CheckAllow
					}
				}
			}
		}
		if !hasPermission {
			permAudit.Unauthorized = append(permAudit.Unauthorized, node.Permission.Action)
		}

	case permissions.NodeTypeOr:
		// returns:
		// Allowed - at least one of the permissions is allowed and no one is denied.
		// Denied - one of the permissions is Deny.
		// Natural - otherwise.
		for _, node := range node.Nodes {
			result := CheckPermissions(ctx, node, username, policies, permAudit)
			if result == CheckDeny {
				return CheckDeny
			}
			if allowed != CheckAllow {
				allowed = result
			}
		}

	case permissions.NodeTypeAnd:
		// returns:
		// Allowed - all the permissions are allowed
		// Denied - one of the permissions is Deny
		// Natural - otherwise
		for _, node := range node.Nodes {
			result := CheckPermissions(ctx, node, username, policies, permAudit)
			if result == CheckNeutral || result == CheckDeny {
				return result
			}
		}
		return CheckAllow

	default:
		log.Error("unknown permission node type")
		return CheckDeny
	}
	return allowed
}

func interpolateUser(resource string, username string) string {
	return strings.ReplaceAll(resource, "${user}", username)
}
