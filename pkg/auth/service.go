package auth

//go:generate go run github.com/treeverse/lakefs/tools/wrapgen --package auth --output ./service_wrapper.gen.go --interface Service ./service.go

// Must run goimports after wrapgen: it adds unused imports.
//go:generate go run golang.org/x/tools/cmd/goimports@latest -w ./service_wrapper.gen.go

import (
	"context"
	"strings"

	"github.com/treeverse/lakefs/pkg/auth/crypt"
	"github.com/treeverse/lakefs/pkg/auth/model"
	"github.com/treeverse/lakefs/pkg/permissions"
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

const (
	InvalidUserID = ""
	MaxPage       = 1000
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

func interpolateUser(resource string, username string) string {
	return strings.ReplaceAll(resource, "${user}", username)
}
