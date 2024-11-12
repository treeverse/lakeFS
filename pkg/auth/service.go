package auth

//go:generate go run github.com/treeverse/lakefs/tools/wrapgen --package auth --output ./service_inviter_wrapper.gen.go --interface ServiceAndInviter ./service.go
//go:generate go run github.com/treeverse/lakefs/tools/wrapgen --package auth --output ./service_wrapper.gen.go --interface Service ./service.go

// Must run goimports after wrapgen: it adds unused imports.
//go:generate go run golang.org/x/tools/cmd/goimports@latest -w ./service_inviter_wrapper.gen.go
//go:generate go run golang.org/x/tools/cmd/goimports@latest -w ./service_wrapper.gen.go

//go:generate go run github.com/deepmap/oapi-codegen/cmd/oapi-codegen@v1.5.6 -package auth -generate "types,client" -o client.gen.go ../../api/authorization.yml
//go:generate go run github.com/golang/mock/mockgen@v1.6.0 -package=mock -destination=mock/mock_auth_client.go github.com/treeverse/lakefs/pkg/auth ClientWithResponsesInterface

import (
	"context"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/cenkalti/backoff/v4"
	"github.com/deepmap/oapi-codegen/pkg/securityprovider"
	"github.com/getkin/kin-openapi/openapi3filter"
	"github.com/go-openapi/swag"
	"github.com/golang-jwt/jwt/v4"
	"github.com/rs/xid"
	"github.com/treeverse/lakefs/pkg/auth/crypt"
	"github.com/treeverse/lakefs/pkg/auth/model"
	"github.com/treeverse/lakefs/pkg/auth/params"
	"github.com/treeverse/lakefs/pkg/auth/wildcard"
	"github.com/treeverse/lakefs/pkg/httputil"
	"github.com/treeverse/lakefs/pkg/logging"
	"github.com/treeverse/lakefs/pkg/permissions"
)

type AuthorizationRequest struct {
	Username            string
	RequiredPermissions permissions.Node
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
}

type Authorizer interface {
	// Authorize checks 'req' containing user and required permissions. An error returns in case we fail perform the request.
	// AuthorizationResponse holds if the request allowed and Error in case we fail with additional reason as ErrInsufficientPermissions.
	Authorize(ctx context.Context, req *AuthorizationRequest) (*AuthorizationResponse, error)
}

type CredentialsCreator interface {
	CreateCredentials(ctx context.Context, username string) (*model.Credential, error)
}

type EmailInviter interface {
	InviteUser(ctx context.Context, email string) error
}

// ExternalPrincipalsService is an interface for managing external principals (e.g. IAM users, groups, etc.)
// It's part of the AuthService api's and is used as an administrative API to that service.
type ExternalPrincipalsService interface {
	IsExternalPrincipalsEnabled(ctx context.Context) bool
	CreateUserExternalPrincipal(ctx context.Context, userID, principalID string) error
	DeleteUserExternalPrincipal(ctx context.Context, userID, principalID string) error
	GetExternalPrincipal(ctx context.Context, principalID string) (*model.ExternalPrincipal, error)
	ListUserExternalPrincipals(ctx context.Context, userID string, params *model.PaginationParams) ([]*model.ExternalPrincipal, *model.Paginator, error)
}

type ServiceAndInviter interface {
	Service
	EmailInviter
}

type Service interface {
	SecretStore() crypt.SecretStore
	Cache() Cache

	// users
	CreateUser(ctx context.Context, user *model.User) (string, error)
	DeleteUser(ctx context.Context, username string) error
	GetUserByID(ctx context.Context, userID string) (*model.User, error)
	GetUser(ctx context.Context, username string) (*model.User, error)
	GetUserByExternalID(ctx context.Context, externalID string) (*model.User, error)
	GetUserByEmail(ctx context.Context, email string) (*model.User, error)
	ListUsers(ctx context.Context, params *model.PaginationParams) ([]*model.User, *model.Paginator, error)
	UpdateUserFriendlyName(ctx context.Context, userID string, friendlyName string) error

	ExternalPrincipalsService

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

const (
	healthCheckMaxInterval     = 5 * time.Second
	healthCheckInitialInterval = 1 * time.Second
)

type APIAuthService struct {
	apiClient                 ClientWithResponsesInterface
	secretStore               crypt.SecretStore
	logger                    logging.Logger
	cache                     Cache
	externalPrincipalsEnabled bool
}

func (a *APIAuthService) InviteUser(ctx context.Context, email string) error {
	ctx = httputil.SetClientTrace(ctx, "api_auth")
	resp, err := a.apiClient.CreateUserWithResponse(ctx, CreateUserJSONRequestBody{
		Email:    swag.String(email),
		Invite:   swag.Bool(true),
		Username: email,
	})
	if err != nil {
		a.logger.WithError(err).Error("failed to create user")
		return err
	}
	return a.validateResponse(resp, http.StatusCreated)
}

func (a *APIAuthService) SecretStore() crypt.SecretStore {
	return a.secretStore
}

func (a *APIAuthService) Cache() Cache {
	return a.cache
}

func (a *APIAuthService) CreateUser(ctx context.Context, user *model.User) (string, error) {
	ctx = httputil.SetClientTrace(ctx, "api_auth")
	resp, err := a.apiClient.CreateUserWithResponse(ctx, CreateUserJSONRequestBody{
		Email:        user.Email,
		FriendlyName: user.FriendlyName,
		Source:       &user.Source,
		Username:     user.Username,
		ExternalId:   user.ExternalID,
	})
	if err != nil {
		a.logger.WithError(err).WithField("username", user.Username).Error("failed to create user")
		return InvalidUserID, err
	}
	if err := a.validateResponse(resp, http.StatusCreated); err != nil {
		return InvalidUserID, err
	}

	return resp.JSON201.Username, nil
}

func (a *APIAuthService) DeleteUser(ctx context.Context, username string) error {
	ctx = httputil.SetClientTrace(ctx, "api_auth")
	resp, err := a.apiClient.DeleteUserWithResponse(ctx, username)
	if err != nil {
		a.logger.WithError(err).WithField("username", username).Error("failed to delete user")
		return err
	}
	return a.validateResponse(resp, http.StatusNoContent)
}

func userIDToInt(userID string) (int64, error) {
	const base, bitSize = 10, 64
	return strconv.ParseInt(userID, base, bitSize)
}

func (a *APIAuthService) getFirstUser(ctx context.Context, userKey UserKey, params *ListUsersParams) (*model.User, error) {
	return a.cache.GetUser(userKey, func() (*model.User, error) {
		// fetch at least two users to make sure we don't have duplicates
		if params.Amount == nil {
			const amount = 2
			params.Amount = paginationAmount(amount)
		}
		resp, err := a.apiClient.ListUsersWithResponse(ctx, params)
		if err != nil {
			a.logger.WithError(err).Error("failed to list users")
			return nil, err
		}
		if err := a.validateResponse(resp, http.StatusOK); err != nil {
			return nil, err
		}
		if resp.JSON200 == nil {
			return nil, ErrInvalidResponse
		}
		results := resp.JSON200.Results
		if len(results) == 0 {
			return nil, ErrNotFound
		}
		if len(results) > 1 {
			// make sure we work with just one user based on 'params'
			return nil, ErrNonUnique
		}
		u := results[0]
		return &model.User{
			CreatedAt:         time.Unix(u.CreationDate, 0),
			Username:          u.Username,
			FriendlyName:      u.FriendlyName,
			Email:             u.Email,
			EncryptedPassword: u.EncryptedPassword,
			Source:            swag.StringValue(u.Source),
		}, nil
	})
}

func (a *APIAuthService) GetUserByID(ctx context.Context, userID string) (*model.User, error) {
	ctx = httputil.SetClientTrace(ctx, "api_auth")
	intID, err := userIDToInt(userID)
	if err != nil {
		return nil, fmt.Errorf("userID as int64: %w", err)
	}
	return a.getFirstUser(ctx, UserKey{id: userID}, &ListUsersParams{Id: &intID})
}

func (a *APIAuthService) GetUser(ctx context.Context, username string) (*model.User, error) {
	ctx = httputil.SetClientTrace(ctx, "api_auth")
	return a.cache.GetUser(UserKey{Username: username}, func() (*model.User, error) {
		resp, err := a.apiClient.GetUserWithResponse(ctx, username)
		if err != nil {
			a.logger.WithError(err).WithField("username", username).Error("failed to get user")
			return nil, err
		}
		if err := a.validateResponse(resp, http.StatusOK); err != nil {
			return nil, err
		}
		u := resp.JSON200
		return &model.User{
			CreatedAt:         time.Unix(u.CreationDate, 0),
			Username:          u.Username,
			FriendlyName:      u.FriendlyName,
			Email:             u.Email,
			EncryptedPassword: u.EncryptedPassword,
			Source:            swag.StringValue(u.Source),
		}, nil
	})
}

func (a *APIAuthService) GetUserByEmail(ctx context.Context, email string) (*model.User, error) {
	ctx = httputil.SetClientTrace(ctx, "api_auth")
	return a.getFirstUser(ctx, UserKey{Email: email}, &ListUsersParams{Email: swag.String(email)})
}

func (a *APIAuthService) GetUserByExternalID(ctx context.Context, externalID string) (*model.User, error) {
	ctx = httputil.SetClientTrace(ctx, "api_auth")
	return a.getFirstUser(ctx, UserKey{ExternalID: externalID}, &ListUsersParams{ExternalId: swag.String(externalID)})
}

func toPagination(paginator Pagination) *model.Paginator {
	return &model.Paginator{
		Amount:        paginator.Results,
		NextPageToken: paginator.NextOffset,
	}
}

func (a *APIAuthService) ListUsers(ctx context.Context, params *model.PaginationParams) ([]*model.User, *model.Paginator, error) {
	ctx = httputil.SetClientTrace(ctx, "api_auth")
	paginationPrefix := PaginationPrefix(params.Prefix)
	paginationAfter := PaginationAfter(params.After)
	paginationAmount := PaginationAmount(params.Amount)
	resp, err := a.apiClient.ListUsersWithResponse(ctx, &ListUsersParams{
		Prefix: &paginationPrefix,
		After:  &paginationAfter,
		Amount: &paginationAmount,
	})
	if err != nil {
		a.logger.WithError(err).Error("failed to list users")
		return nil, nil, err
	}
	if err := a.validateResponse(resp, http.StatusOK); err != nil {
		return nil, nil, err
	}
	pagination := resp.JSON200.Pagination
	results := resp.JSON200.Results
	users := make([]*model.User, len(results))
	for i, r := range results {
		users[i] = &model.User{
			CreatedAt:         time.Unix(r.CreationDate, 0),
			Username:          r.Username,
			FriendlyName:      r.FriendlyName,
			Email:             r.Email,
			EncryptedPassword: nil,
			Source:            swag.StringValue(r.Source),
		}
	}
	return users, toPagination(pagination), nil
}

func (a *APIAuthService) UpdateUserFriendlyName(ctx context.Context, userID string, friendlyName string) error {
	ctx = httputil.SetClientTrace(ctx, "api_auth")
	resp, err := a.apiClient.UpdateUserFriendlyNameWithResponse(ctx, userID, UpdateUserFriendlyNameJSONRequestBody{
		FriendlyName: friendlyName,
	})
	if err != nil {
		a.logger.WithError(err).WithField("userID", userID).Error("update user friendly name")
		return err
	}
	return a.validateResponse(resp, http.StatusNoContent)
}

func (a *APIAuthService) CreateGroup(ctx context.Context, group *model.Group) (*model.Group, error) {
	ctx = httputil.SetClientTrace(ctx, "api_auth")
	resp, err := a.apiClient.CreateGroupWithResponse(ctx, CreateGroupJSONRequestBody{
		Id: group.DisplayName,
	})
	if err != nil {
		a.logger.WithError(err).WithField("group", group).Error("failed to create group")
		return nil, err
	}

	if err = a.validateResponse(resp, http.StatusCreated); err != nil {
		return nil, err
	}
	return &model.Group{
		CreatedAt:   time.Unix(resp.JSON201.CreationDate, 0),
		DisplayName: resp.JSON201.Name,
		ID:          groupIDOrDisplayName(*resp.JSON201),
	}, nil
}

// validateResponse returns ErrUnexpectedStatusCode if the response status code is not as expected
func (a *APIAuthService) validateResponse(resp openapi3filter.StatusCoder, expectedStatusCode int) error {
	statusCode := resp.StatusCode()
	if statusCode == expectedStatusCode {
		return nil
	}
	switch statusCode {
	case http.StatusNotFound:
		return ErrNotFound
	case http.StatusBadRequest:
		return ErrInvalidRequest
	case http.StatusConflict:
		return ErrAlreadyExists
	case http.StatusUnauthorized:
		return ErrInsufficientPermissions
	default:
		return fmt.Errorf("%w - got %d expected %d", ErrUnexpectedStatusCode, statusCode, expectedStatusCode)
	}
}

func paginationPrefix(prefix string) *PaginationPrefix {
	p := PaginationPrefix(prefix)
	return &p
}

func paginationAfter(after string) *PaginationAfter {
	p := PaginationAfter(after)
	return &p
}

func paginationAmount(amount int) *PaginationAmount {
	p := PaginationAmount(amount)
	return &p
}

func (a *APIAuthService) DeleteGroup(ctx context.Context, groupID string) error {
	ctx = httputil.SetClientTrace(ctx, "api_auth")
	resp, err := a.apiClient.DeleteGroupWithResponse(ctx, groupID)
	if err != nil {
		a.logger.WithError(err).WithField("group", groupID).Error("failed to delete group")
		return err
	}
	return a.validateResponse(resp, http.StatusNoContent)
}

func (a *APIAuthService) GetGroup(ctx context.Context, groupID string) (*model.Group, error) {
	ctx = httputil.SetClientTrace(ctx, "api_auth")
	resp, err := a.apiClient.GetGroupWithResponse(ctx, groupID)
	if err != nil {
		a.logger.WithError(err).WithField("group", groupID).Error("failed to get group")
		return nil, err
	}
	if err := a.validateResponse(resp, http.StatusOK); err != nil {
		return nil, err
	}

	return &model.Group{
		CreatedAt:   time.Unix(resp.JSON200.CreationDate, 0),
		DisplayName: resp.JSON200.Name,
		ID:          groupIDOrDisplayName(*resp.JSON200),
	}, nil
}

func (a *APIAuthService) ListGroups(ctx context.Context, params *model.PaginationParams) ([]*model.Group, *model.Paginator, error) {
	ctx = httputil.SetClientTrace(ctx, "api_auth")
	resp, err := a.apiClient.ListGroupsWithResponse(ctx, &ListGroupsParams{
		Prefix: paginationPrefix(params.Prefix),
		After:  paginationAfter(params.After),
		Amount: paginationAmount(params.Amount),
	})
	if err != nil {
		a.logger.WithError(err).Error("failed to list groups")
		return nil, nil, err
	}
	if err := a.validateResponse(resp, http.StatusOK); err != nil {
		return nil, nil, err
	}
	groups := make([]*model.Group, len(resp.JSON200.Results))

	for i, r := range resp.JSON200.Results {
		groups[i] = &model.Group{
			CreatedAt:   time.Unix(r.CreationDate, 0),
			DisplayName: r.Name,
			ID:          groupIDOrDisplayName(r),
		}
	}
	return groups, toPagination(resp.JSON200.Pagination), nil
}

func (a *APIAuthService) AddUserToGroup(ctx context.Context, username, groupID string) error {
	ctx = httputil.SetClientTrace(ctx, "api_auth")
	resp, err := a.apiClient.AddGroupMembershipWithResponse(ctx, groupID, username)
	if err != nil {
		a.logger.WithError(err).WithField("group", groupID).WithField("username", username).Error("failed to add user to group")
		return err
	}
	return a.validateResponse(resp, http.StatusCreated)
}

func (a *APIAuthService) RemoveUserFromGroup(ctx context.Context, username, groupID string) error {
	ctx = httputil.SetClientTrace(ctx, "api_auth")
	resp, err := a.apiClient.DeleteGroupMembershipWithResponse(ctx, groupID, username)
	if err != nil {
		a.logger.WithError(err).WithField("group", groupID).WithField("username", username).Error("failed to remove user from group")
		return err
	}
	return a.validateResponse(resp, http.StatusNoContent)
}

func (a *APIAuthService) ListUserGroups(ctx context.Context, username string, params *model.PaginationParams) ([]*model.Group, *model.Paginator, error) {
	ctx = httputil.SetClientTrace(ctx, "api_auth")
	resp, err := a.apiClient.ListUserGroupsWithResponse(ctx, username, &ListUserGroupsParams{
		Prefix: paginationPrefix(params.Prefix),
		After:  paginationAfter(params.After),
		Amount: paginationAmount(params.Amount),
	})
	if err != nil {
		a.logger.WithError(err).WithField("username", username).Error("failed to list user groups")
		return nil, nil, err
	}
	if err := a.validateResponse(resp, http.StatusOK); err != nil {
		return nil, nil, err
	}
	userGroups := make([]*model.Group, len(resp.JSON200.Results))
	for i, r := range resp.JSON200.Results {
		userGroups[i] = &model.Group{
			CreatedAt:   time.Unix(r.CreationDate, 0),
			DisplayName: r.Name,
			ID:          groupIDOrDisplayName(r),
		}
	}
	return userGroups, toPagination(resp.JSON200.Pagination), nil
}

func (a *APIAuthService) ListGroupUsers(ctx context.Context, groupID string, params *model.PaginationParams) ([]*model.User, *model.Paginator, error) {
	ctx = httputil.SetClientTrace(ctx, "api_auth")
	resp, err := a.apiClient.ListGroupMembersWithResponse(ctx, groupID, &ListGroupMembersParams{
		Prefix: paginationPrefix(params.Prefix),
		After:  paginationAfter(params.After),
		Amount: paginationAmount(params.Amount),
	})
	if err != nil {
		a.logger.WithError(err).WithField("group", groupID).Error("failed to list group users")
		return nil, nil, err
	}
	if err := a.validateResponse(resp, http.StatusOK); err != nil {
		return nil, nil, err
	}
	members := make([]*model.User, len(resp.JSON200.Results))

	for i, r := range resp.JSON200.Results {
		members[i] = &model.User{
			CreatedAt:    time.Unix(r.CreationDate, 0),
			Username:     r.Username,
			FriendlyName: r.FriendlyName,
			Email:        r.Email,
		}
	}
	return members, toPagination(resp.JSON200.Pagination), nil
}

func (a *APIAuthService) WritePolicy(ctx context.Context, policy *model.Policy, update bool) error {
	ctx = httputil.SetClientTrace(ctx, "api_auth")
	if err := model.ValidateAuthEntityID(policy.DisplayName); err != nil {
		return err
	}
	stmts := make([]Statement, len(policy.Statement))
	for i, s := range policy.Statement {
		stmts[i] = Statement{
			Action:   s.Action,
			Effect:   s.Effect,
			Resource: s.Resource,
		}
	}
	createdAt := policy.CreatedAt.Unix()

	requestBody := Policy{
		CreationDate: &createdAt,
		Name:         policy.DisplayName,
		Statement:    stmts,
		Acl:          (*string)(&policy.ACL.Permission),
	}
	if update {
		// Update existing policy
		resp, err := a.apiClient.UpdatePolicyWithResponse(ctx, policy.DisplayName, UpdatePolicyJSONRequestBody(requestBody))
		if err != nil {
			a.logger.WithError(err).WithField("policy", policy.DisplayName).Error("failed to update policy")
			return err
		}
		return a.validateResponse(resp, http.StatusOK)
	}
	// Otherwise Create new
	resp, err := a.apiClient.CreatePolicyWithResponse(ctx, CreatePolicyJSONRequestBody(requestBody))
	if err != nil {
		a.logger.WithError(err).WithField("policy", policy.DisplayName).Error("failed to create policy")
		return err
	}
	return a.validateResponse(resp, http.StatusCreated)
}

func serializePolicyToModalPolicy(p Policy) *model.Policy {
	stmts := make(model.Statements, len(p.Statement))
	for i, apiStatement := range p.Statement {
		stmts[i] = model.Statement{
			Effect:   apiStatement.Effect,
			Action:   apiStatement.Action,
			Resource: apiStatement.Resource,
		}
	}
	var creationTime time.Time
	if p.CreationDate != nil {
		creationTime = time.Unix(*p.CreationDate, 0)
	}
	return &model.Policy{
		CreatedAt:   creationTime,
		DisplayName: p.Name,
		Statement:   stmts,
		ACL: model.ACL{
			Permission: model.ACLPermission(swag.StringValue(p.Acl)),
		},
	}
}

func (a *APIAuthService) GetPolicy(ctx context.Context, policyDisplayName string) (*model.Policy, error) {
	ctx = httputil.SetClientTrace(ctx, "api_auth")
	resp, err := a.apiClient.GetPolicyWithResponse(ctx, policyDisplayName)
	if err != nil {
		a.logger.WithError(err).WithField("policy", policyDisplayName).Error("failed to get policy")
		return nil, err
	}
	if err := a.validateResponse(resp, http.StatusOK); err != nil {
		return nil, err
	}

	return serializePolicyToModalPolicy(*resp.JSON200), nil
}

func (a *APIAuthService) DeletePolicy(ctx context.Context, policyDisplayName string) error {
	ctx = httputil.SetClientTrace(ctx, "api_auth")
	resp, err := a.apiClient.DeletePolicyWithResponse(ctx, policyDisplayName)
	if err != nil {
		a.logger.WithError(err).WithField("policy", policyDisplayName).Error("failed to delete policy")
		return err
	}
	return a.validateResponse(resp, http.StatusNoContent)
}

func (a *APIAuthService) ListPolicies(ctx context.Context, params *model.PaginationParams) ([]*model.Policy, *model.Paginator, error) {
	ctx = httputil.SetClientTrace(ctx, "api_auth")
	resp, err := a.apiClient.ListPoliciesWithResponse(ctx, &ListPoliciesParams{
		Prefix: paginationPrefix(params.Prefix),
		After:  paginationAfter(params.After),
		Amount: paginationAmount(params.Amount),
	})
	if err != nil {
		a.logger.WithError(err).Error("failed to list policies")
		return nil, nil, err
	}
	if err := a.validateResponse(resp, http.StatusOK); err != nil {
		return nil, nil, err
	}
	policies := make([]*model.Policy, len(resp.JSON200.Results))

	for i, r := range resp.JSON200.Results {
		policies[i] = serializePolicyToModalPolicy(r)
	}
	return policies, toPagination(resp.JSON200.Pagination), nil
}

func (a *APIAuthService) CreateCredentials(ctx context.Context, username string) (*model.Credential, error) {
	ctx = httputil.SetClientTrace(ctx, "api_auth")
	resp, err := a.apiClient.CreateCredentialsWithResponse(ctx, username, &CreateCredentialsParams{})
	if err != nil {
		a.logger.WithError(err).WithField("username", username).Error("failed to create credentials")
		return nil, err
	}
	if err := a.validateResponse(resp, http.StatusCreated); err != nil {
		return nil, err
	}
	credentials := resp.JSON201
	return &model.Credential{
		Username: strconv.Itoa(0),
		BaseCredential: model.BaseCredential{
			AccessKeyID:     credentials.AccessKeyId,
			SecretAccessKey: credentials.SecretAccessKey,
			IssuedDate:      time.Unix(credentials.CreationDate, 0),
		},
	}, err
}

func (a *APIAuthService) AddCredentials(ctx context.Context, username, accessKeyID, secretAccessKey string) (*model.Credential, error) {
	ctx = httputil.SetClientTrace(ctx, "api_auth")
	resp, err := a.apiClient.CreateCredentialsWithResponse(ctx, username, &CreateCredentialsParams{
		AccessKey: &accessKeyID,
		SecretKey: &secretAccessKey,
	})
	if err != nil {
		a.logger.WithError(err).WithField("username", username).Error("failed to add credentials")
		return nil, err
	}
	if err := a.validateResponse(resp, http.StatusCreated); err != nil {
		return nil, err
	}
	credentials := resp.JSON201
	return &model.Credential{
		Username: strconv.Itoa(0),
		BaseCredential: model.BaseCredential{
			AccessKeyID:     credentials.AccessKeyId,
			SecretAccessKey: credentials.SecretAccessKey,
			IssuedDate:      time.Unix(credentials.CreationDate, 0),
		},
	}, err
}

func (a *APIAuthService) DeleteCredentials(ctx context.Context, username, accessKeyID string) error {
	ctx = httputil.SetClientTrace(ctx, "api_auth")
	resp, err := a.apiClient.DeleteCredentialsWithResponse(ctx, username, accessKeyID)
	if err != nil {
		a.logger.WithError(err).WithField("username", username).Error("failed to delete credentials")
		return err
	}
	return a.validateResponse(resp, http.StatusNoContent)
}

func (a *APIAuthService) GetCredentialsForUser(ctx context.Context, username, accessKeyID string) (*model.Credential, error) {
	ctx = httputil.SetClientTrace(ctx, "api_auth")
	resp, err := a.apiClient.GetCredentialsForUserWithResponse(ctx, username, accessKeyID)
	if err != nil {
		a.logger.WithError(err).WithField("username", username).Error("failed to get credentials")
		return nil, err
	}
	if err := a.validateResponse(resp, http.StatusOK); err != nil {
		return nil, err
	}
	credentials := resp.JSON200
	return &model.Credential{
		BaseCredential: model.BaseCredential{
			AccessKeyID: credentials.AccessKeyId,
			IssuedDate:  time.Unix(credentials.CreationDate, 0),
		},
		Username: username,
	}, nil
}

func (a *APIAuthService) GetCredentials(ctx context.Context, accessKeyID string) (*model.Credential, error) {
	ctx = httputil.SetClientTrace(ctx, "api_auth")
	return a.cache.GetCredential(accessKeyID, func() (*model.Credential, error) {
		resp, err := a.apiClient.GetCredentialsWithResponse(ctx, accessKeyID)
		if err != nil {
			a.logger.WithError(err).Error("failed to get credentials by access key id")
			return nil, err
		}
		if err := a.validateResponse(resp, http.StatusOK); err != nil {
			return nil, err
		}
		credentials := resp.JSON200
		if credentials == nil {
			return nil, fmt.Errorf("get credentials api %w", ErrInvalidResponse)
		}
		username := aws.ToString(credentials.UserName)
		if username == "" {
			user, err := a.GetUserByID(ctx, model.ConvertDBID(credentials.UserId))
			if err != nil {
				return nil, err
			}
			username = user.Username
		}
		return &model.Credential{
			BaseCredential: model.BaseCredential{
				AccessKeyID:                   credentials.AccessKeyId,
				SecretAccessKey:               credentials.SecretAccessKey,
				SecretAccessKeyEncryptedBytes: nil,
				IssuedDate:                    time.Unix(credentials.CreationDate, 0),
			},
			Username: username,
		}, nil
	})
}

func (a *APIAuthService) ListUserCredentials(ctx context.Context, username string, params *model.PaginationParams) ([]*model.Credential, *model.Paginator, error) {
	ctx = httputil.SetClientTrace(ctx, "api_auth")
	resp, err := a.apiClient.ListUserCredentialsWithResponse(ctx, username, &ListUserCredentialsParams{
		Prefix: paginationPrefix(params.Prefix),
		After:  paginationAfter(params.After),
		Amount: paginationAmount(params.Amount),
	})
	if err != nil {
		a.logger.WithError(err).WithField("username", username).Error("failed to list credentials")
		return nil, nil, err
	}
	if err := a.validateResponse(resp, http.StatusOK); err != nil {
		return nil, nil, err
	}

	credentials := make([]*model.Credential, len(resp.JSON200.Results))

	for i, r := range resp.JSON200.Results {
		credentials[i] = &model.Credential{
			BaseCredential: model.BaseCredential{
				AccessKeyID: r.AccessKeyId,
				IssuedDate:  time.Unix(r.CreationDate, 0),
			},
			Username: strconv.Itoa(0),
		}
	}
	return credentials, toPagination(resp.JSON200.Pagination), nil
}

func (a *APIAuthService) AttachPolicyToUser(ctx context.Context, policyDisplayName, username string) error {
	ctx = httputil.SetClientTrace(ctx, "api_auth")
	resp, err := a.apiClient.AttachPolicyToUserWithResponse(ctx, username, policyDisplayName)
	if err != nil {
		a.logger.WithError(err).WithField("username", username).Error("failed to attach policy to user")
		return err
	}
	return a.validateResponse(resp, http.StatusCreated)
}

func (a *APIAuthService) DetachPolicyFromUser(ctx context.Context, policyDisplayName, username string) error {
	ctx = httputil.SetClientTrace(ctx, "api_auth")
	resp, err := a.apiClient.DetachPolicyFromUserWithResponse(ctx, username, policyDisplayName)
	if err != nil {
		a.logger.WithError(err).WithField("username", username).Error("failed to detach policy from user")
		return err
	}
	return a.validateResponse(resp, http.StatusNoContent)
}

func (a *APIAuthService) listUserPolicies(ctx context.Context, username string, params *model.PaginationParams, effective bool) ([]*model.Policy, *model.Paginator, error) {
	resp, err := a.apiClient.ListUserPoliciesWithResponse(ctx, username, &ListUserPoliciesParams{
		Prefix:    paginationPrefix(params.Prefix),
		After:     paginationAfter(params.After),
		Amount:    paginationAmount(params.Amount),
		Effective: &effective,
	})
	if err != nil {
		a.logger.WithError(err).WithField("username", username).Error("failed to list policies")
		return nil, nil, err
	}
	if err := a.validateResponse(resp, http.StatusOK); err != nil {
		return nil, nil, err
	}
	policies := make([]*model.Policy, len(resp.JSON200.Results))

	for i, r := range resp.JSON200.Results {
		policies[i] = serializePolicyToModalPolicy(r)
	}
	return policies, toPagination(resp.JSON200.Pagination), nil
}

func (a *APIAuthService) ListUserPolicies(ctx context.Context, username string, params *model.PaginationParams) ([]*model.Policy, *model.Paginator, error) {
	ctx = httputil.SetClientTrace(ctx, "api_auth")
	return a.listUserPolicies(ctx, username, params, false)
}

func (a *APIAuthService) listAllEffectivePolicies(ctx context.Context, username string) ([]*model.Policy, error) {
	hasMore := true
	after := ""
	amount := MaxPage
	policies := make([]*model.Policy, 0)
	for hasMore {
		p, paginator, err := a.ListEffectivePolicies(ctx, username, &model.PaginationParams{
			After:  after,
			Amount: amount,
		})
		if err != nil {
			return nil, err
		}
		policies = append(policies, p...)
		after = paginator.NextPageToken
		hasMore = paginator.NextPageToken != ""
	}
	return policies, nil
}

func (a *APIAuthService) ListEffectivePolicies(ctx context.Context, username string, params *model.PaginationParams) ([]*model.Policy, *model.Paginator, error) {
	ctx = httputil.SetClientTrace(ctx, "api_auth")
	if params.Amount == -1 {
		// read through the cache when requesting the full list
		policies, err := a.cache.GetUserPolicies(username, func() ([]*model.Policy, error) {
			return a.listAllEffectivePolicies(ctx, username)
		})
		if err != nil {
			return nil, nil, err
		}
		return policies, &model.Paginator{Amount: len(policies)}, nil
	}
	return a.listUserPolicies(ctx, username, params, true)
}

func (a *APIAuthService) AttachPolicyToGroup(ctx context.Context, policyDisplayName, groupID string) error {
	ctx = httputil.SetClientTrace(ctx, "api_auth")
	resp, err := a.apiClient.AttachPolicyToGroupWithResponse(ctx, groupID, policyDisplayName)
	if err != nil {
		a.logger.WithError(err).
			WithFields(logging.Fields{"policy": policyDisplayName, "group": groupID}).
			Error("failed to attach policy to group")
		return err
	}
	return a.validateResponse(resp, http.StatusCreated)
}

func (a *APIAuthService) DetachPolicyFromGroup(ctx context.Context, policyDisplayName, groupID string) error {
	ctx = httputil.SetClientTrace(ctx, "api_auth")
	resp, err := a.apiClient.DetachPolicyFromGroupWithResponse(ctx, groupID, policyDisplayName)
	if err != nil {
		a.logger.WithError(err).
			WithFields(logging.Fields{"policy": policyDisplayName, "group": groupID}).
			Error("failed to detach policy from group")
		return err
	}
	return a.validateResponse(resp, http.StatusNoContent)
}

func (a *APIAuthService) ListGroupPolicies(ctx context.Context, groupID string, params *model.PaginationParams) ([]*model.Policy, *model.Paginator, error) {
	ctx = httputil.SetClientTrace(ctx, "api_auth")
	resp, err := a.apiClient.ListGroupPoliciesWithResponse(ctx, groupID, &ListGroupPoliciesParams{
		Prefix: paginationPrefix(params.Prefix),
		After:  paginationAfter(params.After),
		Amount: paginationAmount(params.Amount),
	})
	if err != nil {
		a.logger.WithError(err).WithField("group", groupID).Error("failed to list policies")
		return nil, nil, err
	}
	if err := a.validateResponse(resp, http.StatusOK); err != nil {
		return nil, nil, err
	}
	policies := make([]*model.Policy, len(resp.JSON200.Results))

	for i, r := range resp.JSON200.Results {
		policies[i] = serializePolicyToModalPolicy(r)
	}
	return policies, toPagination(resp.JSON200.Pagination), nil
}

func (a *APIAuthService) Authorize(ctx context.Context, req *AuthorizationRequest) (*AuthorizationResponse, error) {
	ctx = httputil.SetClientTrace(ctx, "api_auth")
	policies, _, err := a.ListEffectivePolicies(ctx, req.Username, &model.PaginationParams{
		After:  "", // all
		Amount: -1, // all
	})
	if err != nil {
		return nil, err
	}
	permAudit := &MissingPermissions{}
	allowed := CheckPermissions(ctx, req.RequiredPermissions, req.Username, policies, permAudit)

	if allowed != CheckAllow {
		return &AuthorizationResponse{
			Allowed: false,
			Error:   fmt.Errorf("%w: %s", ErrInsufficientPermissions, permAudit.String()),
		}, nil
	}

	// we're allowed!
	return &AuthorizationResponse{Allowed: true}, nil
}

func (a *APIAuthService) ClaimTokenIDOnce(ctx context.Context, tokenID string, expiresAt int64) error {
	ctx = httputil.SetClientTrace(ctx, "api_auth")
	res, err := a.apiClient.ClaimTokenIdWithResponse(ctx, ClaimTokenIdJSONRequestBody{
		ExpiresAt: expiresAt,
		TokenId:   tokenID,
	})
	if err != nil {
		a.logger.WithError(err).Error("failed to claim token id")
		return err
	}
	if res.StatusCode() == http.StatusBadRequest {
		return ErrInvalidToken
	}
	return a.validateResponse(res, http.StatusCreated)
}

func (a *APIAuthService) CheckHealth(ctx context.Context, logger logging.Logger, timeout time.Duration) error {
	ctx = httputil.SetClientTrace(ctx, "api_auth")
	logger.Info("Performing health check, this can take up to ", timeout)
	bo := backoff.NewExponentialBackOff()
	bo.MaxInterval = healthCheckMaxInterval
	bo.InitialInterval = healthCheckInitialInterval
	bo.MaxElapsedTime = timeout
	err := backoff.Retry(func() error {
		healthResp, err := a.apiClient.HealthCheckWithResponse(ctx)
		switch {
		case err != nil:
			return err
		case healthResp.StatusCode() == http.StatusNoContent:
			return nil
		default:
			return fmt.Errorf("health check returned status %s: %w", healthResp.Status(), ErrInvalidResponse)
		}
	}, bo)
	if err != nil {
		return err
	}

	resp, err := a.apiClient.GetVersionWithResponse(ctx)
	if err != nil {
		return fmt.Errorf("get version failed: %w", err)
	}
	if resp.JSON200 == nil {
		return fmt.Errorf("get version returned status %s: %w", resp.Status(), ErrInvalidResponse)
	}
	logger.Info("auth API server version: ", resp.JSON200.Version)
	return nil
}

func (a *APIAuthService) IsExternalPrincipalsEnabled(_ context.Context) bool {
	return a.externalPrincipalsEnabled
}

func (a *APIAuthService) CreateUserExternalPrincipal(ctx context.Context, userID, principalID string) error {
	ctx = httputil.SetClientTrace(ctx, "api_auth")
	if !a.IsExternalPrincipalsEnabled(ctx) {
		return fmt.Errorf("external principals disabled: %w", ErrInvalidRequest)
	}

	resp, err := a.apiClient.CreateUserExternalPrincipalWithResponse(ctx, userID, &CreateUserExternalPrincipalParams{
		PrincipalId: principalID,
	})
	if err != nil {
		return fmt.Errorf("create principal: %w", err)
	}

	return a.validateResponse(resp, http.StatusCreated)
}

func (a *APIAuthService) DeleteUserExternalPrincipal(ctx context.Context, userID, principalID string) error {
	ctx = httputil.SetClientTrace(ctx, "api_auth")
	if !a.IsExternalPrincipalsEnabled(ctx) {
		return fmt.Errorf("external principals disabled: %w", ErrInvalidRequest)
	}
	resp, err := a.apiClient.DeleteUserExternalPrincipalWithResponse(ctx, userID, &DeleteUserExternalPrincipalParams{
		principalID,
	})
	if err != nil {
		return fmt.Errorf("delete external principal: %w", err)
	}
	return a.validateResponse(resp, http.StatusNoContent)
}

func (a *APIAuthService) GetExternalPrincipal(ctx context.Context, principalID string) (*model.ExternalPrincipal, error) {
	ctx = httputil.SetClientTrace(ctx, "api_auth")
	if !a.IsExternalPrincipalsEnabled(ctx) {
		return nil, fmt.Errorf("external principals disabled: %w", ErrInvalidRequest)
	}
	resp, err := a.apiClient.GetExternalPrincipalWithResponse(ctx, &GetExternalPrincipalParams{
		PrincipalId: principalID,
	})
	if err != nil {
		return nil, fmt.Errorf("get external principal: %w", err)
	}
	if err := a.validateResponse(resp, http.StatusOK); err != nil {
		return nil, err
	}
	return &model.ExternalPrincipal{
		ID:     resp.JSON200.Id,
		UserID: resp.JSON200.UserId,
	}, nil
}

func (a *APIAuthService) ListUserExternalPrincipals(ctx context.Context, userID string, params *model.PaginationParams) ([]*model.ExternalPrincipal, *model.Paginator, error) {
	ctx = httputil.SetClientTrace(ctx, "api_auth")
	if !a.IsExternalPrincipalsEnabled(ctx) {
		return nil, nil, fmt.Errorf("external principals disabled: %w", ErrInvalidRequest)
	}
	resp, err := a.apiClient.ListUserExternalPrincipalsWithResponse(ctx, userID, &ListUserExternalPrincipalsParams{
		Prefix: paginationPrefix(params.Prefix),
		After:  paginationAfter(params.After),
		Amount: paginationAmount(params.Amount),
	})
	if err != nil {
		return nil, nil, fmt.Errorf("list user external principals: %w", err)
	}
	if err := a.validateResponse(resp, http.StatusOK); err != nil {
		return nil, nil, err
	}

	principals := make([]*model.ExternalPrincipal, len(resp.JSON200.Results))
	for i, p := range resp.JSON200.Results {
		principals[i] = &model.ExternalPrincipal{
			ID:     p.Id,
			UserID: p.UserId,
		}
	}
	return principals, toPagination(resp.JSON200.Pagination), nil
}

func NewAPIAuthService(apiEndpoint, token string, externalPrincipalseEnabled bool, secretStore crypt.SecretStore, cacheConf params.ServiceCache, logger logging.Logger) (*APIAuthService, error) {
	if token == "" {
		// when no token is provided, generate one.
		// communicate with auth service always uses a token
		var err error
		token, err = generateAuthAPIJWT(logger, secretStore.SharedSecret())
		if err != nil {
			return nil, fmt.Errorf("failed to generate auth api token: %w", err)
		}
	}

	bearerToken, err := securityprovider.NewSecurityProviderBearerToken(token)
	if err != nil {
		return nil, err
	}

	httpClient := &http.Client{}
	client, err := NewClientWithResponses(
		apiEndpoint,
		WithRequestEditorFn(bearerToken.Intercept),
		WithRequestEditorFn(AddRequestID(httputil.RequestIDHeaderName)),
		WithHTTPClient(httpClient),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create auth api client: %w", err)
	}
	logger.Info("initialized authorization service")
	var cache Cache
	if cacheConf.Enabled {
		cache = NewLRUCache(cacheConf.Size, cacheConf.TTL, cacheConf.Jitter)
	} else {
		cache = &DummyCache{}
	}
	res := &APIAuthService{
		apiClient:                 client,
		secretStore:               secretStore,
		logger:                    logger,
		cache:                     cache,
		externalPrincipalsEnabled: externalPrincipalseEnabled,
	}
	return res, nil
}

// generateAuthAPIJWT generates a new auth api jwt token
func generateAuthAPIJWT(logger logging.Logger, secret []byte) (string, error) {
	const tokenExprInYears = 10
	now := time.Now()
	exp := now.AddDate(tokenExprInYears, 0, 0)
	id := xid.New().String()
	claims := &jwt.RegisteredClaims{
		ID:        id,
		Audience:  jwt.ClaimStrings{"auth-client"},
		Subject:   "_lakefs-internal",
		IssuedAt:  jwt.NewNumericDate(now),
		ExpiresAt: jwt.NewNumericDate(exp),
	}
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	logger.WithField("id", id).Info("generated auth api token")
	return token.SignedString(secret)
}

func groupIDOrDisplayName(group Group) string {
	stringID := swag.StringValue(group.Id)
	if stringID != "" {
		return stringID
	}
	return group.Name
}

func NewAPIAuthServiceWithClient(client ClientWithResponsesInterface, externalPrincipalseEnabled bool, secretStore crypt.SecretStore, cacheConf params.ServiceCache, logger logging.Logger) (*APIAuthService, error) {
	var cache Cache
	if cacheConf.Enabled {
		cache = NewLRUCache(cacheConf.Size, cacheConf.TTL, cacheConf.Jitter)
	} else {
		cache = &DummyCache{}
	}
	return &APIAuthService{
		apiClient:                 client,
		secretStore:               secretStore,
		cache:                     cache,
		logger:                    logger,
		externalPrincipalsEnabled: externalPrincipalseEnabled,
	}, nil
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
	allowed := CheckNeutral
	switch node.Type {
	case permissions.NodeTypeNode:
		hasPermission := false
		// check whether the permission is allowed, denied or natural (not allowed and not denied)
		for _, policy := range policies {
			for _, stmt := range policy.Statement {
				resource := interpolateUser(stmt.Resource, username)
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
		if !hasPermission {
			permAudit.Unauthorized = append(permAudit.Unauthorized, node.Permission.Action)
		}

	case permissions.NodeTypeOr:
		// returns:
		// Allowed - at least one of the permissions is allowed and no one is denied
		// Denied - one of the permissions is Deny
		// Natural - otherwise
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
		logging.FromContext(ctx).Error("unknown permission node type")
		return CheckDeny
	}
	return allowed
}

func interpolateUser(resource string, username string) string {
	return strings.ReplaceAll(resource, "${user}", username)
}
