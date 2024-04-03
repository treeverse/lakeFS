package auth

//go:generate go run github.com/deepmap/oapi-codegen/cmd/oapi-codegen@v1.5.6 -package auth -generate "types,client" -o client.gen.go ../../api/authorization.yml
//go:generate go run github.com/golang/mock/mockgen@v1.6.0 -package=mock -destination=mock/mock_auth_client.go github.com/treeverse/lakefs/pkg/auth ClientWithResponsesInterface

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sort"
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
	"github.com/treeverse/lakefs/pkg/auth/keys"
	"github.com/treeverse/lakefs/pkg/auth/model"
	"github.com/treeverse/lakefs/pkg/auth/params"
	"github.com/treeverse/lakefs/pkg/auth/wildcard"
	"github.com/treeverse/lakefs/pkg/kv"
	"github.com/treeverse/lakefs/pkg/logging"
	"github.com/treeverse/lakefs/pkg/permissions"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type AuthorizationRequest struct {
	Username            string
	RequiredPermissions permissions.Node
}

type AuthorizationResponse struct {
	Allowed bool
	Error   error
}

// CheckResult - the final result for the authorization is accepted only if it's CheckAllow
type CheckResult int

const (
	InvalidUserID = ""
	maxPage       = 1000
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

func (s *AuthService) ListKVPaged(ctx context.Context, protoType protoreflect.MessageType, params *model.PaginationParams, prefix []byte, secondary bool) ([]proto.Message, *model.Paginator, error) {
	var (
		it    kv.MessageIterator
		err   error
		after []byte
	)
	if params.After != "" {
		after = make([]byte, len(prefix)+len(params.After))

		l := copy(after, prefix)
		_ = copy(after[l:], params.After)
	}
	if secondary {
		it, err = kv.NewSecondaryIterator(ctx, s.store, protoType, model.PartitionKey, prefix, after)
	} else {
		it, err = kv.NewPrimaryIterator(ctx, s.store, protoType, model.PartitionKey, prefix, kv.IteratorOptionsAfter(after))
	}
	if err != nil {
		return nil, nil, fmt.Errorf("scan prefix(%s): %w", prefix, err)
	}
	defer it.Close()

	amount := maxPage
	if params.Amount >= 0 && params.Amount < maxPage {
		amount = params.Amount
	}

	entries := make([]proto.Message, 0)
	p := &model.Paginator{}
	for len(entries) < amount && it.Next() {
		entry := it.Entry()
		// skip nil entries (deleted), kv can hold nil values
		if entry == nil {
			continue
		}
		entries = append(entries, entry.Value)
		if len(entries) == amount {
			p.NextPageToken = strings.TrimPrefix(string(entry.Key), string(prefix))
			break
		}
	}
	if err = it.Err(); err != nil {
		return nil, nil, fmt.Errorf("list DB: %w", err)
	}
	p.Amount = len(entries)
	return entries, p, nil
}

type AuthService struct {
	store       kv.Store
	secretStore crypt.SecretStore
	cache       Cache
	log         logging.Logger
}

func NewAuthService(store kv.Store, secretStore crypt.SecretStore, cacheConf params.ServiceCache, logger logging.Logger) *AuthService {
	logger.Info("initialized Auth service")
	var cache Cache
	if cacheConf.Enabled {
		cache = NewLRUCache(cacheConf.Size, cacheConf.TTL, cacheConf.Jitter)
	} else {
		cache = &DummyCache{}
	}
	res := &AuthService{
		store:       store,
		secretStore: secretStore,
		cache:       cache,
		log:         logger,
	}
	return res
}

func (s *AuthService) SecretStore() crypt.SecretStore {
	return s.secretStore
}

func (s *AuthService) Cache() Cache {
	return s.cache
}

func (s *AuthService) CreateUser(ctx context.Context, user *model.User) (string, error) {
	if err := model.ValidateAuthEntityID(user.Username); err != nil {
		return InvalidUserID, err
	}
	userKey := model.UserPath(user.Username)

	err := kv.SetMsgIf(ctx, s.store, model.PartitionKey, userKey, model.ProtoFromUser(user), nil)
	if err != nil {
		if errors.Is(err, kv.ErrPredicateFailed) {
			err = ErrAlreadyExists
		}
		return "", fmt.Errorf("save user (userKey %s): %w", userKey, err)
	}
	return user.Username, err
}

func (s *AuthService) DeleteUser(ctx context.Context, username string) error {
	if _, err := s.GetUser(ctx, username); err != nil {
		return err
	}
	userPath := model.UserPath(username)

	// delete policy attached to user
	policiesKey := model.UserPolicyPath(username, "")
	it, err := kv.NewSecondaryIterator(ctx, s.store, (&model.PolicyData{}).ProtoReflect().Type(), model.PartitionKey, policiesKey, []byte(""))
	if err != nil {
		return err
	}
	defer it.Close()
	for it.Next() {
		entry := it.Entry()
		policy := entry.Value.(*model.PolicyData)
		if err = s.DetachPolicyFromUserNoValidation(ctx, policy.DisplayName, username); err != nil {
			return err
		}
	}
	if err = it.Err(); err != nil {
		return err
	}

	// delete user membership of group
	groupKey := model.GroupPath("")
	itr, err := kv.NewPrimaryIterator(ctx, s.store, (&model.GroupData{}).ProtoReflect().Type(), model.PartitionKey, groupKey, kv.IteratorOptionsAfter([]byte("")))
	if err != nil {
		return err
	}
	defer itr.Close()
	for itr.Next() {
		entry := itr.Entry()
		group := entry.Value.(*model.GroupData)
		if err = s.removeUserFromGroupNoValidation(ctx, username, group.DisplayName); err != nil {
			return err
		}
	}
	if err = itr.Err(); err != nil {
		return err
	}

	// delete user
	err = s.store.Delete(ctx, []byte(model.PartitionKey), userPath)
	if err != nil {
		return fmt.Errorf("delete user (userKey %s): %w", userPath, err)
	}
	return err
}

type UserPredicate func(u *model.UserData) bool

func (s *AuthService) getUserByPredicate(ctx context.Context, key userKey, predicate UserPredicate) (*model.User, error) {
	return s.cache.GetUser(key, func() (*model.User, error) {
		m := &model.UserData{}
		itr, err := kv.NewPrimaryIterator(ctx, s.store, m.ProtoReflect().Type(), model.PartitionKey, model.UserPath(""), kv.IteratorOptionsAfter([]byte("")))
		if err != nil {
			return nil, fmt.Errorf("scan users: %w", err)
		}
		defer itr.Close()
		for itr.Next() {
			entry := itr.Entry()
			value, ok := entry.Value.(*model.UserData)
			if !ok {
				return nil, fmt.Errorf("failed to cast: %w", err)
			}
			if predicate(value) {
				return model.UserFromProto(value), nil
			}
		}
		if itr.Err() != nil {
			return nil, itr.Err()
		}
		return nil, ErrNotFound
	})
}

// GetUserByID TODO(niro): In KV ID == username, Remove this method when DB implementation is deleted
func (s *AuthService) GetUserByID(ctx context.Context, userID string) (*model.User, error) {
	return s.GetUser(ctx, userID)
}

func (s *AuthService) GetUser(ctx context.Context, username string) (*model.User, error) {
	return s.cache.GetUser(userKey{username: username}, func() (*model.User, error) {
		userKey := model.UserPath(username)
		m := model.UserData{}
		_, err := kv.GetMsg(ctx, s.store, model.PartitionKey, userKey, &m)
		if err != nil {
			if errors.Is(err, kv.ErrNotFound) {
				err = ErrNotFound
			}
			return nil, fmt.Errorf("%s: %w", username, err)
		}
		return model.UserFromProto(&m), nil
	})
}

func (s *AuthService) GetUserByEmail(ctx context.Context, email string) (*model.User, error) {
	return s.getUserByPredicate(ctx, userKey{email: email}, func(value *model.UserData) bool {
		return value.Email == email
	})
}

func (s *AuthService) GetUserByExternalID(ctx context.Context, externalID string) (*model.User, error) {
	return s.getUserByPredicate(ctx, userKey{externalID: externalID}, func(value *model.UserData) bool {
		return value.ExternalId == externalID
	})
}

func (s *AuthService) ListUsers(ctx context.Context, params *model.PaginationParams) ([]*model.User, *model.Paginator, error) {
	var user model.UserData
	usersKey := model.UserPath(params.Prefix)

	msgs, paginator, err := s.ListKVPaged(ctx, (&user).ProtoReflect().Type(), params, usersKey, false)
	if msgs == nil {
		return nil, paginator, err
	}
	return model.ConvertUsersDataList(msgs), paginator, err
}

func (s *AuthService) UpdateUserFriendlyName(ctx context.Context, userID string, friendlyName string) error {
	return ErrNotImplemented
}

func (s *AuthService) ListUserCredentials(ctx context.Context, username string, params *model.PaginationParams) ([]*model.Credential, *model.Paginator, error) {
	var credential model.CredentialData
	credentialsKey := model.CredentialPath(username, params.Prefix)
	msgs, paginator, err := s.ListKVPaged(ctx, (&credential).ProtoReflect().Type(), params, credentialsKey, false)
	if err != nil {
		return nil, nil, err
	}
	creds, err := model.ConvertCredDataList(s.secretStore, msgs)
	if err != nil {
		return nil, nil, err
	}
	return creds, paginator, nil
}

func (s *AuthService) AttachPolicyToUser(ctx context.Context, policyDisplayName string, username string) error {
	if _, err := s.GetUser(ctx, username); err != nil {
		return err
	}
	if _, err := s.GetPolicy(ctx, policyDisplayName); err != nil {
		return err
	}

	policyKey := model.PolicyPath(policyDisplayName)
	pu := model.UserPolicyPath(username, policyDisplayName)

	err := kv.SetMsgIf(ctx, s.store, model.PartitionKey, pu, &kv.SecondaryIndex{PrimaryKey: policyKey}, nil)
	if err != nil {
		if errors.Is(err, kv.ErrPredicateFailed) {
			err = ErrAlreadyExists
		}
		return fmt.Errorf("policy attachment to user: (key %s): %w", pu, err)
	}
	return nil
}

func (s *AuthService) DetachPolicyFromUserNoValidation(ctx context.Context, policyDisplayName, username string) error {
	pu := model.UserPolicyPath(username, policyDisplayName)
	err := s.store.Delete(ctx, []byte(model.PartitionKey), pu)
	if err != nil {
		return fmt.Errorf("detaching policy: (key %s): %w", pu, err)
	}
	return nil
}

func (s *AuthService) DetachPolicyFromUser(ctx context.Context, policyDisplayName, username string) error {
	if _, err := s.GetUser(ctx, username); err != nil {
		return err
	}
	if _, err := s.GetPolicy(ctx, policyDisplayName); err != nil {
		return err
	}
	return s.DetachPolicyFromUserNoValidation(ctx, policyDisplayName, username)
}

func (s *AuthService) ListUserPolicies(ctx context.Context, username string, params *model.PaginationParams) ([]*model.Policy, *model.Paginator, error) {
	var policy model.PolicyData
	userPolicyKey := model.UserPolicyPath(username, params.Prefix)

	msgs, paginator, err := s.ListKVPaged(ctx, (&policy).ProtoReflect().Type(), params, userPolicyKey, true)
	if msgs == nil {
		return nil, paginator, err
	}
	return model.ConvertPolicyDataList(msgs), paginator, err
}

func (s *AuthService) getEffectivePolicies(ctx context.Context, username string, params *model.PaginationParams) ([]*model.Policy, *model.Paginator, error) {
	if _, err := s.GetUser(ctx, username); err != nil {
		return nil, nil, err
	}

	hasMoreUserPolicy := true
	afterUserPolicy := ""
	amount := maxPage
	policiesSet := make(map[string]*model.Policy)
	// get policies attracted to user
	for hasMoreUserPolicy {
		policies, userPaginator, err := s.ListUserPolicies(ctx, username, &model.PaginationParams{
			After:  afterUserPolicy,
			Amount: amount,
		})
		if err != nil {
			return nil, nil, fmt.Errorf("list user policies: %w", err)
		}
		for _, policy := range policies {
			policiesSet[policy.DisplayName] = policy
		}
		afterUserPolicy = userPaginator.NextPageToken
		hasMoreUserPolicy = userPaginator.NextPageToken != ""
	}

	hasMoreGroup := true
	afterGroup := ""
	for hasMoreGroup {
		// get membership groups to user
		groups, groupPaginator, err := s.ListUserGroups(ctx, username, &model.PaginationParams{
			After:  afterGroup,
			Amount: amount,
		})
		if err != nil {
			return nil, nil, err
		}
		for _, group := range groups {
			// get policies attracted to group
			hasMoreGroupPolicy := true
			afterGroupPolicy := ""
			for hasMoreGroupPolicy {
				groupPolicies, groupPoliciesPaginator, err := s.ListGroupPolicies(ctx, group.DisplayName, &model.PaginationParams{
					After:  afterGroupPolicy,
					Amount: amount,
				})
				if err != nil {
					return nil, nil, fmt.Errorf("list group policies: %w", err)
				}
				for _, policy := range groupPolicies {
					policiesSet[policy.DisplayName] = policy
				}
				afterGroupPolicy = groupPoliciesPaginator.NextPageToken
				hasMoreGroupPolicy = groupPoliciesPaginator.NextPageToken != ""
			}
		}
		afterGroup = groupPaginator.NextPageToken
		hasMoreGroup = groupPaginator.NextPageToken != ""
	}

	if params.Amount < 0 || params.Amount > maxPage {
		params.Amount = maxPage
	}

	var policiesArr []string
	for k := range policiesSet {
		policiesArr = append(policiesArr, k)
	}
	sort.Strings(policiesArr)

	var resPolicies []*model.Policy
	resPaginator := model.Paginator{Amount: 0, NextPageToken: ""}
	for _, p := range policiesArr {
		if p > params.After {
			resPolicies = append(resPolicies, policiesSet[p])
			if len(resPolicies) == params.Amount {
				resPaginator.NextPageToken = p
				break
			}
		}
	}
	resPaginator.Amount = len(resPolicies)
	return resPolicies, &resPaginator, nil
}

func (s *AuthService) ListEffectivePolicies(ctx context.Context, username string, params *model.PaginationParams) ([]*model.Policy, *model.Paginator, error) {
	return ListEffectivePolicies(ctx, username, params, s.getEffectivePolicies, s.cache)
}

type effectivePoliciesGetter func(ctx context.Context, username string, params *model.PaginationParams) ([]*model.Policy, *model.Paginator, error)

func ListEffectivePolicies(ctx context.Context, username string, params *model.PaginationParams, getEffectivePolicies effectivePoliciesGetter, cache Cache) ([]*model.Policy, *model.Paginator, error) {
	if params.Amount == -1 {
		// read through the cache when requesting the full list
		policies, err := cache.GetUserPolicies(username, func() ([]*model.Policy, error) {
			policies, _, err := getEffectivePolicies(ctx, username, params)
			return policies, err
		})
		if err != nil {
			return nil, nil, err
		}
		return policies, &model.Paginator{Amount: len(policies)}, nil
	}

	return getEffectivePolicies(ctx, username, params)
}

func (s *AuthService) ListGroupPolicies(ctx context.Context, groupDisplayName string, params *model.PaginationParams) ([]*model.Policy, *model.Paginator, error) {
	var policy model.PolicyData
	groupPolicyKey := model.GroupPolicyPath(groupDisplayName, params.Prefix)

	msgs, paginator, err := s.ListKVPaged(ctx, (&policy).ProtoReflect().Type(), params, groupPolicyKey, true)
	if msgs == nil {
		return nil, paginator, err
	}
	return model.ConvertPolicyDataList(msgs), paginator, err
}

func (s *AuthService) CreateGroup(ctx context.Context, group *model.Group) (*model.Group, error) {
	if err := model.ValidateAuthEntityID(group.DisplayName); err != nil {
		return nil, err
	}

	groupKey := model.GroupPath(group.DisplayName)
	err := kv.SetMsgIf(ctx, s.store, model.PartitionKey, groupKey, model.ProtoFromGroup(group), nil)
	if err != nil {
		if errors.Is(err, kv.ErrPredicateFailed) {
			err = ErrAlreadyExists
		}
		return nil, fmt.Errorf("save group (groupKey %s): %w", groupKey, err)
	}
	retGroup := &model.Group{
		DisplayName: group.DisplayName,
		ID:          group.DisplayName,
		CreatedAt:   group.CreatedAt,
	}
	return retGroup, nil
}

func (s *AuthService) DeleteGroup(ctx context.Context, groupID string) error {
	if _, err := s.GetGroup(ctx, groupID); err != nil {
		return err
	}

	// delete user membership to group
	usersKey := model.GroupUserPath(groupID, "")
	it, err := kv.NewSecondaryIterator(ctx, s.store, (&model.UserData{}).ProtoReflect().Type(), model.PartitionKey, usersKey, []byte(""))
	if err != nil {
		return err
	}
	defer it.Close()
	for it.Next() {
		entry := it.Entry()
		user := entry.Value.(*model.UserData)
		if err = s.removeUserFromGroupNoValidation(ctx, user.Username, groupID); err != nil {
			return err
		}
	}
	if err = it.Err(); err != nil {
		return err
	}

	// delete policy attachment to group
	policiesKey := model.GroupPolicyPath(groupID, "")
	itr, err := kv.NewSecondaryIterator(ctx, s.store, (&model.PolicyData{}).ProtoReflect().Type(), model.PartitionKey, policiesKey, []byte(""))
	if err != nil {
		return err
	}
	defer it.Close()
	for itr.Next() {
		entry := itr.Entry()
		policy := entry.Value.(*model.PolicyData)
		if err = s.DetachPolicyFromGroupNoValidation(ctx, policy.DisplayName, groupID); err != nil {
			return err
		}
	}
	if err = itr.Err(); err != nil {
		return err
	}

	// delete group
	groupPath := model.GroupPath(groupID)
	err = s.store.Delete(ctx, []byte(model.PartitionKey), groupPath)
	if err != nil {
		return fmt.Errorf("delete user (userKey %s): %w", groupPath, err)
	}
	return nil
}

func (s *AuthService) GetGroup(ctx context.Context, groupID string) (*model.Group, error) {
	groupKey := model.GroupPath(groupID)
	m := model.GroupData{}
	_, err := kv.GetMsg(ctx, s.store, model.PartitionKey, groupKey, &m)
	if err != nil {
		if errors.Is(err, kv.ErrNotFound) {
			err = ErrNotFound
		}
		return nil, fmt.Errorf("%s: %w", groupID, err)
	}
	return model.GroupFromProto(&m), nil
}

func (s *AuthService) ListGroups(ctx context.Context, params *model.PaginationParams) ([]*model.Group, *model.Paginator, error) {
	var group model.GroupData
	groupKey := model.GroupPath(params.Prefix)

	msgs, paginator, err := s.ListKVPaged(ctx, (&group).ProtoReflect().Type(), params, groupKey, false)
	if msgs == nil {
		return nil, paginator, err
	}
	return model.ConvertGroupDataList(msgs), paginator, err
}

func (s *AuthService) AddUserToGroup(ctx context.Context, username, groupDisplayName string) error {
	if _, err := s.GetUser(ctx, username); err != nil {
		return err
	}
	if _, err := s.GetGroup(ctx, groupDisplayName); err != nil {
		return err
	}

	userKey := model.UserPath(username)
	gu := model.GroupUserPath(groupDisplayName, username)
	err := kv.SetMsgIf(ctx, s.store, model.PartitionKey, gu, &kv.SecondaryIndex{PrimaryKey: userKey}, nil)
	if err != nil {
		if errors.Is(err, kv.ErrPredicateFailed) {
			err = ErrAlreadyExists
		}
		return fmt.Errorf("add user to group: (key %s): %w", gu, err)
	}
	return nil
}

func (s *AuthService) removeUserFromGroupNoValidation(ctx context.Context, username, groupID string) error {
	gu := model.GroupUserPath(groupID, username)
	err := s.store.Delete(ctx, []byte(model.PartitionKey), gu)
	if err != nil {
		return fmt.Errorf("remove user from group: (key %s): %w", gu, err)
	}
	return nil
}

func (s *AuthService) RemoveUserFromGroup(ctx context.Context, username, groupID string) error {
	if _, err := s.GetUser(ctx, username); err != nil {
		return err
	}
	if _, err := s.GetGroup(ctx, groupID); err != nil {
		return err
	}
	return s.removeUserFromGroupNoValidation(ctx, username, groupID)
}

func (s *AuthService) ListUserGroups(ctx context.Context, username string, params *model.PaginationParams) ([]*model.Group, *model.Paginator, error) {
	if _, err := s.GetUser(ctx, username); err != nil {
		return nil, nil, err
	}
	if params.Amount < 0 || params.Amount > maxPage {
		params.Amount = maxPage
	}

	hasMoreGroups := true
	afterGroup := params.After
	var userGroups []*model.Group
	resPaginator := model.Paginator{Amount: 0, NextPageToken: ""}
	for hasMoreGroups && len(userGroups) <= params.Amount {
		groups, paginator, err := s.ListGroups(ctx, &model.PaginationParams{Prefix: params.Prefix, After: afterGroup, Amount: maxPage})
		if err != nil {
			return nil, nil, err
		}
		for _, group := range groups {
			path := model.GroupUserPath(group.DisplayName, username)
			m := kv.SecondaryIndex{}
			_, err := kv.GetMsg(ctx, s.store, model.PartitionKey, path, &m)
			if err != nil && !errors.Is(err, kv.ErrNotFound) {
				return nil, nil, err
			}
			if err == nil {
				appendGroup := &model.Group{
					DisplayName: group.DisplayName,
					ID:          group.DisplayName,
					CreatedAt:   group.CreatedAt,
				}
				userGroups = append(userGroups, appendGroup)
			}
			if len(userGroups) == params.Amount {
				resPaginator.NextPageToken = group.DisplayName
				resPaginator.Amount = len(userGroups)
				return userGroups, &resPaginator, nil
			}
		}
		hasMoreGroups = paginator.NextPageToken != ""
		afterGroup = paginator.NextPageToken
	}
	resPaginator.Amount = len(userGroups)
	return userGroups, &resPaginator, nil
}

func (s *AuthService) ListGroupUsers(ctx context.Context, groupID string, params *model.PaginationParams) ([]*model.User, *model.Paginator, error) {
	if _, err := s.GetGroup(ctx, groupID); err != nil {
		return nil, nil, err
	}
	var policy model.UserData
	userGroupKey := model.GroupUserPath(groupID, params.Prefix)

	msgs, paginator, err := s.ListKVPaged(ctx, (&policy).ProtoReflect().Type(), params, userGroupKey, true)
	if msgs == nil {
		return nil, paginator, err
	}
	return model.ConvertUsersDataList(msgs), paginator, err
}

func ValidatePolicy(policy *model.Policy) error {
	if err := model.ValidateAuthEntityID(policy.DisplayName); err != nil {
		return err
	}
	for _, stmt := range policy.Statement {
		for _, action := range stmt.Action {
			if err := model.ValidateActionName(action); err != nil {
				return err
			}
		}
		if err := model.ValidateArn(stmt.Resource); err != nil {
			return err
		}
		if err := model.ValidateStatementEffect(stmt.Effect); err != nil {
			return err
		}
	}
	return nil
}

func (s *AuthService) WritePolicy(ctx context.Context, policy *model.Policy, update bool) error {
	if err := ValidatePolicy(policy); err != nil {
		return err
	}
	policyKey := model.PolicyPath(policy.DisplayName)
	m := model.ProtoFromPolicy(policy)

	if update { // update policy only if it already exists
		err := kv.SetMsgIf(ctx, s.store, model.PartitionKey, policyKey, m, kv.PrecondConditionalExists)
		if err != nil {
			if errors.Is(err, kv.ErrPredicateFailed) {
				err = ErrNotFound
			}
			return err
		}
		return nil
	}

	// create policy only if it does not exist
	err := kv.SetMsgIf(ctx, s.store, model.PartitionKey, policyKey, m, nil)
	if err != nil {
		if errors.Is(err, kv.ErrPredicateFailed) {
			err = ErrAlreadyExists
		}
		return err
	}
	return nil
}

func (s *AuthService) GetPolicy(ctx context.Context, policyDisplayName string) (*model.Policy, error) {
	policyKey := model.PolicyPath(policyDisplayName)
	p := model.PolicyData{}
	_, err := kv.GetMsg(ctx, s.store, model.PartitionKey, policyKey, &p)
	if err != nil {
		if errors.Is(err, kv.ErrNotFound) {
			err = ErrNotFound
		}
		return nil, fmt.Errorf("%s: %w", policyDisplayName, err)
	}
	return model.PolicyFromProto(&p), nil
}

func (s *AuthService) DeletePolicy(ctx context.Context, policyDisplayName string) error {
	if _, err := s.GetPolicy(ctx, policyDisplayName); err != nil {
		return err
	}
	policyPath := model.PolicyPath(policyDisplayName)

	// delete policy attachment to user
	usersKey := model.UserPath("")
	it, err := kv.NewPrimaryIterator(ctx, s.store, (&model.UserData{}).ProtoReflect().Type(), model.PartitionKey, usersKey, kv.IteratorOptionsAfter([]byte("")))
	if err != nil {
		return err
	}
	defer it.Close()
	for it.Next() {
		entry := it.Entry()
		user := entry.Value.(*model.UserData)
		if err = s.DetachPolicyFromUserNoValidation(ctx, policyDisplayName, user.Username); err != nil {
			return err
		}
	}

	// delete policy attachment to group
	groupKey := model.GroupPath("")
	it, err = kv.NewPrimaryIterator(ctx, s.store, (&model.GroupData{}).ProtoReflect().Type(), model.PartitionKey, groupKey, kv.IteratorOptionsAfter([]byte("")))
	if err != nil {
		return err
	}
	defer it.Close()
	for it.Next() {
		entry := it.Entry()
		group := entry.Value.(*model.GroupData)
		if err = s.DetachPolicyFromGroupNoValidation(ctx, policyDisplayName, group.DisplayName); err != nil {
			return err
		}
	}

	// delete policy
	err = s.store.Delete(ctx, []byte(model.PartitionKey), policyPath)
	if err != nil {
		return fmt.Errorf("delete policy (policyKey %s): %w", policyPath, err)
	}
	return nil
}

func (s *AuthService) ListPolicies(ctx context.Context, params *model.PaginationParams) ([]*model.Policy, *model.Paginator, error) {
	var policy model.PolicyData
	policyKey := model.PolicyPath(params.Prefix)

	msgs, paginator, err := s.ListKVPaged(ctx, (&policy).ProtoReflect().Type(), params, policyKey, false)
	if msgs == nil {
		return nil, paginator, err
	}
	return model.ConvertPolicyDataList(msgs), paginator, err
}

func (s *AuthService) CreateCredentials(ctx context.Context, username string) (*model.Credential, error) {
	accessKeyID := keys.GenAccessKeyID()
	secretAccessKey := keys.GenSecretAccessKey()
	return s.AddCredentials(ctx, username, accessKeyID, secretAccessKey)
}

func (s *AuthService) AddCredentials(ctx context.Context, username, accessKeyID, secretAccessKey string) (*model.Credential, error) {
	if !IsValidAccessKeyID(accessKeyID) {
		return nil, ErrInvalidAccessKeyID
	}
	if len(secretAccessKey) == 0 {
		return nil, ErrInvalidSecretAccessKey
	}
	now := time.Now()
	encryptedKey, err := model.EncryptSecret(s.secretStore, secretAccessKey)
	if err != nil {
		return nil, err
	}
	user, err := s.GetUser(ctx, username)
	if err != nil {
		return nil, err
	}

	c := &model.Credential{
		BaseCredential: model.BaseCredential{
			AccessKeyID:                   accessKeyID,
			SecretAccessKey:               secretAccessKey,
			SecretAccessKeyEncryptedBytes: encryptedKey,
			IssuedDate:                    now,
		},
		Username: user.Username,
	}
	credentialsKey := model.CredentialPath(user.Username, c.AccessKeyID)
	err = kv.SetMsgIf(ctx, s.store, model.PartitionKey, credentialsKey, model.ProtoFromCredential(c), nil)
	if err != nil {
		if errors.Is(err, kv.ErrPredicateFailed) {
			err = ErrAlreadyExists
		}
		return nil, fmt.Errorf("save credentials (credentialsKey %s): %w", credentialsKey, err)
	}

	return c, nil
}

func IsValidAccessKeyID(key string) bool {
	l := len(key)
	return l >= 3 && l <= 20
}

func (s *AuthService) DeleteCredentials(ctx context.Context, username, accessKeyID string) error {
	if _, err := s.GetUser(ctx, username); err != nil {
		return err
	}
	if _, err := s.GetCredentials(ctx, accessKeyID); err != nil {
		return err
	}

	credPath := model.CredentialPath(username, accessKeyID)
	err := s.store.Delete(ctx, []byte(model.PartitionKey), credPath)
	if err != nil {
		return fmt.Errorf("delete credentials (credentialsKey %s): %w", credPath, err)
	}
	return nil
}

func (s *AuthService) AttachPolicyToGroup(ctx context.Context, policyDisplayName, groupDisplayName string) error {
	if _, err := s.GetGroup(ctx, groupDisplayName); err != nil {
		return err
	}
	if _, err := s.GetPolicy(ctx, policyDisplayName); err != nil {
		return err
	}

	policyKey := model.PolicyPath(policyDisplayName)
	pg := model.GroupPolicyPath(groupDisplayName, policyDisplayName)

	err := kv.SetMsgIf(ctx, s.store, model.PartitionKey, pg, &kv.SecondaryIndex{PrimaryKey: policyKey}, nil)
	if err != nil {
		if errors.Is(err, kv.ErrPredicateFailed) {
			err = ErrAlreadyExists
		}
		return fmt.Errorf("policy attachment to group: (key %s): %w", pg, err)
	}
	return nil
}

func (s *AuthService) DetachPolicyFromGroupNoValidation(ctx context.Context, policyDisplayName, groupDisplayName string) error {
	pg := model.GroupPolicyPath(groupDisplayName, policyDisplayName)
	err := s.store.Delete(ctx, []byte(model.PartitionKey), pg)
	if err != nil {
		return fmt.Errorf("policy detachment to group: (key %s): %w", pg, err)
	}
	return nil
}

func (s *AuthService) DetachPolicyFromGroup(ctx context.Context, policyDisplayName, groupDisplayName string) error {
	if _, err := s.GetGroup(ctx, groupDisplayName); err != nil {
		return err
	}
	if _, err := s.GetPolicy(ctx, policyDisplayName); err != nil {
		return err
	}
	return s.DetachPolicyFromGroupNoValidation(ctx, policyDisplayName, groupDisplayName)
}

func (s *AuthService) GetCredentialsForUser(ctx context.Context, username, accessKeyID string) (*model.Credential, error) {
	if _, err := s.GetUser(ctx, username); err != nil {
		return nil, err
	}
	credentialsKey := model.CredentialPath(username, accessKeyID)
	m := model.CredentialData{}
	_, err := kv.GetMsg(ctx, s.store, model.PartitionKey, credentialsKey, &m)
	if err != nil {
		if errors.Is(err, kv.ErrNotFound) {
			err = ErrNotFound
		}
		return nil, err
	}

	c, err := model.CredentialFromProto(s.secretStore, &m)
	if err != nil {
		return nil, err
	}
	c.SecretAccessKey = ""
	return c, nil
}

func (s *AuthService) GetCredentials(ctx context.Context, accessKeyID string) (*model.Credential, error) {
	return s.cache.GetCredential(accessKeyID, func() (*model.Credential, error) {
		m := &model.UserData{}
		itr, err := kv.NewPrimaryIterator(ctx, s.store, m.ProtoReflect().Type(), model.PartitionKey, model.UserPath(""), kv.IteratorOptionsAfter([]byte("")))
		if err != nil {
			return nil, fmt.Errorf("scan users: %w", err)
		}
		defer itr.Close()

		for itr.Next() {
			entry := itr.Entry()
			user, ok := entry.Value.(*model.UserData)
			if !ok {
				return nil, fmt.Errorf("failed to cast: %w", err)
			}
			c := model.CredentialData{}
			credentialsKey := model.CredentialPath(user.Username, accessKeyID)
			_, err := kv.GetMsg(ctx, s.store, model.PartitionKey, credentialsKey, &c)
			if err != nil && !errors.Is(err, kv.ErrNotFound) {
				return nil, err
			}
			if err == nil {
				return model.CredentialFromProto(s.secretStore, &c)
			}
		}
		if err = itr.Err(); err != nil {
			return nil, err
		}
		return nil, fmt.Errorf("credentials %w", ErrNotFound)
	})
}

func interpolateUser(resource string, username string) string {
	return strings.ReplaceAll(resource, "${user}", username)
}

func checkPermissions(ctx context.Context, node permissions.Node, username string, policies []*model.Policy) CheckResult {
	allowed := CheckNeutral
	switch node.Type {
	case permissions.NodeTypeNode:
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
						return CheckDeny
					}

					allowed = CheckAllow
				}
			}
		}

	case permissions.NodeTypeOr:
		// returns:
		// Allowed - at least one of the permissions is allowed and no one is denied
		// Denied - one of the permissions is Deny
		// Natural - otherwise
		for _, node := range node.Nodes {
			result := checkPermissions(ctx, node, username, policies)
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
			result := checkPermissions(ctx, node, username, policies)
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

func (s *AuthService) Authorize(ctx context.Context, req *AuthorizationRequest) (*AuthorizationResponse, error) {
	policies, _, err := s.ListEffectivePolicies(ctx, req.Username, &model.PaginationParams{
		After:  "", // all
		Amount: -1, // all
	})
	if err != nil {
		return nil, err
	}

	allowed := checkPermissions(ctx, req.RequiredPermissions, req.Username, policies)

	if allowed != CheckAllow {
		return &AuthorizationResponse{
			Allowed: false,
			Error:   ErrInsufficientPermissions,
		}, nil
	}

	// we're allowed!
	return &AuthorizationResponse{Allowed: true}, nil
}

func (s *AuthService) ClaimTokenIDOnce(ctx context.Context, tokenID string, expiresAt int64) error {
	return claimTokenIDOnce(ctx, tokenID, expiresAt, s.markTokenSingleUse)
}

func claimTokenIDOnce(ctx context.Context, tokenID string, expiresAt int64, markTokenSingleUse func(context.Context, string, time.Time) (bool, error)) error {
	tokenExpiresAt := time.Unix(expiresAt, 0)
	canUseToken, err := markTokenSingleUse(ctx, tokenID, tokenExpiresAt)
	if err != nil {
		return err
	}
	if !canUseToken {
		return ErrInvalidToken
	}
	return nil
}

func (s *AuthService) IsExternalPrincipalsEnabled(ctx context.Context) bool {
	return false
}

func (s *AuthService) CreateUserExternalPrincipal(ctx context.Context, userID, principalID string) error {
	return ErrNotImplemented
}

func (s *AuthService) DeleteUserExternalPrincipal(ctx context.Context, userID, principalID string) error {
	return ErrNotImplemented
}

func (s *AuthService) GetExternalPrincipal(ctx context.Context, principalID string) (*model.ExternalPrincipal, error) {
	return nil, ErrNotImplemented
}

func (s *AuthService) ListUserExternalPrincipals(ctx context.Context, userID string, params *model.PaginationParams) ([]*model.ExternalPrincipal, *model.Paginator, error) {
	return nil, nil, ErrNotImplemented
}

// markTokenSingleUse returns true if token is valid for single use
func (s *AuthService) markTokenSingleUse(ctx context.Context, tokenID string, tokenExpiresAt time.Time) (bool, error) {
	tokenPath := model.ExpiredTokenPath(tokenID)
	m := model.TokenData{TokenId: tokenID, ExpiredAt: timestamppb.New(tokenExpiresAt)}
	err := kv.SetMsgIf(ctx, s.store, model.PartitionKey, tokenPath, &m, nil)
	if err != nil {
		if errors.Is(err, kv.ErrPredicateFailed) {
			return false, nil
		}
		return false, err
	}

	if err := s.deleteTokens(ctx); err != nil {
		s.log.WithError(err).Error("Failed to delete expired tokens")
	}
	return true, nil
}

func (s *AuthService) deleteTokens(ctx context.Context) error {
	it, err := kv.NewPrimaryIterator(ctx, s.store, (&model.TokenData{}).ProtoReflect().Type(), model.PartitionKey, model.ExpiredTokensPath(), kv.IteratorOptionsFrom([]byte("")))
	if err != nil {
		return err
	}
	defer it.Close()

	deletionCutoff := time.Now()
	for it.Next() {
		msg := it.Entry()
		if msg == nil {
			return fmt.Errorf("nil token: %w", ErrInvalidToken)
		}
		token, ok := msg.Value.(*model.TokenData)
		if token == nil || !ok {
			return fmt.Errorf("wrong token type: %w", ErrInvalidToken)
		}

		if token.ExpiredAt.AsTime().After(deletionCutoff) {
			// reached a token with expiry greater than the cutoff,
			// tokens are k-ordered (xid) hence we'll not find more expired tokens
			return nil
		}

		tokenPath := model.ExpiredTokenPath(token.TokenId)
		if err := s.store.Delete(ctx, []byte(model.PartitionKey), tokenPath); err != nil {
			return fmt.Errorf("deleting token: %w", err)
		}
	}

	return it.Err()
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

func (a *APIAuthService) getFirstUser(ctx context.Context, userKey userKey, params *ListUsersParams) (*model.User, error) {
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
	intID, err := userIDToInt(userID)
	if err != nil {
		return nil, fmt.Errorf("userID as int64: %w", err)
	}
	return a.getFirstUser(ctx, userKey{id: userID}, &ListUsersParams{Id: &intID})
}

func (a *APIAuthService) GetUser(ctx context.Context, username string) (*model.User, error) {
	return a.cache.GetUser(userKey{username: username}, func() (*model.User, error) {
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
	return a.getFirstUser(ctx, userKey{email: email}, &ListUsersParams{Email: swag.String(email)})
}

func (a *APIAuthService) GetUserByExternalID(ctx context.Context, externalID string) (*model.User, error) {
	return a.getFirstUser(ctx, userKey{externalID: externalID}, &ListUsersParams{ExternalId: swag.String(externalID)})
}

func toPagination(paginator Pagination) *model.Paginator {
	return &model.Paginator{
		Amount:        paginator.Results,
		NextPageToken: paginator.NextOffset,
	}
}

func (a *APIAuthService) ListUsers(ctx context.Context, params *model.PaginationParams) ([]*model.User, *model.Paginator, error) {
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
	resp, err := a.apiClient.DeleteGroupWithResponse(ctx, groupID)
	if err != nil {
		a.logger.WithError(err).WithField("group", groupID).Error("failed to delete group")
		return err
	}
	return a.validateResponse(resp, http.StatusNoContent)
}

func (a *APIAuthService) GetGroup(ctx context.Context, groupID string) (*model.Group, error) {
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
	resp, err := a.apiClient.AddGroupMembershipWithResponse(ctx, groupID, username)
	if err != nil {
		a.logger.WithError(err).WithField("group", groupID).WithField("username", username).Error("failed to add user to group")
		return err
	}
	return a.validateResponse(resp, http.StatusCreated)
}

func (a *APIAuthService) RemoveUserFromGroup(ctx context.Context, username, groupID string) error {
	resp, err := a.apiClient.DeleteGroupMembershipWithResponse(ctx, groupID, username)
	if err != nil {
		a.logger.WithError(err).WithField("group", groupID).WithField("username", username).Error("failed to remove user from group")
		return err
	}
	return a.validateResponse(resp, http.StatusNoContent)
}

func (a *APIAuthService) ListUserGroups(ctx context.Context, username string, params *model.PaginationParams) ([]*model.Group, *model.Paginator, error) {
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

	if update {
		// Update existing policy
		resp, err := a.apiClient.UpdatePolicyWithResponse(ctx, policy.DisplayName, UpdatePolicyJSONRequestBody{
			CreationDate: &createdAt,
			Name:         policy.DisplayName,
			Statement:    stmts,
		})
		if err != nil {
			a.logger.WithError(err).WithField("policy", policy.DisplayName).Error("failed to update policy")
			return err
		}
		return a.validateResponse(resp, http.StatusOK)
	}
	// Otherwise Create new
	resp, err := a.apiClient.CreatePolicyWithResponse(ctx, CreatePolicyJSONRequestBody{
		CreationDate: &createdAt,
		Name:         policy.DisplayName,
		Statement:    stmts,
	})
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
	}
}

func (a *APIAuthService) GetPolicy(ctx context.Context, policyDisplayName string) (*model.Policy, error) {
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
	resp, err := a.apiClient.DeletePolicyWithResponse(ctx, policyDisplayName)
	if err != nil {
		a.logger.WithError(err).WithField("policy", policyDisplayName).Error("failed to delete policy")
		return err
	}
	return a.validateResponse(resp, http.StatusNoContent)
}

func (a *APIAuthService) ListPolicies(ctx context.Context, params *model.PaginationParams) ([]*model.Policy, *model.Paginator, error) {
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
	resp, err := a.apiClient.DeleteCredentialsWithResponse(ctx, username, accessKeyID)
	if err != nil {
		a.logger.WithError(err).WithField("username", username).Error("failed to delete credentials")
		return err
	}
	return a.validateResponse(resp, http.StatusNoContent)
}

func (a *APIAuthService) GetCredentialsForUser(ctx context.Context, username, accessKeyID string) (*model.Credential, error) {
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
	resp, err := a.apiClient.AttachPolicyToUserWithResponse(ctx, username, policyDisplayName)
	if err != nil {
		a.logger.WithError(err).WithField("username", username).Error("failed to attach policy to user")
		return err
	}
	return a.validateResponse(resp, http.StatusCreated)
}

func (a *APIAuthService) DetachPolicyFromUser(ctx context.Context, policyDisplayName, username string) error {
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
	return a.listUserPolicies(ctx, username, params, false)
}

func (a *APIAuthService) listAllEffectivePolicies(ctx context.Context, username string) ([]*model.Policy, error) {
	hasMore := true
	after := ""
	amount := maxPage
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
	policies, _, err := a.ListEffectivePolicies(ctx, req.Username, &model.PaginationParams{
		After:  "", // all
		Amount: -1, // all
	})
	if err != nil {
		return nil, err
	}

	allowed := checkPermissions(ctx, req.RequiredPermissions, req.Username, policies)

	if allowed != CheckAllow {
		return &AuthorizationResponse{
			Allowed: false,
			Error:   ErrInsufficientPermissions,
		}, nil
	}

	// we're allowed!
	return &AuthorizationResponse{Allowed: true}, nil
}

func (a *APIAuthService) ClaimTokenIDOnce(ctx context.Context, tokenID string, expiresAt int64) error {
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
	logger.Info("perform health check, this can take up to ", timeout)
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

func (a *APIAuthService) IsExternalPrincipalsEnabled(ctx context.Context) bool {
	return a.externalPrincipalsEnabled
}

func (a *APIAuthService) CreateUserExternalPrincipal(ctx context.Context, userID, principalID string) error {
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
		token, err = generateAuthAPIJWT(secretStore.SharedSecret())
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
func generateAuthAPIJWT(secret []byte) (string, error) {
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
	logging.ContextUnavailable().WithField("id", id).Info("generated auth api token")
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
