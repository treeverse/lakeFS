package auth

//go:generate oapi-codegen -package auth -generate "types,client"  -o client.gen.go ../../api/authorization.yml

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"time"

	sq "github.com/Masterminds/squirrel"
	"github.com/deepmap/oapi-codegen/pkg/securityprovider"
	"github.com/getkin/kin-openapi/openapi3filter"
	"github.com/go-openapi/swag"
	"github.com/google/uuid"
	"github.com/treeverse/lakefs/pkg/auth/crypt"
	"github.com/treeverse/lakefs/pkg/auth/keys"
	"github.com/treeverse/lakefs/pkg/auth/model"
	"github.com/treeverse/lakefs/pkg/auth/params"
	"github.com/treeverse/lakefs/pkg/auth/wildcard"
	"github.com/treeverse/lakefs/pkg/kv"
	"github.com/treeverse/lakefs/pkg/logging"
	"github.com/treeverse/lakefs/pkg/permissions"
	"golang.org/x/crypto/bcrypt"
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

const InvalidUserID = "-1"

type GatewayService interface {
	GetCredentials(_ context.Context, accessKey string) (*model.Credential, error)
	GetUserByID(ctx context.Context, userID string) (*model.User, error)
	Authorize(_ context.Context, req *AuthorizationRequest) (*AuthorizationResponse, error)
}

type Service interface {
	SecretStore() crypt.SecretStore

	// users
	CreateUser(ctx context.Context, user *model.BaseUser) (string, error)
	DeleteUser(ctx context.Context, username string) error
	GetUserByID(ctx context.Context, userID string) (*model.User, error)
	GetUser(ctx context.Context, username string) (*model.User, error)
	GetUserByEmail(ctx context.Context, email string) (*model.User, error)
	ListUsers(ctx context.Context, params *model.PaginationParams) ([]*model.User, *model.Paginator, error)

	// groups
	CreateGroup(ctx context.Context, group *model.BaseGroup) error
	DeleteGroup(ctx context.Context, groupDisplayName string) error
	GetGroup(ctx context.Context, groupDisplayName string) (*model.Group, error)
	ListGroups(ctx context.Context, params *model.PaginationParams) ([]*model.Group, *model.Paginator, error)

	// group<->user memberships
	AddUserToGroup(ctx context.Context, username, groupDisplayName string) error
	RemoveUserFromGroup(ctx context.Context, username, groupDisplayName string) error
	ListUserGroups(ctx context.Context, username string, params *model.PaginationParams) ([]*model.Group, *model.Paginator, error)
	ListGroupUsers(ctx context.Context, groupDisplayName string, params *model.PaginationParams) ([]*model.User, *model.Paginator, error)

	// policies
	WritePolicy(ctx context.Context, policy *model.BasePolicy) error
	GetPolicy(ctx context.Context, policyDisplayName string) (*model.BasePolicy, error)
	DeletePolicy(ctx context.Context, policyDisplayName string) error
	ListPolicies(ctx context.Context, params *model.PaginationParams) ([]*model.BasePolicy, *model.Paginator, error)

	// credentials
	CreateCredentials(ctx context.Context, username string) (*model.Credential, error)
	AddCredentials(ctx context.Context, username, accessKeyID, secretAccessKey string) (*model.Credential, error)
	DeleteCredentials(ctx context.Context, username, accessKeyID string) error
	GetCredentialsForUser(ctx context.Context, username, accessKeyID string) (*model.Credential, error)
	GetCredentials(ctx context.Context, accessKeyID string) (*model.Credential, error)
	ListUserCredentials(ctx context.Context, username string, params *model.PaginationParams) ([]*model.Credential, *model.Paginator, error)
	HashAndUpdatePassword(ctx context.Context, username string, password string) error

	// policy<->user attachments
	AttachPolicyToUser(ctx context.Context, policyDisplayName, username string) error
	DetachPolicyFromUser(ctx context.Context, policyDisplayName, username string) error
	ListUserPolicies(ctx context.Context, username string, params *model.PaginationParams) ([]*model.BasePolicy, *model.Paginator, error)
	ListEffectivePolicies(ctx context.Context, username string, params *model.PaginationParams) ([]*model.BasePolicy, *model.Paginator, error)

	// policy<->group attachments
	AttachPolicyToGroup(ctx context.Context, policyDisplayName, groupDisplayName string) error
	DetachPolicyFromGroup(ctx context.Context, policyDisplayName, groupDisplayName string) error
	ListGroupPolicies(ctx context.Context, groupDisplayName string, params *model.PaginationParams) ([]*model.BasePolicy, *model.Paginator, error)

	// authorize user for an action
	Authorize(ctx context.Context, req *AuthorizationRequest) (*AuthorizationResponse, error)

	ClaimTokenIDOnce(ctx context.Context, tokenID string, expiresAt int64) error
}

var psql = sq.StatementBuilder.PlaceholderFormat(sq.Dollar)

// fieldNameByTag returns the name of the field of t that is tagged tag on key, or an empty string.
func fieldByTag(t reflect.Type, key, tag string) string {
	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		if field.Type.Kind() == reflect.Struct && field.Anonymous {
			if innerRes := fieldByTag(field.Type, key, tag); innerRes != "" {
				return innerRes
			}
			continue
		}

		if l, ok := field.Tag.Lookup(key); ok {
			if l == tag {
				return field.Name
			}
		}
	}
	return ""
}

const maxPage = 1000

func (s *KVAuthService) ListKVPaged(ctx context.Context, protoType protoreflect.MessageType, params *model.PaginationParams, prefix string, secondary bool) ([]proto.Message, *model.Paginator, error) {
	amount := maxPage
	var it kv.MessageIterator
	var err error
	if params != nil {
		if secondary {
			it, err = kv.NewSecondaryIterator(ctx, s.store.Store, protoType, model.PartitionKey, prefix, params.After)
		} else {
			it, err = kv.NewPrimaryIterator(ctx, s.store.Store, protoType, model.PartitionKey, prefix, params.After)
		}
		if err != nil {
			return nil, nil, fmt.Errorf("sacn prefix(%s): %w", prefix, err)
		}
		defer it.Close()
		if params.Amount >= 0 && params.Amount < maxPage {
			amount = params.Amount
		}
	}

	var entries []proto.Message
	p := &model.Paginator{}
	for len(entries) <= amount && it.Next() {
		entry := it.Entry()
		value := entry.Value
		if len(entries) == amount {
			p.NextPageToken = entry.Key
			break
		}
		entries = append(entries, value)
	}
	if err = it.Err(); err != nil {
		return nil, nil, fmt.Errorf("list DB: %w", err)
	}
	p.Amount = len(entries)
	return entries, p, nil
}

type KVAuthService struct {
	store       kv.StoreMessage
	secretStore crypt.SecretStore
	cache       Cache
	log         logging.Logger
}

func NewKVAuthService(store kv.StoreMessage, secretStore crypt.SecretStore, cacheConf params.ServiceCache, logger logging.Logger) *KVAuthService {
	logger.Info("initialized Auth service")
	var cache Cache
	if cacheConf.Enabled {
		cache = NewLRUCache(cacheConf.Size, cacheConf.TTL, cacheConf.EvictionJitter)
	} else {
		cache = &DummyCache{}
	}
	return &KVAuthService{
		store:       store,
		secretStore: secretStore,
		cache:       cache,
		log:         logger,
	}
}

func encryptSecret(s crypt.SecretStore, secretAccessKey string) ([]byte, error) {
	encrypted, err := s.Encrypt([]byte(secretAccessKey))
	if err != nil {
		return nil, err
	}
	return encrypted, nil
}

func (s *KVAuthService) SecretStore() crypt.SecretStore {
	return s.secretStore
}

func (s *KVAuthService) CreateUser(ctx context.Context, user *model.BaseUser) (string, error) {
	if err := model.ValidateAuthEntityID(user.Username); err != nil {
		return InvalidUserID, err
	}
	userKey := model.KVUserPath(user.Username)
	id := uuid.New().String()
	userWithID := model.User{ID: id, BaseUser: *user}

	err := s.store.SetMsgIf(ctx, model.PartitionKey, userKey, model.ProtoFromUser(&userWithID), nil)
	if err != nil {
		if errors.Is(err, kv.ErrPredicateFailed) {
			err = ErrAlreadyExists
		}
		return "", fmt.Errorf("save user (usesrKey %s): %w", userKey, err)
	}
	return fmt.Sprint(id), err
}

func (s *KVAuthService) DeleteUser(ctx context.Context, username string) error {
	var err error
	if _, err = s.GetUser(ctx, username); err != nil {
		return fmt.Errorf("%s: %w", username, err)
	}
	userPath := model.KVUserPath(username)

	// delete policy attached to user
	policiesKey := model.KVPolicyToUser(username, "")
	it, err := kv.NewSecondaryIterator(ctx, s.store.Store, (&model.PolicyData{}).ProtoReflect().Type(), model.PartitionKey, policiesKey, "")
	if err != nil {
		return err
	}
	defer it.Close()
	for it.Next() {
		entry := it.Entry()
		policy := entry.Value.(*model.PolicyData)
		if err = s.DetachPolicyFromUser(ctx, policy.DisplayName, username); err != nil {
			return fmt.Errorf("%s: %w", policy.DisplayName, err)
		}
	}
	if err = it.Err(); err != nil {
		return err
	}

	// delete user membership of group
	groupKey := model.KVGroupPath("")
	itr, err := kv.NewPrimaryIterator(ctx, s.store.Store, (&model.GroupData{}).ProtoReflect().Type(), model.PartitionKey, groupKey, "")
	if err != nil {
		return err
	}

	for itr.Next() {
		entry := itr.Entry()
		group := entry.Value.(*model.GroupData)
		if err = s.RemoveUserFromGroup(ctx, username, group.DisplayName); err != nil {
			return fmt.Errorf("%s: %w", username, err)
		}
	}
	if err = itr.Err(); err != nil {
		return err
	}
	defer itr.Close()

	// delete user
	err = s.store.DeleteMsg(ctx, model.PartitionKey, userPath)
	if err != nil {
		return fmt.Errorf("delete user (userKey %s): %w", userPath, err)
	}
	return err
}

func (s *KVAuthService) GetUser(ctx context.Context, username string) (*model.User, error) {
	return s.cache.GetUser(username, func() (*model.User, error) {
		userKey := model.KVUserPath(username)
		m := model.UserData{}
		_, err := s.store.GetMsg(ctx, model.PartitionKey, userKey, &m)
		if err != nil {
			if errors.Is(err, kv.ErrNotFound) {
				err = ErrNotFound // Wrap error for compatibility with DBService
			}
			return nil, err
		}
		return model.UserFromProto(&m), nil
	})
}

func (s *KVAuthService) GetUserByEmail(ctx context.Context, email string) (*model.User, error) {
	m := &model.UserData{}
	itr, err := s.store.Scan(ctx, m.ProtoReflect().Type(), model.PartitionKey, model.KVUserPath(""))
	if err != nil {
		return nil, fmt.Errorf("sacn users: %w", err)
	}
	defer itr.Close()

	for itr.Next() {
		if itr.Err() != nil {
			return nil, itr.Err()
		}
		entry := itr.Entry()
		value, ok := entry.Value.(*model.UserData)
		if !ok {
			return nil, fmt.Errorf("list users: %w", err)
		}
		if value.Email == email {
			return model.UserFromProto(value), nil
		}
	}
	return nil, ErrNotFound
}

func (s *KVAuthService) GetUserByID(ctx context.Context, userID string) (*model.User, error) {
	return s.cache.GetUserByID(userID, func() (*model.User, error) {
		m := &model.UserData{}
		itr, err := s.store.Scan(ctx, m.ProtoReflect().Type(), model.PartitionKey, model.KVUserPath(""))
		if err != nil {
			return nil, fmt.Errorf("sacn users: %w", err)
		}
		defer itr.Close()

		for itr.Next() {
			entry := itr.Entry()
			value, ok := entry.Value.(*model.UserData)
			if !ok {
				return nil, fmt.Errorf("list users: %w", err)
			}
			if string(value.Id) == userID {
				return model.UserFromProto(value), nil
			}
		}
		if err = itr.Err(); err != nil {
			return nil, err
		}
		return nil, ErrNotFound
	})
}

func (s *KVAuthService) ListUsers(ctx context.Context, params *model.PaginationParams) ([]*model.User, *model.Paginator, error) {
	var user model.UserData
	usersKey := model.KVUserPath("")

	msgs, paginator, err := s.ListKVPaged(ctx, (&user).ProtoReflect().Type(), params, usersKey, false)
	if msgs == nil {
		return nil, paginator, err
	}
	return model.ConvertUsersDataList(msgs), paginator, err
}

func (s *KVAuthService) ListUserCredentials(ctx context.Context, username string, params *model.PaginationParams) ([]*model.Credential, *model.Paginator, error) {
	var credential model.CredentialData
	credentialsKey := model.KVCredentialPath(username, "")

	msgs, paginator, err := s.ListKVPaged(ctx, (&credential).ProtoReflect().Type(), params, credentialsKey, false)
	if msgs == nil {
		return nil, paginator, err
	}
	return model.ConvertCredDataList(msgs), paginator, err
}

func (s *KVAuthService) AttachPolicyToUser(ctx context.Context, policyDisplayName string, username string) error {
	if _, err := s.GetUser(ctx, username); err != nil {
		return fmt.Errorf("%s: %w", username, err)
	}
	if _, err := s.GetPolicy(ctx, policyDisplayName); err != nil {
		return fmt.Errorf("%s: %w", policyDisplayName, err)
	}

	policyKey := model.KVPolicyPath(policyDisplayName)
	pu := model.KVPolicyToUser(username, policyDisplayName)

	err := s.store.SetMsgIf(ctx, model.PartitionKey, pu, &kv.SecondaryIndex{PrimaryKey: []byte(policyKey)}, nil)
	if err != nil {
		if errors.Is(err, kv.ErrPredicateFailed) {
			err = ErrAlreadyExists
		}
		return fmt.Errorf("policy attachment to user: (key %s): %w", pu, err)
	}
	return nil
}

func (s *KVAuthService) DetachPolicyFromUser(ctx context.Context, policyDisplayName, username string) error {
	if _, err := s.GetUser(ctx, username); err != nil {
		return fmt.Errorf("%s: %w", username, err)
	}
	if _, err := s.GetPolicy(ctx, policyDisplayName); err != nil {
		return fmt.Errorf("detaching policy: %w", err)
	}

	pu := model.KVPolicyToUser(username, policyDisplayName)
	err := s.store.DeleteMsg(ctx, model.PartitionKey, pu)
	if err != nil {
		return fmt.Errorf("policy detachment to user: (key %s): %w", pu, err)
	}
	return nil
}

func (s *KVAuthService) ListUserPolicies(ctx context.Context, username string, params *model.PaginationParams) ([]*model.BasePolicy, *model.Paginator, error) {
	var policy model.PolicyData
	userPolicyKey := model.KVPolicyToUser(username, "")

	msgs, paginator, err := s.ListKVPaged(ctx, (&policy).ProtoReflect().Type(), params, userPolicyKey, true)
	if msgs == nil {
		return nil, paginator, err
	}
	return model.ConvertPolicyDataList(msgs), paginator, err
}

func (s *KVAuthService) getEffectivePolicies(ctx context.Context, username string, params *model.PaginationParams) ([]*model.BasePolicy, *model.Paginator, error) {
	if _, err := s.GetUser(ctx, username); err != nil {
		return nil, nil, fmt.Errorf("%s: %w", username, err)
	}

	hasMoreUserPolicy := true
	after := ""
	amount := maxPage
	policiesSet := make(map[string]*model.BasePolicy)
	var policiesArr []string
	// get policies attracted to user
	for hasMoreUserPolicy {
		policies, userPaginator, err := s.ListUserPolicies(ctx, username, &model.PaginationParams{
			After:  after,
			Amount: amount,
		})
		if err != nil {
			return nil, nil, fmt.Errorf("list user policies: %w", err)
		}
		for _, policy := range policies {
			policiesSet[policy.DisplayName] = policy
			policiesArr = append(policiesArr, policy.DisplayName)
		}
		after = userPaginator.NextPageToken
		hasMoreUserPolicy = userPaginator.NextPageToken != ""
	}

	hasMoreGroup := true
	hasMoreGroupPolicy := true
	after = ""
	for hasMoreGroup {
		// get membership groups to user
		groups, groupPaginator, err := s.ListGroups(ctx, &model.PaginationParams{
			After:  after,
			Amount: amount,
		})
		if err != nil {
			return nil, nil, err
		}
		for _, group := range groups {
			// get policies attracted to group
			for hasMoreGroupPolicy {
				groupPolicies, groupPoliciesPaginator, err := s.ListGroupPolicies(ctx, group.DisplayName, &model.PaginationParams{
					After:  after,
					Amount: amount,
				})
				if err != nil {
					return nil, nil, fmt.Errorf("list group policies: %w", err)
				}
				for _, policy := range groupPolicies {
					policiesSet[policy.DisplayName] = policy
					policiesArr = append(policiesArr, policy.DisplayName)
				}
				after = groupPoliciesPaginator.NextPageToken
				hasMoreGroupPolicy = groupPoliciesPaginator.NextPageToken != ""
			}
		}
		after = groupPaginator.NextPageToken
		hasMoreGroup = groupPaginator.NextPageToken != ""
	}

	if params.Amount < 0 || params.Amount > maxPage {
		params.Amount = maxPage
	}
	sort.Strings(policiesArr)
	var resPolicies []*model.BasePolicy
	var resPaginator *model.Paginator
	for _, p := range policiesArr {
		if p > after {
			if len(resPolicies) == params.Amount {
				resPaginator.NextPageToken = p
				resPaginator.Amount = len(resPolicies)
				return resPolicies, resPaginator, nil
			}
			resPolicies = append(resPolicies, policiesSet[p])
		}
	}
	return resPolicies, resPaginator, nil
}

func (s *KVAuthService) ListEffectivePolicies(ctx context.Context, username string, params *model.PaginationParams) ([]*model.BasePolicy, *model.Paginator, error) {
	if params.Amount == -1 {
		// read through the cache when requesting the full list
		policies, err := s.cache.GetUserPolicies(username, func() ([]*model.BasePolicy, error) {
			policies, _, err := s.getEffectivePolicies(ctx, username, params)
			return policies, err
		})
		if err != nil {
			return nil, nil, err
		}
		return policies, &model.Paginator{Amount: len(policies)}, nil
	}

	return s.getEffectivePolicies(ctx, username, params)
}

func (s *KVAuthService) ListGroupPolicies(ctx context.Context, groupDisplayName string, params *model.PaginationParams) ([]*model.BasePolicy, *model.Paginator, error) {
	var policy model.PolicyData
	groupPolicyKey := model.KVPolicyToGroup(groupDisplayName, "")

	msgs, paginator, err := s.ListKVPaged(ctx, (&policy).ProtoReflect().Type(), params, groupPolicyKey, true)
	if msgs == nil {
		return nil, paginator, err
	}
	return model.ConvertPolicyDataList(msgs), paginator, err
}

func (s *KVAuthService) CreateGroup(ctx context.Context, group *model.BaseGroup) error {
	if err := model.ValidateAuthEntityID(group.DisplayName); err != nil {
		return err
	}

	groupKey := model.KVGroupPath(group.DisplayName)
	id := uuid.New().String()
	groupWithID := model.Group{ID: id, BaseGroup: *group}

	err := s.store.SetMsgIf(ctx, model.PartitionKey, groupKey, model.ProtoFromGroup(&groupWithID), nil)
	if err != nil {
		if errors.Is(err, kv.ErrPredicateFailed) {
			err = ErrAlreadyExists
		}
		return fmt.Errorf("save group (groupKey %s): %w", groupKey, err)
	}
	return err
}

func (s *KVAuthService) DeleteGroup(ctx context.Context, groupDisplayName string) error {
	var err error
	if _, err = s.GetGroup(ctx, groupDisplayName); err != nil {
		return fmt.Errorf("%s: %w", groupDisplayName, err)
	}

	// delete user membership to group
	usersKey := model.KVUserToGroup(groupDisplayName, "")
	it, err := kv.NewSecondaryIterator(ctx, s.store.Store, (&model.UserData{}).ProtoReflect().Type(), model.PartitionKey, usersKey, "")
	if err != nil {
		return err
	}
	defer it.Close()
	for it.Next() {
		entry := it.Entry()
		user := entry.Value.(*model.UserData)
		if err = s.RemoveUserFromGroup(ctx, user.Username, groupDisplayName); err != nil {
			return fmt.Errorf("%s: %w", user.Username, err)
		}
	}
	if err = it.Err(); err != nil {
		return err
	}

	// delete policy attachment to group
	policiesKey := model.KVPolicyToGroup(groupDisplayName, "")
	itr, err := kv.NewSecondaryIterator(ctx, s.store.Store, (&model.PolicyData{}).ProtoReflect().Type(), model.PartitionKey, policiesKey, "")
	if err != nil {
		return err
	}
	defer it.Close()
	for itr.Next() {
		entry := itr.Entry()
		policy := entry.Value.(*model.PolicyData)
		if err = s.DetachPolicyFromGroup(ctx, policy.DisplayName, groupDisplayName); err != nil {
			return fmt.Errorf("%s: %w", policy.DisplayName, err)
		}
	}
	if err = itr.Err(); err != nil {
		return err
	}

	// delete group
	groupPath := model.KVGroupPath(groupDisplayName)
	err = s.store.DeleteMsg(ctx, model.PartitionKey, groupPath)
	if err != nil {
		return fmt.Errorf("delete user (usesrKey %s): %w", groupPath, err)
	}
	return err
}

func (s *KVAuthService) GetGroup(ctx context.Context, groupDisplayName string) (*model.Group, error) {
	groupKey := model.KVGroupPath(groupDisplayName)
	m := model.GroupData{}
	_, err := s.store.GetMsg(ctx, model.PartitionKey, groupKey, &m)
	if err != nil {
		if errors.Is(err, kv.ErrNotFound) {
			err = ErrNotFound
		}
		return nil, err
	}
	return model.GroupFromProto(&m), nil
}

func (s *KVAuthService) ListGroups(ctx context.Context, params *model.PaginationParams) ([]*model.Group, *model.Paginator, error) {
	var group model.GroupData
	groupKey := model.KVGroupPath("")

	msgs, paginator, err := s.ListKVPaged(ctx, (&group).ProtoReflect().Type(), params, groupKey, false)
	if msgs == nil {
		return nil, paginator, err
	}
	return model.ConvertGroupDataList(msgs), paginator, err
}

func (s *KVAuthService) AddUserToGroup(ctx context.Context, username, groupDisplayName string) error {
	if _, err := s.GetUser(ctx, username); err != nil {
		return fmt.Errorf("%s: %w", username, err)
	}
	if _, err := s.GetGroup(ctx, groupDisplayName); err != nil {
		return fmt.Errorf("%s: %w", groupDisplayName, err)
	}

	userKey := model.KVUserPath(username)
	gu := model.KVUserToGroup(groupDisplayName, username)

	err := s.store.SetMsgIf(ctx, model.PartitionKey, gu, &kv.SecondaryIndex{PrimaryKey: []byte(userKey)}, nil)
	if err != nil {
		if errors.Is(err, kv.ErrPredicateFailed) {
			err = ErrAlreadyExists
		}
		return fmt.Errorf("add user to group: (key %s): %w", gu, err)
	}
	return err
}

func (s *KVAuthService) RemoveUserFromGroup(ctx context.Context, username, groupDisplayName string) error {
	if _, err := s.GetUser(ctx, username); err != nil {
		return fmt.Errorf("%s: %w", username, err)
	}
	if _, err := s.GetGroup(ctx, groupDisplayName); err != nil {
		return fmt.Errorf("%s: %w", groupDisplayName, err)
	}

	gu := model.KVUserToGroup(groupDisplayName, username)
	err := s.store.DeleteMsg(ctx, model.PartitionKey, gu)
	if err != nil {
		return fmt.Errorf("remove user from group: (key %s): %w", gu, err)
	}
	return err
}

func (s *KVAuthService) ListUserGroups(ctx context.Context, username string, params *model.PaginationParams) ([]*model.Group, *model.Paginator, error) {
	if _, err := s.GetUser(ctx, username); err != nil {
		return nil, nil, fmt.Errorf("%s: %w", username, err)
	}

	hasMoreGroups := true
	nextPage := ""
	count := 0
	var userGroups []*model.Group
	for hasMoreGroups && count < params.Amount {
		groups, paginator, err := s.ListGroups(ctx, params)
		if err != nil {
			return nil, nil, err
		}
		for _, group := range groups {
			path := model.KVUserToGroup(group.DisplayName, username)
			m := model.UserData{}
			_, err := s.store.GetMsg(ctx, model.PartitionKey, path, &m)
			if err != nil && !errors.Is(err, kv.ErrNotFound) {
				return nil, nil, err
			}
			if !errors.Is(err, kv.ErrNotFound) {
				userGroups = append(userGroups, group)
				count += 1
			}
		}
		if paginator.NextPageToken == "" {
			hasMoreGroups = false
		}
		params = &model.PaginationParams{
			After: paginator.NextPageToken,
		}
	}
	if count == params.Amount {
		nextPage = params.After
	}
	p := &model.Paginator{Amount: count, NextPageToken: nextPage}
	return userGroups, p, nil
}

func (s *KVAuthService) ListGroupUsers(ctx context.Context, groupDisplayName string, params *model.PaginationParams) ([]*model.User, *model.Paginator, error) {
	if _, err := s.GetGroup(ctx, groupDisplayName); err != nil {
		return nil, nil, fmt.Errorf("%s: %w", groupDisplayName, err)
	}
	var policy model.UserData
	userGroupKey := model.KVUserToGroup(groupDisplayName, "")

	msgs, paginator, err := s.ListKVPaged(ctx, (&policy).ProtoReflect().Type(), params, userGroupKey, true)
	if msgs == nil {
		return nil, paginator, err
	}
	return model.ConvertUsersDataList(msgs), paginator, err
}

func (s *KVAuthService) WritePolicy(ctx context.Context, policy *model.BasePolicy) error {
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
	policyKey := model.KVPolicyPath(policy.DisplayName)
	id := uuid.New().String()

	m := model.ProtoFromPolicy(policy, id)
	err := s.store.SetMsgIf(ctx, model.PartitionKey, policyKey, m, nil)
	if err != nil {
		if errors.Is(err, kv.ErrPredicateFailed) {
			err = ErrAlreadyExists
		}
		return fmt.Errorf("save policy (policyKey %s): %w", policyKey, err)
	}
	return err
}

func (s *KVAuthService) GetPolicy(ctx context.Context, policyDisplayName string) (*model.BasePolicy, error) {
	policyKey := model.KVPolicyPath(policyDisplayName)
	p := model.PolicyData{}
	_, err := s.store.GetMsg(ctx, model.PartitionKey, policyKey, &p)
	if err != nil {
		if errors.Is(err, kv.ErrNotFound) {
			err = ErrNotFound
		}
		return nil, err
	}
	return model.PolicyFromProto(&p), nil
}

func (s *KVAuthService) DeletePolicy(ctx context.Context, policyDisplayName string) error {
	if _, err := s.GetPolicy(ctx, policyDisplayName); err != nil {
		return fmt.Errorf("%s: %w", policyDisplayName, err)
	}
	policyPath := model.KVPolicyPath(policyDisplayName)

	// delete policy attachment to user
	usersKey := model.KVUserPath("")
	it, err := kv.NewPrimaryIterator(ctx, s.store.Store, (&model.UserData{}).ProtoReflect().Type(), model.PartitionKey, usersKey, "")
	if err != nil {
		return err
	}
	defer it.Close()
	for it.Next() {
		entry := it.Entry()
		user := entry.Value.(*model.UserData)
		if err = s.DetachPolicyFromUser(ctx, policyDisplayName, user.Username); err != nil {
			return fmt.Errorf("%s: %w", policyDisplayName, err)
		}
	}

	// delete policy attachment to group
	groupKey := model.KVGroupPath("")
	it, err = kv.NewPrimaryIterator(ctx, s.store.Store, (&model.UserData{}).ProtoReflect().Type(), model.PartitionKey, groupKey, "")
	if err != nil {
		return err
	}
	defer it.Close()
	for it.Next() {
		entry := it.Entry()
		group := entry.Value.(*model.GroupData)
		if err = s.DetachPolicyFromGroup(ctx, policyDisplayName, group.DisplayName); err != nil {
			return fmt.Errorf("%s: %w", policyDisplayName, err)
		}
	}

	// delete policy
	err = s.store.DeleteMsg(ctx, model.PartitionKey, policyPath)
	if err != nil {
		return fmt.Errorf("delete policy (policyKey %s): %w", policyPath, err)
	}
	return err
}

func (s *KVAuthService) ListPolicies(ctx context.Context, params *model.PaginationParams) ([]*model.BasePolicy, *model.Paginator, error) {
	var policy model.PolicyData
	policyKey := model.KVPolicyPath("")

	msgs, paginator, err := s.ListKVPaged(ctx, (&policy).ProtoReflect().Type(), params, policyKey, false)
	if msgs == nil {
		return nil, paginator, err
	}
	return model.ConvertPolicyDataList(msgs), paginator, err
}

func (s *KVAuthService) CreateCredentials(ctx context.Context, username string) (*model.Credential, error) {
	accessKeyID := keys.GenAccessKeyID()
	secretAccessKey := keys.GenSecretAccessKey()
	return s.AddCredentials(ctx, username, accessKeyID, secretAccessKey)
}

func (s *KVAuthService) AddCredentials(ctx context.Context, username, accessKeyID, secretAccessKey string) (*model.Credential, error) {
	if !IsValidAccessKeyID(accessKeyID) {
		return nil, ErrInvalidAccessKeyID
	}
	if len(secretAccessKey) == 0 {
		return nil, ErrInvalidSecretAccessKey
	}
	now := time.Now()
	encryptedKey, err := encryptSecret(s.secretStore, secretAccessKey)
	if err != nil {
		return nil, err
	}
	user, err := s.GetUser(ctx, username)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", username, err)
	}

	c := &model.Credential{
		BaseCredential: model.BaseCredential{
			AccessKeyID:                   accessKeyID,
			SecretAccessKey:               secretAccessKey,
			SecretAccessKeyEncryptedBytes: encryptedKey,
			IssuedDate:                    now,
		},
		UserID: user.ID,
	}
	credentialsKey := model.KVCredentialPath(user.Username, c.AccessKeyID)
	err = s.store.SetMsgIf(ctx, model.PartitionKey, credentialsKey, model.ProtoFromCredential(c), nil)
	if err != nil {
		if errors.Is(err, kv.ErrPredicateFailed) {
			err = ErrAlreadyExists
		}
		return nil, fmt.Errorf("save credentials (credentialsKey %s): %w", credentialsKey, err)
	}

	return c, err
}

func IsValidAccessKeyID(key string) bool {
	l := len(key)
	return l >= 3 && l <= 20
}

func (s *KVAuthService) DeleteCredentials(ctx context.Context, username, accessKeyID string) error {
	if _, err := s.GetUser(ctx, username); err != nil {
		return fmt.Errorf("%s: %w", username, err)
	}
	if _, err := s.GetCredentials(ctx, accessKeyID); err != nil {
		return fmt.Errorf("%s: %w", accessKeyID, err)
	}

	credPath := model.KVCredentialPath(username, accessKeyID)
	err := s.store.DeleteMsg(ctx, model.PartitionKey, credPath)
	if err != nil {
		return fmt.Errorf("delete credentials (credentialsKey %s): %w", credPath, err)
	}
	return err
}

func (s *KVAuthService) AttachPolicyToGroup(ctx context.Context, policyDisplayName, groupDisplayName string) error {
	if _, err := s.GetGroup(ctx, groupDisplayName); err != nil {
		return fmt.Errorf("%s: %w", groupDisplayName, err)
	}
	if _, err := s.GetPolicy(ctx, policyDisplayName); err != nil {
		return fmt.Errorf("%s: %w", policyDisplayName, err)
	}

	policyKey := model.KVPolicyPath(policyDisplayName)
	pg := model.KVPolicyToGroup(groupDisplayName, policyDisplayName)

	err := s.store.SetMsgIf(ctx, model.PartitionKey, pg, &kv.SecondaryIndex{PrimaryKey: []byte(policyKey)}, nil)
	if err != nil {
		if errors.Is(err, kv.ErrPredicateFailed) {
			err = ErrAlreadyExists
		}
		return fmt.Errorf("policy attachment to group: (key %s): %w", pg, err)
	}
	return err
}

func (s *KVAuthService) DetachPolicyFromGroup(ctx context.Context, policyDisplayName, groupDisplayName string) error {
	if _, err := s.GetGroup(ctx, groupDisplayName); err != nil {
		return fmt.Errorf("%s: %w", groupDisplayName, err)
	}
	if _, err := s.GetPolicy(ctx, policyDisplayName); err != nil {
		return fmt.Errorf("%s: %w", policyDisplayName, err)
	}

	pg := model.KVPolicyToGroup(groupDisplayName, policyDisplayName)
	err := s.store.DeleteMsg(ctx, model.PartitionKey, pg)
	if err != nil {
		return fmt.Errorf("policy detachment to group: (key %s): %w", pg, err)
	}
	return err
}

func (s *KVAuthService) GetCredentialsForUser(ctx context.Context, username, accessKeyID string) (*model.Credential, error) {
	if _, err := s.GetUser(ctx, username); err != nil {
		return nil, fmt.Errorf("%s: %w", username, err)
	}
	credentialsKey := model.KVCredentialPath(username, accessKeyID)
	m := model.CredentialData{}
	_, err := s.store.GetMsg(ctx, model.PartitionKey, credentialsKey, &m)
	if err != nil {
		if errors.Is(err, kv.ErrNotFound) {
			err = ErrNotFound
		}
		return nil, err
	}
	m.SecretAccessKey = ""
	return model.CredentialFromProto(&m), nil
}

func (s *KVAuthService) GetCredentials(ctx context.Context, accessKeyID string) (*model.Credential, error) {
	return s.cache.GetCredential(accessKeyID, func() (*model.Credential, error) {
		m := &model.UserData{}
		itr, err := s.store.Scan(ctx, m.ProtoReflect().Type(), model.PartitionKey, model.KVUserPath(""))
		if err != nil {
			return nil, fmt.Errorf("sacn users: %w", err)
		}
		defer itr.Close()

		for itr.Next() {
			if itr.Err() != nil {
				return nil, itr.Err()
			}
			entry := itr.Entry()
			user, ok := entry.Value.(*model.UserData)
			if !ok {
				return nil, fmt.Errorf("list users: %w", err)
			}
			c := model.CredentialData{}
			credentialsKey := model.KVCredentialPath(user.Username, accessKeyID)
			_, err := s.store.GetMsg(ctx, model.PartitionKey, credentialsKey, &c)
			if err != nil && errors.Is(err, kv.ErrNotFound) {
				return nil, err
			} else if !errors.Is(err, kv.ErrNotFound) {
				return model.CredentialFromProto(&c), err
			}
		}
		return nil, ErrNotFound
	})
}

func (s *KVAuthService) HashAndUpdatePassword(ctx context.Context, username string, password string) error {
	user, err := s.GetUser(ctx, username)
	if err != nil {
		return fmt.Errorf("%s: %w", username, err)
	}
	pw, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
	if err != nil {
		return err
	}
	userKey := model.KVUserPath(user.Username)
	userUpdatePassword := model.User{ID: user.ID, BaseUser: model.BaseUser{
		CreatedAt:         user.BaseUser.CreatedAt,
		Username:          user.BaseUser.Username,
		FriendlyName:      user.BaseUser.FriendlyName,
		Email:             user.BaseUser.Email,
		EncryptedPassword: pw,
		Source:            user.BaseUser.Source}}
	err = s.store.SetMsg(ctx, model.PartitionKey, userKey, model.ProtoFromUser(&userUpdatePassword))
	if err != nil {
		return fmt.Errorf("update user password (usesrKey %s): %w", userKey, err)
	}
	return err
}

func interpolateUser(resource string, username string) string {
	return strings.ReplaceAll(resource, "${user}", username)
}

// CheckResult - the final result for the authorization is accepted only if it's CheckAllow
type CheckResult int

const (
	// CheckAllow Permission allowed
	CheckAllow CheckResult = iota
	// CheckNeutral Permission neither allowed nor denied
	CheckNeutral
	// CheckDeny Permission denied
	CheckDeny
)

func checkPermissions(node permissions.Node, username string, policies []*model.BasePolicy) CheckResult {
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
			result := checkPermissions(node, username, policies)
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
			result := checkPermissions(node, username, policies)
			if result == CheckNeutral || result == CheckDeny {
				return result
			}
		}
		return CheckAllow

	default:
		logging.Default().Error("unknown permission node type")
		return CheckDeny
	}
	return allowed
}

func (s *KVAuthService) Authorize(ctx context.Context, req *AuthorizationRequest) (*AuthorizationResponse, error) {
	policies, _, err := s.ListEffectivePolicies(ctx, req.Username, &model.PaginationParams{
		After:  "", // all
		Amount: -1, // all
	})

	if err != nil {
		return nil, err
	}

	allowed := checkPermissions(req.RequiredPermissions, req.Username, policies)

	if allowed != CheckAllow {
		return &AuthorizationResponse{
			Allowed: false,
			Error:   ErrInsufficientPermissions,
		}, nil
	}

	// we're allowed!
	return &AuthorizationResponse{Allowed: true}, nil
}

func (s *KVAuthService) ClaimTokenIDOnce(ctx context.Context, tokenID string, expiresAt int64) error {
	tokenExpiresAt := time.Unix(expiresAt, 0)
	canUseToken, err := s.markTokenSingleUse(ctx, tokenID, tokenExpiresAt)
	if err != nil {
		return err
	}
	if !canUseToken {
		return ErrInvalidToken
	}
	return nil
}

// markTokenSingleUse returns true if token is valid for single use
func (s *KVAuthService) markTokenSingleUse(ctx context.Context, tokenID string, tokenExpiresAt time.Time) (bool, error) {
	tokenPath := model.KVTokenPath(tokenID)
	m := model.TokenData{TokenId: tokenID, ExpiredAt: timestamppb.New(tokenExpiresAt)}
	err := s.store.SetMsgIf(ctx, model.PartitionKey, tokenPath, &m, nil)
	if err != nil {
		return false, err
	}
	// TODO(issue 3500) - delete expired reset password tokens
	return true, nil
}

type APIAuthService struct {
	apiClient   *ClientWithResponses
	secretStore crypt.SecretStore
	cache       Cache
}

func (a *APIAuthService) SecretStore() crypt.SecretStore {
	return a.secretStore
}

func (a *APIAuthService) CreateUser(ctx context.Context, user *model.BaseUser) (string, error) {
	resp, err := a.apiClient.CreateUserWithResponse(ctx, CreateUserJSONRequestBody{
		Email:        user.Email,
		FriendlyName: user.FriendlyName,
		Source:       &user.Source,
		Username:     user.Username,
	})
	if err != nil {
		return InvalidUserID, err
	}
	if err := a.validateResponse(resp, http.StatusCreated); err != nil {
		return InvalidUserID, err
	}

	return fmt.Sprint(resp.JSON201.Id), nil
}

func (a *APIAuthService) DeleteUser(ctx context.Context, username string) error {
	resp, err := a.apiClient.DeleteUserWithResponse(ctx, username)
	if err != nil {
		return err
	}
	return a.validateResponse(resp, http.StatusNoContent)
}

func userIDToInt(userID string) (int64, error) {
	const base, bitSize = 10, 64
	return strconv.ParseInt(userID, base, bitSize)
}

func (a *APIAuthService) GetUserByID(ctx context.Context, userID string) (*model.User, error) {
	intID, err := userIDToInt(userID)
	if err != nil {
		return nil, fmt.Errorf("userID as int64: %w", err)
	}
	return a.cache.GetUserByID(userID, func() (*model.User, error) {
		resp, err := a.apiClient.ListUsersWithResponse(ctx, &ListUsersParams{Id: &intID})
		if err != nil {
			return nil, err
		}
		if err := a.validateResponse(resp, http.StatusOK); err != nil {
			return nil, err
		}
		results := resp.JSON200.Results
		if len(results) == 0 {
			return nil, ErrNotFound
		}
		u := results[0]
		return &model.User{
			ID: model.ConvertDBID(u.Id),
			BaseUser: model.BaseUser{
				CreatedAt:         time.Unix(u.CreationDate, 0),
				Username:          u.Name,
				FriendlyName:      u.FriendlyName,
				Email:             u.Email,
				EncryptedPassword: nil,
				Source:            "",
			},
		}, nil
	})
}

func (a *APIAuthService) GetUser(ctx context.Context, username string) (*model.User, error) {
	return a.cache.GetUser(username, func() (*model.User, error) {
		resp, err := a.apiClient.GetUserWithResponse(ctx, username)
		if err != nil {
			return nil, err
		}
		if err := a.validateResponse(resp, http.StatusOK); err != nil {
			return nil, err
		}
		u := resp.JSON200
		return &model.User{
			ID: model.ConvertDBID(u.Id),
			BaseUser: model.BaseUser{
				CreatedAt:         time.Unix(u.CreationDate, 0),
				Username:          u.Name,
				FriendlyName:      u.FriendlyName,
				Email:             u.Email,
				EncryptedPassword: u.EncryptedPassword,
				Source:            swag.StringValue(u.Source),
			},
		}, nil
	})
}

func (a *APIAuthService) GetUserByEmail(ctx context.Context, email string) (*model.User, error) {
	return a.cache.GetUser(email, func() (*model.User, error) {
		resp, err := a.apiClient.ListUsersWithResponse(ctx, &ListUsersParams{Email: &email})
		if err != nil {
			return nil, err
		}
		if err := a.validateResponse(resp, http.StatusOK); err != nil {
			return nil, err
		}
		results := resp.JSON200.Results
		if len(results) == 0 {
			return nil, ErrNotFound
		}
		u := results[0]
		user := &model.User{
			ID: model.ConvertDBID(u.Id),
			BaseUser: model.BaseUser{
				CreatedAt:         time.Unix(u.CreationDate, 0),
				Username:          u.Name,
				FriendlyName:      u.FriendlyName,
				Email:             u.Email,
				EncryptedPassword: u.EncryptedPassword,
				Source:            swag.StringValue(u.Source),
			},
		}

		return user, err
	})
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
			ID: model.ConvertDBID(r.Id),
			BaseUser: model.BaseUser{
				CreatedAt:         time.Unix(r.CreationDate, 0),
				Username:          r.Name,
				FriendlyName:      r.FriendlyName,
				Email:             r.Email,
				EncryptedPassword: nil,
				Source:            swag.StringValue(r.Source),
			},
		}
	}
	return users, toPagination(pagination), nil
}

func (a *APIAuthService) HashAndUpdatePassword(ctx context.Context, username string, password string) error {
	encryptedPassword, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
	if err != nil {
		return err
	}
	resp, err := a.apiClient.UpdatePasswordWithResponse(ctx, username, UpdatePasswordJSONRequestBody{EncryptedPassword: encryptedPassword})
	if err != nil {
		return err
	}

	return a.validateResponse(resp, http.StatusOK)
}

func (a *APIAuthService) CreateGroup(ctx context.Context, group *model.BaseGroup) error {
	resp, err := a.apiClient.CreateGroupWithResponse(ctx, CreateGroupJSONRequestBody{
		Id: group.DisplayName,
	})
	if err != nil {
		return err
	}
	return a.validateResponse(resp, http.StatusCreated)
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

func (a *APIAuthService) DeleteGroup(ctx context.Context, groupDisplayName string) error {
	resp, err := a.apiClient.DeleteGroupWithResponse(ctx, groupDisplayName)
	if err != nil {
		return err
	}
	return a.validateResponse(resp, http.StatusNoContent)
}

func (a *APIAuthService) GetGroup(ctx context.Context, groupDisplayName string) (*model.Group, error) {
	resp, err := a.apiClient.GetGroupWithResponse(ctx, groupDisplayName)
	if err != nil {
		return nil, err
	}
	if err := a.validateResponse(resp, http.StatusOK); err != nil {
		return nil, err
	}
	return &model.Group{
		BaseGroup: model.BaseGroup{
			CreatedAt:   time.Unix(resp.JSON200.CreationDate, 0),
			DisplayName: resp.JSON200.Name,
		},
	}, nil
}

func (a *APIAuthService) ListGroups(ctx context.Context, params *model.PaginationParams) ([]*model.Group, *model.Paginator, error) {
	resp, err := a.apiClient.ListGroupsWithResponse(ctx, &ListGroupsParams{
		Prefix: paginationPrefix(params.Prefix),
		After:  paginationAfter(params.After),
		Amount: paginationAmount(params.Amount),
	})
	if err != nil {
		return nil, nil, err
	}
	if err := a.validateResponse(resp, http.StatusOK); err != nil {
		return nil, nil, err
	}
	groups := make([]*model.Group, len(resp.JSON200.Results))

	for i, r := range resp.JSON200.Results {
		groups[i] = &model.Group{
			ID: strconv.Itoa(0),
			BaseGroup: model.BaseGroup{
				CreatedAt:   time.Unix(r.CreationDate, 0),
				DisplayName: r.Name,
			},
		}
	}
	return groups, toPagination(resp.JSON200.Pagination), nil
}

func (a *APIAuthService) AddUserToGroup(ctx context.Context, username, groupDisplayName string) error {
	resp, err := a.apiClient.AddGroupMembershipWithResponse(ctx, groupDisplayName, username)
	if err != nil {
		return err
	}
	return a.validateResponse(resp, http.StatusCreated)
}

func (a *APIAuthService) RemoveUserFromGroup(ctx context.Context, username, groupDisplayName string) error {
	resp, err := a.apiClient.DeleteGroupMembershipWithResponse(ctx, groupDisplayName, username)
	if err != nil {
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
		return nil, nil, err
	}
	if err := a.validateResponse(resp, http.StatusOK); err != nil {
		return nil, nil, err
	}
	userGroups := make([]*model.Group, len(resp.JSON200.Results))

	for i, r := range resp.JSON200.Results {
		userGroups[i] = &model.Group{
			ID: strconv.Itoa(0),
			BaseGroup: model.BaseGroup{
				CreatedAt:   time.Unix(r.CreationDate, 0),
				DisplayName: r.Name,
			},
		}
	}
	return userGroups, toPagination(resp.JSON200.Pagination), nil
}

func (a *APIAuthService) ListGroupUsers(ctx context.Context, groupDisplayName string, params *model.PaginationParams) ([]*model.User, *model.Paginator, error) {
	resp, err := a.apiClient.ListGroupMembersWithResponse(ctx, groupDisplayName, &ListGroupMembersParams{
		Prefix: paginationPrefix(params.Prefix),
		After:  paginationAfter(params.After),
		Amount: paginationAmount(params.Amount),
	})
	if err != nil {
		return nil, nil, err
	}
	if err := a.validateResponse(resp, http.StatusOK); err != nil {
		return nil, nil, err
	}
	members := make([]*model.User, len(resp.JSON200.Results))

	for i, r := range resp.JSON200.Results {
		members[i] = &model.User{
			ID: strconv.Itoa(0),
			BaseUser: model.BaseUser{
				CreatedAt:    time.Unix(r.CreationDate, 0),
				Username:     r.Name,
				FriendlyName: r.FriendlyName,
				Email:        r.Email,
			},
		}
	}
	return members, toPagination(resp.JSON200.Pagination), nil
}

func (a *APIAuthService) WritePolicy(ctx context.Context, policy *model.BasePolicy) error {
	stmts := make([]Statement, len(policy.Statement))
	for i, s := range policy.Statement {
		stmts[i] = Statement{
			Action:   s.Action,
			Effect:   s.Effect,
			Resource: s.Resource,
		}
	}
	createdAt := policy.CreatedAt.Unix()

	resp, err := a.apiClient.CreatePolicyWithResponse(ctx, CreatePolicyJSONRequestBody{
		CreationDate: &createdAt,
		Name:         policy.DisplayName,
		Statement:    stmts,
	})
	if err != nil {
		return err
	}
	return a.validateResponse(resp, http.StatusCreated)
}

func serializePolicyToModalPolicy(p Policy) *model.BasePolicy {
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
	return &model.BasePolicy{
		CreatedAt:   creationTime,
		DisplayName: p.Name,
		Statement:   stmts,
	}
}

func (a *APIAuthService) GetPolicy(ctx context.Context, policyDisplayName string) (*model.BasePolicy, error) {
	resp, err := a.apiClient.GetPolicyWithResponse(ctx, policyDisplayName)
	if err != nil {
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
		return err
	}
	return a.validateResponse(resp, http.StatusNoContent)
}

func (a *APIAuthService) ListPolicies(ctx context.Context, params *model.PaginationParams) ([]*model.BasePolicy, *model.Paginator, error) {
	resp, err := a.apiClient.ListPoliciesWithResponse(ctx, &ListPoliciesParams{
		Prefix: paginationPrefix(params.Prefix),
		After:  paginationAfter(params.After),
		Amount: paginationAmount(params.Amount),
	})
	if err != nil {
		return nil, nil, err
	}
	if err := a.validateResponse(resp, http.StatusOK); err != nil {
		return nil, nil, err
	}
	policies := make([]*model.BasePolicy, len(resp.JSON200.Results))

	for i, r := range resp.JSON200.Results {
		policies[i] = serializePolicyToModalPolicy(r)
	}
	return policies, toPagination(resp.JSON200.Pagination), nil
}

func (a *APIAuthService) CreateCredentials(ctx context.Context, username string) (*model.Credential, error) {
	resp, err := a.apiClient.CreateCredentialsWithResponse(ctx, username, &CreateCredentialsParams{})
	if err != nil {
		return nil, err
	}
	if err := a.validateResponse(resp, http.StatusCreated); err != nil {
		return nil, err
	}
	credentials := resp.JSON201
	return &model.Credential{
		UserID: strconv.Itoa(0),
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
		return nil, err
	}
	if err := a.validateResponse(resp, http.StatusCreated); err != nil {
		return nil, err
	}
	credentials := resp.JSON201
	return &model.Credential{
		UserID: strconv.Itoa(0),
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
		return err
	}
	return a.validateResponse(resp, http.StatusNoContent)
}

func (a *APIAuthService) GetCredentialsForUser(ctx context.Context, username, accessKeyID string) (*model.Credential, error) {
	resp, err := a.apiClient.GetCredentialsForUserWithResponse(ctx, username, accessKeyID)
	if err != nil {
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
		UserID: strconv.Itoa(0),
	}, nil
}

func (a *APIAuthService) GetCredentials(ctx context.Context, accessKeyID string) (*model.Credential, error) {
	return a.cache.GetCredential(accessKeyID, func() (*model.Credential, error) {
		resp, err := a.apiClient.GetCredentialsWithResponse(ctx, accessKeyID)
		if err != nil {
			return nil, err
		}
		if err := a.validateResponse(resp, http.StatusOK); err != nil {
			return nil, err
		}
		credentials := resp.JSON200
		return &model.Credential{
			BaseCredential: model.BaseCredential{
				AccessKeyID:                   credentials.AccessKeyId,
				SecretAccessKey:               credentials.SecretAccessKey,
				SecretAccessKeyEncryptedBytes: nil,
				IssuedDate:                    time.Unix(credentials.CreationDate, 0),
			},
			UserID: model.ConvertDBID(credentials.UserId),
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
			UserID: strconv.Itoa(0),
		}
	}
	return credentials, toPagination(resp.JSON200.Pagination), nil
}

func (a *APIAuthService) AttachPolicyToUser(ctx context.Context, policyDisplayName, username string) error {
	resp, err := a.apiClient.AttachPolicyToUserWithResponse(ctx, username, policyDisplayName)
	if err != nil {
		return err
	}
	return a.validateResponse(resp, http.StatusCreated)
}

func (a *APIAuthService) DetachPolicyFromUser(ctx context.Context, policyDisplayName, username string) error {
	resp, err := a.apiClient.DetachPolicyFromUserWithResponse(ctx, username, policyDisplayName)
	if err != nil {
		return err
	}
	return a.validateResponse(resp, http.StatusNoContent)
}

func (a *APIAuthService) listUserPolicies(ctx context.Context, username string, params *model.PaginationParams, effective bool) ([]*model.BasePolicy, *model.Paginator, error) {
	resp, err := a.apiClient.ListUserPoliciesWithResponse(ctx, username, &ListUserPoliciesParams{
		Prefix:    paginationPrefix(params.Prefix),
		After:     paginationAfter(params.After),
		Amount:    paginationAmount(params.Amount),
		Effective: &effective,
	})
	if err != nil {
		return nil, nil, err
	}
	if err := a.validateResponse(resp, http.StatusOK); err != nil {
		return nil, nil, err
	}
	policies := make([]*model.BasePolicy, len(resp.JSON200.Results))

	for i, r := range resp.JSON200.Results {
		policies[i] = serializePolicyToModalPolicy(r)
	}
	return policies, toPagination(resp.JSON200.Pagination), nil
}

func (a *APIAuthService) ListUserPolicies(ctx context.Context, username string, params *model.PaginationParams) ([]*model.BasePolicy, *model.Paginator, error) {
	return a.listUserPolicies(ctx, username, params, false)
}

func (a *APIAuthService) listAllEffectivePolicies(ctx context.Context, username string) ([]*model.BasePolicy, error) {
	hasMore := true
	after := ""
	amount := maxPage
	policies := make([]*model.BasePolicy, 0)
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

func (a *APIAuthService) ListEffectivePolicies(ctx context.Context, username string, params *model.PaginationParams) ([]*model.BasePolicy, *model.Paginator, error) {
	if params.Amount == -1 {
		// read through the cache when requesting the full list
		policies, err := a.cache.GetUserPolicies(username, func() ([]*model.BasePolicy, error) {
			p, err := a.listAllEffectivePolicies(ctx, username)

			return p, err
		})
		if err != nil {
			return nil, nil, err
		}
		return policies, &model.Paginator{Amount: len(policies)}, nil
	}
	return a.listUserPolicies(ctx, username, params, true)
}

func (a *APIAuthService) AttachPolicyToGroup(ctx context.Context, policyDisplayName, groupDisplayName string) error {
	resp, err := a.apiClient.AttachPolicyToGroupWithResponse(ctx, groupDisplayName, policyDisplayName)
	if err != nil {
		return err
	}
	return a.validateResponse(resp, http.StatusCreated)
}

func (a *APIAuthService) DetachPolicyFromGroup(ctx context.Context, policyDisplayName, groupDisplayName string) error {
	resp, err := a.apiClient.DetachPolicyFromGroupWithResponse(ctx, groupDisplayName, policyDisplayName)
	if err != nil {
		return err
	}
	return a.validateResponse(resp, http.StatusNoContent)
}

func (a *APIAuthService) ListGroupPolicies(ctx context.Context, groupDisplayName string, params *model.PaginationParams) ([]*model.BasePolicy, *model.Paginator, error) {
	resp, err := a.apiClient.ListGroupPoliciesWithResponse(ctx, groupDisplayName, &ListGroupPoliciesParams{
		Prefix: paginationPrefix(params.Prefix),
		After:  paginationAfter(params.After),
		Amount: paginationAmount(params.Amount),
	})
	if err != nil {
		return nil, nil, err
	}
	if err := a.validateResponse(resp, http.StatusOK); err != nil {
		return nil, nil, err
	}
	policies := make([]*model.BasePolicy, len(resp.JSON200.Results))

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

	allowed := checkPermissions(req.RequiredPermissions, req.Username, policies)

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
		return err
	}
	if res.StatusCode() == http.StatusBadRequest {
		return ErrInvalidToken
	}
	return a.validateResponse(res, http.StatusCreated)
}

func NewAPIAuthService(apiEndpoint, token string, secretStore crypt.SecretStore, cacheConf params.ServiceCache, timeout *time.Duration) (*APIAuthService, error) {
	bearerToken, err := securityprovider.NewSecurityProviderBearerToken(token)
	if err != nil {
		return nil, err
	}

	httpClient := &http.Client{}
	if timeout != nil {
		httpClient.Timeout = *timeout
	}
	client, err := NewClientWithResponses(
		apiEndpoint,
		WithRequestEditorFn(bearerToken.Intercept),
		WithHTTPClient(httpClient),
	)
	if err != nil {
		return nil, err
	}
	logging.Default().Info("initialized authorization service")
	var cache Cache
	if cacheConf.Enabled {
		cache = NewLRUCache(cacheConf.Size, cacheConf.TTL, cacheConf.EvictionJitter)
	} else {
		cache = &DummyCache{}
	}
	return &APIAuthService{
		apiClient:   client,
		secretStore: secretStore,
		cache:       cache,
	}, nil
}
