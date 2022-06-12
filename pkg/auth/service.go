package auth

//go:generate oapi-codegen -package auth -generate "types,client"  -o client.gen.go ../../api/authorization.yml

import (
	"context"
	"fmt"
	"net/http"
	"reflect"
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
	"google.golang.org/protobuf/reflect/protoreflect"
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

func (s *KVAuthService) ListKVPaged(ctx context.Context, protoType protoreflect.MessageType, params *model.PaginationParams, prefix string) (*[]protoreflect.ProtoMessage, *model.Paginator, error) {
	amount := 0
	var it kv.PrimaryIterator
	var err error
	if params != nil {
		it, err := kv.NewPrimaryIterator(ctx, s.store.Store, protoType, model.PartitionKey, prefix, params.After)
		if err != nil {
			return nil, nil, fmt.Errorf("sacn prefix(%s): %w", prefix, err)
		}
		defer it.Close()
		if params.Amount >= 0 {
			amount = params.Amount + 1
		}
	}
	if amount > maxPage {
		amount = maxPage
	}
	count := 0
	entries := make([]protoreflect.ProtoMessage, 0)
	var lastKey string
	for it.Next() && count < amount {
		entry := it.Entry()
		value := entry.Value
		entries = append(entries, value)
		lastKey = entry.Key
		//nolint
		//value, ok := entry.Value
		//if ok == false {
		//	return nil, nil, fmt.Errorf("nil value")
		//}
		//val := reflect.New(value)
		//slice = reflect.Append(slice, value)
		count += 1
	}
	if err = it.Err(); err != nil {
		return nil, nil, fmt.Errorf("list DB: %w", err)
	}

	p := &model.Paginator{}
	if params != nil && params.Amount >= 0 && len(entries) == params.Amount+1 {
		//nolint
		// we have more pages
		//entries = make([]protoreflect.ProtoMessage, 0, params.Amount)
		//slice = slice.Slice(0, params.Amount)
		p.Amount = params.Amount
		//nolint
		//lastElem := slice.Index(slice.Len() - 1).Elem()
		//p.NextPageToken = lastElem.FieldByName(tokenField).String()
		p.NextPageToken = lastKey
		return &entries, p, nil
	}
	p.Amount = len(entries)
	return &entries, p, nil
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

//nolint
func (s *KVAuthService) decryptSecret(value []byte) (string, error) {
	decrypted, err := s.secretStore.Decrypt(value)
	if err != nil {
		return "", err
	}
	return string(decrypted), nil
}

func (s *KVAuthService) encryptSecret(secretAccessKey string) ([]byte, error) {
	encrypted, err := s.secretStore.Encrypt([]byte(secretAccessKey))
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
		return "", err
	}
	userKey := model.KvUserPath(user.Username)
	id := uuid.New().String()
	userWithID := model.User{ID: id, BaseUser: *user}

	err := s.store.SetMsg(ctx, model.PartitionKey, userKey, model.ProtoFromUser(&userWithID))
	if err != nil {
		return "", fmt.Errorf("save user (usesrKey %s): %w", userKey, err)
	}
	return fmt.Sprint(id), err
}

func (s *KVAuthService) DeleteUser(ctx context.Context, username string) error {
	if _, err := s.GetUser(ctx, username); err != nil {
		return fmt.Errorf("%s: %w", username, err)
	}
	userPath := model.KvUserPath(username)
	// delete policy attached to user
	hasMorePolicies := true
	params := model.PaginationParams{
		Amount: maxPage,
	}
	for hasMorePolicies {
		userPolicies, paginator, err := s.ListUserPolicies(ctx, username, &params)
		if err != nil {
			return err
		}
		for _, policy := range userPolicies {
			err := s.DetachPolicyFromUser(ctx, policy.DisplayName, username)
			if err != nil {
				return err
			}
		}
		if paginator.NextPageToken == "" {
			hasMorePolicies = false
		}
		params = model.PaginationParams{
			After:  paginator.NextPageToken,
			Amount: maxPage,
		}
	}

	// delete user membership of group
	hasMoreGroups := true
	params = model.PaginationParams{
		Amount: maxPage,
	}
	for hasMoreGroups {
		userGroups, paginator, err := s.ListUserGroups(ctx, username, &params)
		if err != nil {
			return err
		}
		for _, group := range userGroups {
			err := s.RemoveUserFromGroup(ctx, username, group.DisplayName)
			if err != nil {
				return err
			}
		}
		if paginator.NextPageToken == "" {
			hasMoreGroups = false
		}
		params = model.PaginationParams{
			After:  paginator.NextPageToken,
			Amount: maxPage,
		}
	}

	// delete user
	err := s.store.DeleteMsg(ctx, model.PartitionKey, userPath)
	if err != nil {
		return fmt.Errorf("delete user (usesrKey %s): %w", userPath, err)
	}
	return err
}

func (s *KVAuthService) GetUser(ctx context.Context, username string) (*model.User, error) {
	return s.cache.GetUser(username, func() (*model.User, error) {
		userKey := model.KvUserPath(username)
		m := model.UserData{}
		_, err := s.store.GetMsg(ctx, model.PartitionKey, userKey, &m)
		if err != nil {
			return nil, err
		}
		return model.UserFromProto(&m), nil
	})
}

func (s *KVAuthService) GetUserByEmail(ctx context.Context, email string) (*model.User, error) {
	m := &model.UserData{}
	itr, err := s.store.Scan(ctx, m.ProtoReflect().Type(), model.PartitionKey, model.KvUserPath(""))
	if err != nil {
		return nil, fmt.Errorf("sacn users users: %w", err)
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
		itr, err := s.store.Scan(ctx, m.ProtoReflect().Type(), model.PartitionKey, model.KvUserPath(""))
		if err != nil {
			return nil, fmt.Errorf("sacn users users: %w", err)
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
	usersKey := model.KvUserPath(params.Prefix)

	msgs, paginator, err := s.ListKVPaged(ctx, (&user).ProtoReflect().Type(), params, usersKey)
	if msgs == nil {
		return nil, paginator, err
	}
	return model.ConvertUsersDataList(interface{}(msgs).([]*model.UserData)), paginator, err
}

func (s *KVAuthService) ListUserCredentials(ctx context.Context, username string, params *model.PaginationParams) ([]*model.Credential, *model.Paginator, error) {
	var credential model.CredentialData
	credentialsKey := model.KvCredentialPath(username, params.After)

	msgs, paginator, err := s.ListKVPaged(ctx, (&credential).ProtoReflect().Type(), params, credentialsKey)
	if msgs == nil {
		return nil, paginator, err
	}
	return model.ConvertCredDataList(interface{}(msgs).([]*model.CredentialData)), paginator, err
}

func (s *KVAuthService) AttachPolicyToUser(ctx context.Context, policyDisplayName string, username string) error {
	if _, err := s.GetUser(ctx, username); err != nil {
		return fmt.Errorf("%s: %w", username, err)
	}
	if _, err := s.GetPolicy(ctx, policyDisplayName); err != nil {
		return fmt.Errorf("%s: %w", policyDisplayName, err)
	}

	userKey := model.KvUserPath(username)
	pu := model.PolicyToUser(policyDisplayName, username)

	err := s.store.SetMsg(ctx, model.PartitionKey, pu, &kv.SecondaryIndex{PrimaryKey: []byte(userKey)})
	if err != nil {
		return fmt.Errorf("policy attachment to user: (key %s): %w", pu, err)
	}
	return err
}

func (s *KVAuthService) DetachPolicyFromUser(ctx context.Context, policyDisplayName, username string) error {
	if _, err := s.GetUser(ctx, username); err != nil {
		return fmt.Errorf("%s: %w", username, err)
	}
	if _, err := s.GetPolicy(ctx, policyDisplayName); err != nil {
		return fmt.Errorf("%s: %w", policyDisplayName, err)
	}

	pu := model.PolicyToUser(policyDisplayName, username)
	err := s.store.DeleteMsg(ctx, model.PartitionKey, pu)
	if err != nil {
		return fmt.Errorf("policy detachment to user: (key %s): %w", pu, err)
	}
	return err
}

func (s *KVAuthService) ListUserPolicies(ctx context.Context, username string, params *model.PaginationParams) ([]*model.BasePolicy, *model.Paginator, error) {
	var policy model.PolicyData
	userPolicyKey := model.PolicyToUser(params.After, username)

	msgs, paginator, err := s.ListKVPaged(ctx, (&policy).ProtoReflect().Type(), params, userPolicyKey)
	if msgs == nil {
		return nil, paginator, err
	}
	return model.ConvertPolicyDataList(interface{}(msgs).([]*model.PolicyData)), paginator, err
}

//nolint
func (s *KVAuthService) getEffectivePolicies(ctx context.Context, username string, params *model.PaginationParams) ([]*model.BasePolicy, *model.Paginator, error) {
	//nolint
	//var Empty struct{}
	//count := 0
	set := make(map[*model.BasePolicy]struct{})
	//nolint
	//p := pagina
	//if params.After
	//	// get policies attated to user
	//	userPolicies, paginator, err := s.ListUserPolicies(ctx, username, params)
	//	if err != nil {
	//		return nil, nil, fmt.Errorf("list user policies: %w", err)
	//	}
	//	for _, policy := range userPolicies {
	//		set[policy] = Empty
	//	}
	//	if len(set) == params.Amount {
	//		break
	//	}
	//	if len(set) > params.Amount {
	//		After:  paginator.NextPageToken,
	//		break
	//	}
	//
	//	// get user's groups
	//	groups, paginator, err := s.ListUserGroups(ctx, username, params)
	//	if err != nil {
	//		return nil, nil, fmt.Errorf("list user group: %w", err)
	//	}
	//	for _, group := range groups {
	//		groupPolicies, paginator, err := s.ListGroupPolicies(ctx, group.DisplayName, params)
	//	}
	//
	//	var policy model.BasePolicy
	//	userPolicyKey := model.PolicyToUser(params.After, username)
	//
	//	msgs, paginator, err := s.ListKVPaged(ctx, (&policy).ProtoReflect().Type(), params, userPolicyKey)
	//	if msgs == nil {
	//		return nil, paginator, err
	//	}
	//}
	keys := make([]*model.BasePolicy, len(set))
	i := 0
	for k := range set {
		keys[i] = k
		i++
	}
	return keys, nil, nil
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
	groupPolicyKey := model.PolicyToGroup(params.After, groupDisplayName)

	msgs, paginator, err := s.ListKVPaged(ctx, (&policy).ProtoReflect().Type(), params, groupPolicyKey)
	if msgs == nil {
		return nil, paginator, err
	}
	return model.ConvertPolicyDataList(interface{}(msgs).([]*model.PolicyData)), paginator, err
}

func (s *KVAuthService) CreateGroup(ctx context.Context, group *model.BaseGroup) error {
	if err := model.ValidateAuthEntityID(group.DisplayName); err != nil {
		return err
	}

	groupKey := model.KvGroupPath(group.DisplayName)
	id := uuid.New().String()
	groupWithID := model.Group{ID: id, BaseGroup: *group}

	err := s.store.SetMsg(ctx, model.PartitionKey, groupKey, model.ProtoFromGroup(&groupWithID))
	if err != nil {
		return fmt.Errorf("save group (groupKey %s): %w", groupKey, err)
	}
	return err
}

func (s *KVAuthService) DeleteGroup(ctx context.Context, groupDisplayName string) error {
	if _, err := s.GetGroup(ctx, groupDisplayName); err != nil {
		return fmt.Errorf("%s: %w", groupDisplayName, err)
	}
	groupPath := model.KvGroupPath(groupDisplayName)
	// delete user membership to group
	hasMoreUsers := true
	params := model.PaginationParams{
		Amount: maxPage,
	}
	for hasMoreUsers {
		users, paginator, err := s.ListGroupUsers(ctx, groupDisplayName, &params)
		if err != nil {
			return err
		}
		for _, user := range users {
			err := s.RemoveUserFromGroup(ctx, user.Username, groupDisplayName)
			if err != nil {
				return err
			}
		}
		if paginator.NextPageToken == "" {
			hasMoreUsers = false
		}
		params = model.PaginationParams{
			After:  paginator.NextPageToken,
			Amount: maxPage,
		}
	}

	// delete policy attachment to group
	hasMorePolicies := true
	params = model.PaginationParams{
		Amount: maxPage,
	}
	for hasMorePolicies {
		policies, paginator, err := s.ListGroupPolicies(ctx, groupDisplayName, &params)
		if err != nil {
			return err
		}
		for _, policy := range policies {
			err := s.DetachPolicyFromGroup(ctx, policy.DisplayName, groupDisplayName)
			if err != nil {
				return err
			}
		}
		if paginator.NextPageToken == "" {
			hasMorePolicies = false
		}
		params = model.PaginationParams{
			After:  paginator.NextPageToken,
			Amount: maxPage,
		}
	}

	// delete group
	err := s.store.DeleteMsg(ctx, model.PartitionKey, groupPath)
	if err != nil {
		return fmt.Errorf("delete user (usesrKey %s): %w", groupPath, err)
	}
	return err
}

func (s *KVAuthService) GetGroup(ctx context.Context, groupDisplayName string) (*model.Group, error) {
	groupKey := model.KvGroupPath(groupDisplayName)
	m := model.GroupData{}
	_, err := s.store.GetMsg(ctx, model.PartitionKey, groupKey, &m)
	if err != nil {
		return nil, err
	}
	return model.GroupFromProto(&m), nil
}

func (s *KVAuthService) ListGroups(ctx context.Context, params *model.PaginationParams) ([]*model.Group, *model.Paginator, error) {
	var group model.GroupData
	groupKey := model.KvGroupPath(params.Prefix)

	msgs, paginator, err := s.ListKVPaged(ctx, (&group).ProtoReflect().Type(), params, groupKey)
	if msgs == nil {
		return nil, paginator, err
	}
	return model.ConvertGroupDataList(interface{}(msgs).([]*model.GroupData)), paginator, err
}

func (s *KVAuthService) AddUserToGroup(ctx context.Context, username, groupDisplayName string) error {
	if _, err := s.GetUser(ctx, username); err != nil {
		return fmt.Errorf("%s: %w", username, err)
	}
	if _, err := s.GetGroup(ctx, groupDisplayName); err != nil {
		return fmt.Errorf("%s: %w", groupDisplayName, err)
	}

	userKey := model.KvUserPath(username)
	gu := model.KvUserToGroup(username, groupDisplayName)

	err := s.store.SetMsg(ctx, model.PartitionKey, gu, &kv.SecondaryIndex{PrimaryKey: []byte(userKey)})
	if err != nil {
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

	gu := model.KvUserToGroup(username, groupDisplayName)
	err := s.store.DeleteMsg(ctx, model.PartitionKey, gu)
	if err != nil {
		return fmt.Errorf("remove user from group: (key %s): %w", gu, err)
	}
	return err
}

func (s *KVAuthService) ListUserGroups(ctx context.Context, username string, params *model.PaginationParams) ([]*model.Group, *model.Paginator, error) {
	// TODO - NOT CORRECT
	if _, err := s.GetUser(ctx, username); err != nil {
		return nil, nil, fmt.Errorf("%s: %w", username, err)
	}
	var policy model.GroupData
	userGroupKey := model.KvUserToGroup(username, "")

	msgs, paginator, err := s.ListKVPaged(ctx, (&policy).ProtoReflect().Type(), params, userGroupKey)
	if msgs == nil {
		return nil, paginator, err
	}
	return model.ConvertGroupDataList(interface{}(msgs).([]*model.GroupData)), paginator, err
}

func (s *KVAuthService) ListGroupUsers(ctx context.Context, groupDisplayName string, params *model.PaginationParams) ([]*model.User, *model.Paginator, error) {
	if _, err := s.GetGroup(ctx, groupDisplayName); err != nil {
		return nil, nil, fmt.Errorf("%s: %w", groupDisplayName, err)
	}
	var policy model.UserData
	userGroupKey := model.KvUserToGroup("", groupDisplayName)

	msgs, paginator, err := s.ListKVPaged(ctx, (&policy).ProtoReflect().Type(), params, userGroupKey)
	if msgs == nil {
		return nil, paginator, err
	}
	return model.ConvertUsersDataList(interface{}(msgs).([]*model.UserData)), paginator, err
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
	policyKey := model.PolicyPath(policy.DisplayName)
	id := uuid.New().String()

	err := s.store.SetMsg(ctx, model.PartitionKey, policyKey, model.ProtoFromPolicy(policy, id))
	if err != nil {
		return fmt.Errorf("save policy (policyKey %s): %w", policyKey, err)
	}
	return err
}

func (s *KVAuthService) GetPolicy(ctx context.Context, policyDisplayName string) (*model.BasePolicy, error) {
	policyKey := model.PolicyPath(policyDisplayName)
	p := model.PolicyData{}
	_, err := s.store.GetMsg(ctx, model.PartitionKey, policyKey, &p)
	if err != nil {
		return nil, err
	}
	return model.PolicyFromProto(&p), nil
}

func (s *KVAuthService) DeletePolicy(ctx context.Context, policyDisplayName string) error {
	if _, err := s.GetPolicy(ctx, policyDisplayName); err != nil {
		return fmt.Errorf("%s: %w", policyDisplayName, err)
	}
	policyPath := model.PolicyPath(policyDisplayName)
	// TODO delete policy attachment to user

	// TODO delete policy attachment to group

	// delete policy
	err := s.store.DeleteMsg(ctx, model.PartitionKey, policyPath)
	if err != nil {
		return fmt.Errorf("delete policy (policyKey %s): %w", policyPath, err)
	}
	return err
}

func (s *KVAuthService) ListPolicies(ctx context.Context, params *model.PaginationParams) ([]*model.BasePolicy, *model.Paginator, error) {
	var policy model.PolicyData
	policyKey := model.PolicyPath("")

	msgs, paginator, err := s.ListKVPaged(ctx, (&policy).ProtoReflect().Type(), params, policyKey)
	if msgs == nil {
		return nil, paginator, err
	}
	return model.ConvertPolicyDataList(interface{}(msgs).([]*model.PolicyData)), paginator, err
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
	encryptedKey, err := s.encryptSecret(secretAccessKey)
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
	credentialsKey := model.KvCredentialPath(user.Username, c.AccessKeyID)
	err = s.store.SetMsg(ctx, model.PartitionKey, credentialsKey, model.ProtoFromCredential(c))
	if err != nil {
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

	credPath := model.KvCredentialPath(username, accessKeyID)
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

	policyKey := model.PolicyPath(policyDisplayName)
	pg := model.PolicyToGroup(policyDisplayName, groupDisplayName)

	err := s.store.SetMsg(ctx, model.PartitionKey, pg, &kv.SecondaryIndex{PrimaryKey: []byte(policyKey)})
	if err != nil {
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

	pg := model.PolicyToGroup(policyDisplayName, groupDisplayName)
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
	creadantialsKey := model.KvCredentialPath(username, accessKeyID)
	m := model.CredentialData{}
	_, err := s.store.GetMsg(ctx, model.PartitionKey, creadantialsKey, &m)
	if err != nil {
		return nil, err
	}
	return model.CredentialFromProto(&m), nil
}

func (s *KVAuthService) GetCredentials(ctx context.Context, accessKeyID string) (*model.Credential, error) {
	// TODO - need to list all users
	//nolint
	//return s.cache.GetCredential(accessKeyID, func() (*model.Credential, error) {
	//creadantialsKey := model.KvCredentialPath(accessKeyID)
	//m := model.CredentialData{}
	//_, err := s.store.GetMsg(ctx, model.PartitionKey, userKey, &m)
	//if err != nil {
	//	return nil, err
	//}
	//return model.CREAFromProto(&m), nil
	//credentials, err := s.db.Transact(ctx, func(tx db.Tx) (interface{}, error) {
	//	credentials := &model.DBCredential{}
	//	err := tx.Get(credentials, `SELECT * FROM auth_credentials WHERE auth_credentials.access_key_id = $1`,
	//		accessKeyID)
	//	if err != nil {
	//		return nil, err
	//	}
	//	key, err := s.decryptSecret(credentials.SecretAccessKeyEncryptedBytes)
	//	if err != nil {
	//		return nil, err
	//	}
	//	credentials.SecretAccessKey = key
	//	return credentials, nil
	//})
	//if err != nil {
	//	return nil, err
	//}
	//return model.ConvertCreds(credentials.(*model.DBCredential)), nil
	//})
	return nil, ErrNotFound
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
	userKey := model.KvUserPath(user.Username)
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
	// TODO
	//nolint
	//tokenPath := model.KvTokenPath(tokenID)
	//
	//m := model.TokenData{tokenID: tokenID, tokenExpiresAt: tokenExpiresAt}
	//err := s.store.SetMsg(ctx, model.PartitionKey, tokenPath, &m)
	//if err != nil {
	//	return false, err
	//}
	//
	//return true, nil
	//
	//res, err := s.db.Exec(ctx, `INSERT INTO auth_expired_tokens (token_id, token_expires_at) VALUES ($1,$2) ON CONFLICT DO NOTHING`,
	//	tokenID, tokenExpiresAt)
	//if err != nil {
	//	return false, err
	//}
	//canUseToken := res.RowsAffected() == 1
	//// cleanup old tokens
	//_, err = s.db.Exec(ctx, `DELETE FROM auth_expired_tokens WHERE token_expires_at < $1`, time.Now())
	//if err != nil {
	//	s.log.WithError(err).Error("delete expired tokens")
	//}
	//return canUseToken, nil
	return false, nil
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
