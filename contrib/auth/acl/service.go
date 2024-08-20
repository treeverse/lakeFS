package acl

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/treeverse/lakefs/pkg/auth"
	"github.com/treeverse/lakefs/pkg/auth/crypt"
	"github.com/treeverse/lakefs/pkg/auth/keys"
	"github.com/treeverse/lakefs/pkg/auth/model"
	"github.com/treeverse/lakefs/pkg/auth/params"
	"github.com/treeverse/lakefs/pkg/kv"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

const ServerPartitionKey = "aclauth"

type AuthService struct {
	store       kv.Store
	secretStore crypt.SecretStore
	cache       auth.Cache
}

func NewAuthService(store kv.Store, secretStore crypt.SecretStore, cacheConf params.ServiceCache) *AuthService {
	var cache auth.Cache
	if cacheConf.Enabled {
		cache = auth.NewLRUCache(cacheConf.Size, cacheConf.TTL, cacheConf.Jitter)
	} else {
		cache = &auth.DummyCache{}
	}
	res := &AuthService{
		store:       store,
		secretStore: secretStore,
		cache:       cache,
	}
	return res
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

	amount := auth.MaxPage
	if params.Amount >= 0 && params.Amount < auth.MaxPage {
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

func (s *AuthService) SecretStore() crypt.SecretStore {
	return s.secretStore
}

func (s *AuthService) Cache() auth.Cache {
	return s.cache
}

func (s *AuthService) CreateUser(ctx context.Context, user *model.User) (string, error) {
	if err := model.ValidateAuthEntityID(user.Username); err != nil {
		return auth.InvalidUserID, err
	}
	userKey := model.UserPath(user.Username)

	err := kv.SetMsgIf(ctx, s.store, model.PartitionKey, userKey, model.ProtoFromUser(user), nil)
	if err != nil {
		if errors.Is(err, kv.ErrPredicateFailed) {
			err = auth.ErrAlreadyExists
		}
		return "", fmt.Errorf("save user (auth.UserKey %s): %w", userKey, err)
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
		return fmt.Errorf("delete user (auth.UserKey %s): %w", userPath, err)
	}
	return err
}

type UserPredicate func(u *model.UserData) bool

func (s *AuthService) getUserByPredicate(ctx context.Context, key auth.UserKey, predicate UserPredicate) (*model.User, error) {
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
		return nil, auth.ErrNotFound
	})
}

func (s *AuthService) GetUserByID(ctx context.Context, userID string) (*model.User, error) {
	return s.GetUser(ctx, userID)
}

func (s *AuthService) GetUser(ctx context.Context, username string) (*model.User, error) {
	return s.cache.GetUser(auth.UserKey{Username: username}, func() (*model.User, error) {
		userKey := model.UserPath(username)
		m := model.UserData{}
		_, err := kv.GetMsg(ctx, s.store, model.PartitionKey, userKey, &m)
		if err != nil {
			if errors.Is(err, kv.ErrNotFound) {
				err = auth.ErrNotFound
			}
			return nil, fmt.Errorf("%s: %w", username, err)
		}
		return model.UserFromProto(&m), nil
	})
}

func (s *AuthService) GetUserByEmail(ctx context.Context, email string) (*model.User, error) {
	return s.getUserByPredicate(ctx, auth.UserKey{Email: email}, func(value *model.UserData) bool {
		return value.Email == email
	})
}

func (s *AuthService) GetUserByExternalID(ctx context.Context, externalID string) (*model.User, error) {
	return s.getUserByPredicate(ctx, auth.UserKey{ExternalID: externalID}, func(value *model.UserData) bool {
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

func (s *AuthService) UpdateUserFriendlyName(_ context.Context, _ string, _ string) error {
	return auth.ErrNotImplemented
}

func (s *AuthService) ListUserCredentials(ctx context.Context, username string, params *model.PaginationParams) ([]*model.Credential, *model.Paginator, error) {
	var credential model.CredentialData
	credentialsKey := model.CredentialPath(username, params.Prefix)
	msgs, paginator, err := s.ListKVPaged(ctx, (&credential).ProtoReflect().Type(), params, credentialsKey, false)
	if err != nil {
		return nil, nil, err
	}
	creds, err := model.ConvertCredDataList(s.secretStore, msgs, false)
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
			err = auth.ErrAlreadyExists
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
	amount := auth.MaxPage
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

	if params.Amount < 0 || params.Amount > auth.MaxPage {
		params.Amount = auth.MaxPage
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

func ListEffectivePolicies(ctx context.Context, username string, params *model.PaginationParams, getEffectivePolicies effectivePoliciesGetter, cache auth.Cache) ([]*model.Policy, *model.Paginator, error) {
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
			err = auth.ErrAlreadyExists
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
		return fmt.Errorf("delete user (auth.UserKey %s): %w", groupPath, err)
	}
	return nil
}

func (s *AuthService) GetGroup(ctx context.Context, groupID string) (*model.Group, error) {
	groupKey := model.GroupPath(groupID)
	m := model.GroupData{}
	_, err := kv.GetMsg(ctx, s.store, model.PartitionKey, groupKey, &m)
	if err != nil {
		if errors.Is(err, kv.ErrNotFound) {
			err = auth.ErrNotFound
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
			err = auth.ErrAlreadyExists
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
	if params.Amount < 0 || params.Amount > auth.MaxPage {
		params.Amount = auth.MaxPage
	}

	hasMoreGroups := true
	afterGroup := params.After
	var userGroups []*model.Group
	resPaginator := model.Paginator{Amount: 0, NextPageToken: ""}
	for hasMoreGroups && len(userGroups) <= params.Amount {
		groups, paginator, err := s.ListGroups(ctx, &model.PaginationParams{Prefix: params.Prefix, After: afterGroup, Amount: auth.MaxPage})
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
				err = auth.ErrNotFound
			}
			return err
		}
		return nil
	}

	// create policy only if it does not exist
	err := kv.SetMsgIf(ctx, s.store, model.PartitionKey, policyKey, m, nil)
	if err != nil {
		if errors.Is(err, kv.ErrPredicateFailed) {
			err = auth.ErrAlreadyExists
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
			err = auth.ErrNotFound
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
		return nil, auth.ErrInvalidAccessKeyID
	}
	if len(secretAccessKey) == 0 {
		return nil, auth.ErrInvalidSecretAccessKey
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
			err = auth.ErrAlreadyExists
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
			err = auth.ErrAlreadyExists
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
			err = auth.ErrNotFound
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
		return nil, fmt.Errorf("credentials %w", auth.ErrNotFound)
	})
}

func (s *AuthService) Authorize(ctx context.Context, req *auth.AuthorizationRequest) (*auth.AuthorizationResponse, error) {
	policies, _, err := s.ListEffectivePolicies(ctx, req.Username, &model.PaginationParams{
		After:  "", // all
		Amount: -1, // all
	})
	if err != nil {
		return nil, err
	}

	allowed := auth.CheckPermissions(ctx, req.RequiredPermissions, req.Username, policies)

	if allowed != auth.CheckAllow {
		return &auth.AuthorizationResponse{
			Allowed: false,
			Error:   auth.ErrInsufficientPermissions,
		}, nil
	}

	// we're allowed!
	return &auth.AuthorizationResponse{Allowed: true}, nil
}

func (s *AuthService) ClaimTokenIDOnce(_ context.Context, _ string, _ int64) error {
	return auth.ErrNotImplemented
}

func (s *AuthService) IsExternalPrincipalsEnabled(_ context.Context) bool {
	return false
}

func (s *AuthService) CreateUserExternalPrincipal(_ context.Context, _, _ string) error {
	return auth.ErrNotImplemented
}

func (s *AuthService) DeleteUserExternalPrincipal(_ context.Context, _, _ string) error {
	return auth.ErrNotImplemented
}

func (s *AuthService) GetExternalPrincipal(_ context.Context, _ string) (*model.ExternalPrincipal, error) {
	return nil, auth.ErrNotImplemented
}

func (s *AuthService) ListUserExternalPrincipals(_ context.Context, _ string, _ *model.PaginationParams) ([]*model.ExternalPrincipal, *model.Paginator, error) {
	return nil, nil, auth.ErrNotImplemented
}

func (s *AuthService) getSetupTimestamp(ctx context.Context) (time.Time, error) {
	valWithPred, err := s.store.Get(ctx, []byte(ServerPartitionKey), []byte(auth.SetupTimestampKeyName))
	if err != nil {
		return time.Time{}, err
	}
	return time.Parse(time.RFC3339, string(valWithPred.Value))
}

func (s *AuthService) updateSetupTimestamp(ctx context.Context, ts time.Time) error {
	return s.store.SetIf(ctx, []byte(ServerPartitionKey), []byte(model.MetadataKeyPath(auth.SetupTimestampKeyName)), []byte(ts.UTC().Format(time.RFC3339)), nil)
}
