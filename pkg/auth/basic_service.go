package auth

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/treeverse/lakefs/pkg/auth/crypt"
	"github.com/treeverse/lakefs/pkg/auth/keys"
	"github.com/treeverse/lakefs/pkg/auth/model"
	"github.com/treeverse/lakefs/pkg/auth/params"
	"github.com/treeverse/lakefs/pkg/kv"
	"github.com/treeverse/lakefs/pkg/logging"
	"google.golang.org/protobuf/proto"
)

const (
	basicPartitionKey     = "basicAuth"
	SuperAdminKey         = "superAdmin"
	MaxCredentialsPerUser = 1
)

type BasicAuthService struct {
	store       kv.Store
	secretStore crypt.SecretStore
	cache       Cache
	log         logging.Logger
}

func NewBasicAuthService(store kv.Store, secretStore crypt.SecretStore, cacheConf params.ServiceCache, logger logging.Logger) *BasicAuthService {
	logger.Info("initialized Auth service")
	var cache Cache
	if cacheConf.Enabled {
		cache = NewLRUCache(cacheConf.Size, cacheConf.TTL, cacheConf.Jitter)
	} else {
		cache = &DummyCache{}
	}
	res := &BasicAuthService{
		store:       store,
		secretStore: secretStore,
		cache:       cache,
		log:         logger,
	}
	return res
}

func (s *BasicAuthService) Authorize(ctx context.Context, req *AuthorizationRequest) (*AuthorizationResponse, error) {
	_, err := s.GetUser(ctx, req.Username)
	if err != nil {
		return nil, err
	}

	// If user exists - single admin user - allow
	return &AuthorizationResponse{Allowed: true}, nil
}

func (s *BasicAuthService) GetUser(ctx context.Context, username string) (*model.User, error) {
	user, err := s.getUser(ctx)
	if err != nil {
		return nil, err
	}
	// After fetching SuperAdmin, verify equals to the username requested
	if user.Username != username {
		return nil, kv.ErrNotFound
	}
	return user, nil
}

// getUser - returns the only existing user in DB
func (s *BasicAuthService) getUser(ctx context.Context) (*model.User, error) {
	return s.cache.GetUser(UserKey{Username: SuperAdminKey}, func() (*model.User, error) {
		// Single user, always stored under this key!
		userKey := model.UserPath(SuperAdminKey)
		m := model.UserData{}
		_, err := kv.GetMsg(ctx, s.store, basicPartitionKey, userKey, &m)
		if err != nil {
			if errors.Is(err, kv.ErrNotFound) {
				err = ErrNotFound
			}
			return nil, fmt.Errorf("%s: %w", userKey, err)
		}

		user := model.UserFromProto(&m)
		return user, nil
	})
}

func (s *BasicAuthService) CreateUser(ctx context.Context, user *model.User) (string, error) {
	if err := model.ValidateAuthEntityID(user.Username); err != nil {
		return InvalidUserID, err
	}
	userKey := model.UserPath(SuperAdminKey)

	err := kv.SetMsgIf(ctx, s.store, basicPartitionKey, userKey, model.ProtoFromUser(user), nil)
	if err != nil {
		if errors.Is(err, kv.ErrPredicateFailed) {
			err = ErrAlreadyExists
		}
		return "", fmt.Errorf("failed to create user (auth.UserKey %s): %w", userKey, err)
	}
	return user.Username, err
}

func (s *BasicAuthService) ListUsers(ctx context.Context, _ *model.PaginationParams) ([]*model.User, *model.Paginator, error) {
	user, err := s.getUser(ctx)
	if err != nil {
		return nil, nil, err
	}
	return []*model.User{user}, &model.Paginator{Amount: 1}, nil
}

func (s *BasicAuthService) GetCredentials(ctx context.Context, accessKeyID string) (*model.Credential, error) {
	return s.cache.GetCredential(accessKeyID, func() (*model.Credential, error) {
		m := &model.UserData{}
		itr, err := kv.NewPrimaryIterator(ctx, s.store, m.ProtoReflect().Type(), basicPartitionKey, model.UserPath(""), kv.IteratorOptionsAfter([]byte("")))
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
			_, err := kv.GetMsg(ctx, s.store, basicPartitionKey, credentialsKey, &c)
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

func (s *BasicAuthService) CreateCredentials(ctx context.Context, username string) (*model.Credential, error) {
	accessKeyID := keys.GenAccessKeyID()
	secretAccessKey := keys.GenSecretAccessKey()
	user, err := s.GetUser(ctx, username)
	if err != nil {
		return nil, err
	}
	return s.AddCredentials(ctx, user.Username, accessKeyID, secretAccessKey)
}

func (s *BasicAuthService) AddCredentials(ctx context.Context, username, accessKeyID, secretAccessKey string) (*model.Credential, error) {
	currCreds, err := s.listUserCredentials(ctx, username)
	if err != nil {
		return nil, err
	}
	// TODO (niro): Support swap?
	if len(currCreds) >= MaxCredentialsPerUser {
		return nil, fmt.Errorf("exceeded number of allowed credentials: %w", ErrInvalidRequest)
	}
	return s.addCredentials(ctx, username, accessKeyID, secretAccessKey)
}

func (s *BasicAuthService) addCredentials(ctx context.Context, username, accessKeyID, secretAccessKey string) (*model.Credential, error) {
	encryptedKey, err := model.EncryptSecret(s.secretStore, secretAccessKey)
	if err != nil {
		return nil, err
	}
	now := time.Now()
	c := &model.Credential{
		BaseCredential: model.BaseCredential{
			AccessKeyID:                   accessKeyID,
			SecretAccessKey:               secretAccessKey,
			SecretAccessKeyEncryptedBytes: encryptedKey,
			IssuedDate:                    now,
		},
		Username: username,
	}
	credentialsKey := model.CredentialPath(username, c.AccessKeyID)
	err = kv.SetMsgIf(ctx, s.store, basicPartitionKey, credentialsKey, model.ProtoFromCredential(c), nil)
	if err != nil {
		if errors.Is(err, kv.ErrPredicateFailed) {
			err = ErrAlreadyExists
		}
		return nil, fmt.Errorf("save credentials (credentialsKey %s): %w", credentialsKey, err)
	}

	return c, nil
}

func (s *BasicAuthService) listUserCredentials(ctx context.Context, username string) ([]*model.Credential, error) {
	var credential model.CredentialData
	credentialsKey := model.CredentialPath(username, "")
	var (
		it  kv.MessageIterator
		err error
	)
	it, err = kv.NewSecondaryIterator(ctx, s.store, (&credential).ProtoReflect().Type(), basicPartitionKey, credentialsKey, []byte(""))
	if err != nil {
		return nil, fmt.Errorf("create iterator: %w", err)
	}
	defer it.Close()

	amount := 2
	entries := make([]proto.Message, 0)
	for len(entries) < amount && it.Next() {
		entry := it.Entry()
		value := entry.Value
		entries = append(entries, value)
	}
	if err = it.Err(); err != nil {
		return nil, fmt.Errorf("iterate credentials: %w", err)
	}

	creds, err := model.ConvertCredDataList(s.secretStore, entries)
	if err != nil {
		return nil, err
	}
	return creds, nil
}

func (s *BasicAuthService) Cache() Cache {
	return s.cache
}

func (s *BasicAuthService) SecretStore() crypt.SecretStore {
	return s.secretStore
}

func (s *BasicAuthService) DeleteUser(_ context.Context, _ string) error {
	return ErrNotImplemented
}

func (s *BasicAuthService) GetUserByID(_ context.Context, _ string) (*model.User, error) {
	return nil, ErrNotImplemented
}

func (s *BasicAuthService) GetUserByExternalID(_ context.Context, _ string) (*model.User, error) {
	return nil, ErrNotImplemented
}

func (s *BasicAuthService) GetUserByEmail(_ context.Context, _ string) (*model.User, error) {
	return nil, ErrNotImplemented
}

func (s *BasicAuthService) UpdateUserFriendlyName(_ context.Context, _ string, _ string) error {
	return ErrNotImplemented
}

func (s *BasicAuthService) IsExternalPrincipalsEnabled(_ context.Context) bool {
	return false
}

func (s *BasicAuthService) CreateUserExternalPrincipal(_ context.Context, _, _ string) error {
	return ErrNotImplemented
}

func (s *BasicAuthService) DeleteUserExternalPrincipal(_ context.Context, _, _ string) error {
	return ErrNotImplemented
}

func (s *BasicAuthService) GetExternalPrincipal(_ context.Context, _ string) (*model.ExternalPrincipal, error) {
	return nil, ErrNotImplemented
}

func (s *BasicAuthService) ListUserExternalPrincipals(_ context.Context, _ string, _ *model.PaginationParams) ([]*model.ExternalPrincipal, *model.Paginator, error) {
	return nil, nil, ErrNotImplemented
}

func (s *BasicAuthService) CreateGroup(_ context.Context, _ *model.Group) (*model.Group, error) {
	return nil, ErrNotImplemented
}

func (s *BasicAuthService) DeleteGroup(_ context.Context, _ string) error {
	return ErrNotImplemented
}

func (s *BasicAuthService) GetGroup(_ context.Context, _ string) (*model.Group, error) {
	return nil, ErrNotImplemented
}

func (s *BasicAuthService) ListGroups(_ context.Context, _ *model.PaginationParams) ([]*model.Group, *model.Paginator, error) {
	return nil, nil, ErrNotImplemented
}

func (s *BasicAuthService) AddUserToGroup(_ context.Context, _, _ string) error {
	return ErrNotImplemented
}

func (s *BasicAuthService) RemoveUserFromGroup(_ context.Context, _, _ string) error {
	return ErrNotImplemented
}

func (s *BasicAuthService) ListUserGroups(_ context.Context, _ string, _ *model.PaginationParams) ([]*model.Group, *model.Paginator, error) {
	return nil, nil, ErrNotImplemented
}

func (s *BasicAuthService) ListGroupUsers(_ context.Context, _ string, _ *model.PaginationParams) ([]*model.User, *model.Paginator, error) {
	return nil, nil, ErrNotImplemented
}

func (s *BasicAuthService) WritePolicy(_ context.Context, _ *model.Policy, _ bool) error {
	return ErrNotImplemented
}

func (s *BasicAuthService) GetPolicy(_ context.Context, _ string) (*model.Policy, error) {
	return nil, ErrNotImplemented
}

func (s *BasicAuthService) DeletePolicy(_ context.Context, _ string) error {
	return ErrNotImplemented
}

func (s *BasicAuthService) ListPolicies(_ context.Context, _ *model.PaginationParams) ([]*model.Policy, *model.Paginator, error) {
	return nil, nil, ErrNotImplemented
}

func (s *BasicAuthService) DeleteCredentials(_ context.Context, _, _ string) error {
	return ErrNotImplemented
}

func (s *BasicAuthService) GetCredentialsForUser(_ context.Context, _, _ string) (*model.Credential, error) {
	return nil, ErrNotImplemented
}

func (s *BasicAuthService) ListUserCredentials(_ context.Context, _ string, _ *model.PaginationParams) ([]*model.Credential, *model.Paginator, error) {
	return nil, nil, ErrNotImplemented
}

func (s *BasicAuthService) AttachPolicyToUser(_ context.Context, _, _ string) error {
	return ErrNotImplemented
}

func (s *BasicAuthService) DetachPolicyFromUser(_ context.Context, _, _ string) error {
	return ErrNotImplemented
}

func (s *BasicAuthService) ListUserPolicies(_ context.Context, _ string, _ *model.PaginationParams) ([]*model.Policy, *model.Paginator, error) {
	return nil, nil, ErrNotImplemented
}

func (s *BasicAuthService) ListEffectivePolicies(_ context.Context, _ string, _ *model.PaginationParams) ([]*model.Policy, *model.Paginator, error) {
	return nil, nil, ErrNotImplemented
}

func (s *BasicAuthService) AttachPolicyToGroup(_ context.Context, _, _ string) error {
	return ErrNotImplemented
}

func (s *BasicAuthService) DetachPolicyFromGroup(_ context.Context, _, _ string) error {
	return ErrNotImplemented
}

func (s *BasicAuthService) ListGroupPolicies(context.Context, string, *model.PaginationParams) ([]*model.Policy, *model.Paginator, error) {
	return nil, nil, ErrNotImplemented
}

func (s *BasicAuthService) ClaimTokenIDOnce(_ context.Context, _ string, _ int64) error {
	return ErrNotImplemented
}

func (s *BasicAuthService) InviteUser(context.Context, string) error {
	return ErrNotImplemented
}
