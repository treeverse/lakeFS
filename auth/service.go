package auth

import (
	"fmt"
	"strings"
	"time"
	"treeverse-lake/auth/model"
	"treeverse-lake/db"

	"github.com/dgraph-io/badger"

	"golang.org/x/xerrors"
)

var (
	ErrClientNotFound = xerrors.Errorf("client: %w", db.ErrNotFound)
	ErrUserNotFound   = xerrors.Errorf("user: %w", db.ErrNotFound)
	ErrGroupNotFound  = xerrors.Errorf("group: %w", db.ErrNotFound)
	ErrRoleNotFound   = xerrors.Errorf("role: %w", db.ErrNotFound)

	SubspaceClients    = db.Namespace("clients")
	SubspaceAuthKeys   = db.Namespace("keys")
	SubspacesAuthUsers = db.Namespace("users")
	SubspaceAuthGroups = db.Namespace("groups")
	SubspacesAuthRoles = db.Namespace("roles")
)

// AuthenticationRequest/AuthenticationResponse are used for user login information
// This is encapsulated in the auth module to allow password management
type AuthenticationRequest struct {
}

type AuthenticationResponse struct {
}

type AuthorizationRequest struct {
	UserID     string
	Permission string
	SubjectARN string
}

type AuthorizationResponse struct {
	Allowed bool
	Error   error
}

type Service interface {
	CreateUser(user *model.User) error
	CreateGroup(group *model.Group) error
	CreateRole(group *model.Role) error

	AssignRoleToUser(roleId, userId string) error
	AssignRoleToGroup(roleId, groupId string) error

	GetUser(userId string) (*model.User, error)
	GetGroup(groupId string) (*model.Group, error)
	GetRole(roleId string) (*model.Role, error)

	CreateAppCredentials(application *model.Application) (*model.APICredentials, error)
	CreateUserCredentials(user *model.User) (*model.APICredentials, error)
	GetAPICredentials(accessKey string) (*model.APICredentials, error)
	Authenticate(req *AuthenticationRequest) (*AuthenticationResponse, error)
	Authorize(req *AuthorizationRequest) (*AuthorizationResponse, error)
}

type KVAuthService struct {
	kv db.Store
}

func NewKVAuthService(database *badger.DB) *KVAuthService {
	return &KVAuthService{kv: db.NewDBStore(database)}
}

func genAccessKeyId() string {
	key := KeyGenerator(14)
	return fmt.Sprintf("%s%s%s", "AKIAJ", key, "Q")
}

func genAccessSecretKey() string {
	return Base64StringGenerator(30)
}

func (s *KVAuthService) CreateUser(user *model.User) error {
	_, err := s.kv.Transact(func(q db.Query) (interface{}, error) {
		err := q.SetProto(user, SubspacesAuthUsers, db.CompositeStrings(user.GetId()))
		if err != nil {
			return nil, err
		}
		return nil, nil
	})
	return err
}
func (s *KVAuthService) CreateGroup(group *model.Group) error {
	_, err := s.kv.Transact(func(q db.Query) (interface{}, error) {
		err := q.SetProto(group, SubspaceAuthGroups, db.CompositeStrings(group.GetId()))
		if err != nil {
			return nil, err
		}
		return nil, nil
	})
	return err
}

func (s *KVAuthService) CreateRole(role *model.Role) error {
	_, err := s.kv.Transact(func(q db.Query) (interface{}, error) {
		err := q.SetProto(role, SubspacesAuthRoles, db.CompositeStrings(role.GetId()))
		if err != nil {
			return nil, err
		}
		return nil, nil
	})
	return err
}

func (s *KVAuthService) CreateAppCredentials(application *model.Application) (*model.APICredentials, error) {
	now := time.Now().Unix()
	accessKey := genAccessKeyId()
	secretKey := genAccessSecretKey() // TODO: Encrypt this?
	creds, err := s.kv.Transact(func(q db.Query) (interface{}, error) {
		creds := &model.APICredentials{
			AccessKeyId:     accessKey,
			AccessSecretKey: secretKey,
			CredentialType:  model.APICredentials_CREDENTIAL_TYPE_APPLICATION,
			EntityId:        application.GetId(),
			IssuedDate:      now,
		}
		err := q.SetProto(creds, SubspaceAuthKeys, db.CompositeStrings(accessKey))
		return creds, err
	})
	return creds.(*model.APICredentials), err
}

func (s *KVAuthService) CreateUserCredentials(user *model.User) (*model.APICredentials, error) {
	now := time.Now().Unix()
	accessKey := genAccessKeyId()
	secretKey := genAccessSecretKey() // TODO: Encrypt this before saving, probably with the client ID as part of the salt
	creds, err := s.kv.Transact(func(q db.Query) (interface{}, error) {
		// ensure user exists
		_, err := q.Get(SubspacesAuthUsers, db.CompositeStrings(user.GetId()))
		if xerrors.Is(err, db.ErrNotFound) {
			return nil, ErrUserNotFound
		}
		if err != nil {
			return nil, err // could not validate user
		}
		creds := &model.APICredentials{
			AccessKeyId:     accessKey,
			AccessSecretKey: secretKey,
			CredentialType:  model.APICredentials_CREDENTIAL_TYPE_USER,
			EntityId:        user.GetId(),
			IssuedDate:      now,
		}
		err = q.SetProto(creds, SubspaceAuthKeys, db.CompositeStrings(accessKey))
		return creds, err
	})
	return creds.(*model.APICredentials), err
}

func (s *KVAuthService) GetUser(userId string) (*model.User, error) {
	user, err := s.kv.ReadTransact(func(q db.ReadQuery) (interface{}, error) {
		user := &model.User{}
		err := q.GetAsProto(user, SubspacesAuthUsers, db.CompositeStrings(userId))
		if xerrors.Is(err, db.ErrNotFound) {
			return nil, ErrUserNotFound
		}
		if err != nil {
			return nil, err
		}
		return user, nil
	})
	if err != nil {
		return nil, err
	}
	return user.(*model.User), nil
}

func (s *KVAuthService) GetGroup(groupId string) (*model.Group, error) {
	group, err := s.kv.ReadTransact(func(q db.ReadQuery) (interface{}, error) {
		group := &model.Group{}
		err := q.GetAsProto(group, SubspaceAuthGroups, db.CompositeStrings(groupId))
		if xerrors.Is(err, db.ErrNotFound) {
			return nil, ErrGroupNotFound
		}
		if err != nil {
			return nil, err
		}
		return group, nil
	})
	if err != nil {
		return nil, err
	}
	return group.(*model.Group), nil
}

func (s *KVAuthService) GetRole(roleId string) (*model.Role, error) {
	role, err := s.kv.ReadTransact(func(q db.ReadQuery) (interface{}, error) {
		role := &model.Role{}
		err := q.GetAsProto(role, SubspacesAuthRoles, db.CompositeStrings(roleId))
		if xerrors.Is(err, db.ErrNotFound) {
			return nil, ErrRoleNotFound
		}
		if err != nil {
			return nil, err
		}
		return role, nil
	})
	if err != nil {
		return nil, err
	}
	return role.(*model.Role), nil
}

func (s *KVAuthService) AssignRoleToUser(roleId, userId string) error {
	_, err := s.kv.Transact(func(q db.Query) (interface{}, error) {
		// get user
		user := &model.User{}
		err := q.GetAsProto(user, SubspacesAuthUsers, db.CompositeStrings(userId))
		if xerrors.Is(err, db.ErrNotFound) {
			return nil, ErrUserNotFound
		}
		if err != nil {
			return nil, err
		}
		// get roles
		roles := user.Roles
		if roles == nil {
			roles = []string{}
		}
		user.Roles = append(roles, roleId)
		return nil, q.SetProto(user, SubspacesAuthUsers, db.CompositeStrings(userId))
	})
	return err
}

func (s *KVAuthService) AssignRoleToGroup(roleId, groupId string) error {
	_, err := s.kv.Transact(func(q db.Query) (interface{}, error) {
		// get user
		group := &model.Group{}
		err := q.GetAsProto(group, SubspaceAuthGroups, db.CompositeStrings(groupId))
		if xerrors.Is(err, db.ErrNotFound) {
			return nil, ErrGroupNotFound
		}
		if err != nil {
			return nil, err
		}
		// get roles
		roles := group.Roles
		if roles == nil {
			roles = []string{}
		}
		group.Roles = append(roles, roleId)
		return nil, q.SetProto(group, SubspaceAuthGroups, db.CompositeStrings(groupId))
	})
	return err
}

func (s *KVAuthService) GetAPICredentials(accessKey string) (*model.APICredentials, error) {
	credentials, err := s.kv.ReadTransact(func(q db.ReadQuery) (interface{}, error) {
		// get the key pair for this key
		creds := &model.APICredentials{}
		err := q.GetAsProto(creds, SubspaceAuthKeys, db.CompositeStrings(accessKey))
		if err != nil {
			return nil, err
		}
		// TODO: decrypt secret
		// return credential pair if it exists
		return creds, nil
	})
	if err != nil {
		return nil, err
	}
	return credentials.(*model.APICredentials), nil
}

func (s *KVAuthService) Authenticate(req *AuthenticationRequest) (*AuthenticationResponse, error) {
	panic("implement me")
}

func (s *KVAuthService) Authorize(req *AuthorizationRequest) (*AuthorizationResponse, error) {
	resp, err := s.kv.ReadTransact(func(q db.ReadQuery) (interface{}, error) {
		// get the user
		user := &model.User{}
		err := q.GetAsProto(user, SubspacesAuthUsers, db.CompositeStrings(req.UserID))
		if xerrors.Is(err, db.ErrNotFound) {
			return nil, ErrUserNotFound
		}
		if err != nil {
			return nil, err
		}
		// resolve all user roles and user group roles
		roles := make(map[string]*model.Role)

		// get roles and check them
		for _, rid := range user.GetRoles() {
			role := &model.Role{}
			err := q.GetAsProto(role, SubspacesAuthRoles, db.CompositeStrings(rid))
			if xerrors.Is(err, db.ErrNotFound) {
				return nil, ErrRoleNotFound
			}
			if err != nil {
				return nil, err
			}
			roles[rid] = role
			for _, p := range role.GetPolicies() {
				// get permissions....
				if strings.EqualFold(p.GetPermission(), req.Permission) && ArnMatch(p.GetArn(), req.SubjectARN) {
					return &AuthorizationResponse{
						Allowed: true,
						Error:   nil,
					}, nil
				}
			}
		}

		// get all groups together
		groupIds := user.GetGroups()
		for _, gid := range groupIds {
			group := &model.Group{}
			err := q.GetAsProto(group, SubspaceAuthGroups, db.CompositeStrings(gid))
			if xerrors.Is(err, db.ErrNotFound) {
				return nil, ErrGroupNotFound
			}
			if err != nil {
				return nil, err
			}
			// get roles if we don't already have them from the user
			for _, rid := range group.GetRoles() {
				if _, exists := roles[rid]; exists {
					continue // we've already tested that one
				}
				role := &model.Role{}
				err := q.GetAsProto(role, SubspacesAuthRoles, db.CompositeStrings(rid))
				if xerrors.Is(err, db.ErrNotFound) {
					return nil, ErrRoleNotFound
				}
				if err != nil {
					return nil, err
				}
				roles[rid] = role
				for _, p := range role.GetPolicies() {
					// get permissions....
					if strings.EqualFold(p.GetPermission(), req.Permission) && ArnMatch(p.GetArn(), req.SubjectARN) {
						return &AuthorizationResponse{
							Allowed: true,
							Error:   nil,
						}, nil
					}
				}
			}
		}

		return &AuthorizationResponse{
			Allowed: false,
			Error:   ErrInsufficientPermissions,
		}, nil
	})
	if err != nil {
		return nil, err
	}
	return resp.(*AuthorizationResponse), nil
}
