package auth

import (
	"fmt"
	"time"
	"versio-index/auth/model"
	"versio-index/db"

	"github.com/apple/foundationdb/bindings/go/src/fdb/directory"

	"github.com/apple/foundationdb/bindings/go/src/fdb/subspace"

	"github.com/apple/foundationdb/bindings/go/src/fdb"

	"golang.org/x/xerrors"

	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
)

const (
	SubspaceClients    = "clients"
	SubspaceAuthKeys   = "keys"
	SubspacesAuthUsers = "users"
	SubspaceAuthGroups = "groups"
	SubspacesAuthRoles = "roles"
)

var (
	ErrClientNotFound = xerrors.Errorf("client: %w", db.ErrNotFound)
	ErrUserNotFound   = xerrors.Errorf("user: %w", db.ErrNotFound)
	ErrGroupNotFound  = xerrors.Errorf("group: %w", db.ErrNotFound)
	ErrRoleNotFound   = xerrors.Errorf("role: %w", db.ErrNotFound)
)

// AuthenticationRequest/AuthenticationResponse are used for user login information
// This is encapsulated in the auth module to allow password management
type AuthenticationRequest struct {
}

type AuthenticationResponse struct {
}

type AuthorizationRequest struct {
	ClientID   string
	UserID     string
	Intent     model.Permission_Intent
	SubjectARN string
}

type AuthorizationResponse struct {
	Allowed bool
	Error   error
}

type Service interface {
	CreateClient(*model.Client) error
	CreateUser(user *model.User) error
	CreateGroup(group *model.Group) error
	CreateRole(group *model.Role) error

	AssignRoleToUser(clientId, roleId, userId string) error
	AssignRoleToGroup(clientId, roleId, groupId string) error

	GetClient(clientId string) (*model.Client, error)
	GetUser(clientId, userId string) (*model.User, error)
	GetGroup(clientId, groupId string) (*model.Group, error)
	GetRole(clientId, roleId string) (*model.Role, error)

	CreateAppCredentials(application *model.Application) (*model.APICredentials, error)
	CreateUserCredentials(user *model.User) (*model.APICredentials, error)
	GetAPICredentials(accessKey string) (*model.APICredentials, error)
	Authenticate(req *AuthenticationRequest) (*AuthenticationResponse, error)
	Authorize(req *AuthorizationRequest) (*AuthorizationResponse, error)
}

type KVAuthService struct {
	kv db.Store
}

func NewKVAuthService(database fdb.Database, dir directory.DirectorySubspace) *KVAuthService {
	return &KVAuthService{kv: db.NewFDBStore(database, map[string]subspace.Subspace{
		SubspaceClients:    dir.Sub(SubspaceClients),
		SubspaceAuthKeys:   dir.Sub(SubspaceAuthKeys),
		SubspacesAuthUsers: dir.Sub(SubspacesAuthUsers),
		SubspaceAuthGroups: dir.Sub(SubspaceAuthGroups),
		SubspacesAuthRoles: dir.Sub(SubspacesAuthRoles),
	})}
}

func genAccessKeyId() string {
	key := KeyGenerator(14)
	return fmt.Sprintf("%s%s%s", "AKIAJ", key, "Q")
}

func genAccessSecretKey() string {
	return Base64StringGenerator(30)
}

func (s *KVAuthService) CreateClient(client *model.Client) error {
	_, err := s.kv.Transact([]tuple.TupleElement{}, func(q db.Query) (interface{}, error) {
		err := q.SetProto(client, s.kv.Space(SubspaceClients), client.GetId())
		if err != nil {
			return nil, err
		}
		return nil, nil
	})
	return err
}
func (s *KVAuthService) CreateUser(user *model.User) error {
	_, err := s.kv.Transact([]tuple.TupleElement{}, func(q db.Query) (interface{}, error) {
		err := q.SetProto(user, s.kv.Space(SubspacesAuthUsers), user.GetClientId(), user.GetId())
		if err != nil {
			return nil, err
		}
		return nil, nil
	})
	return err
}
func (s *KVAuthService) CreateGroup(group *model.Group) error {
	_, err := s.kv.Transact([]tuple.TupleElement{}, func(q db.Query) (interface{}, error) {
		err := q.SetProto(group, s.kv.Space(SubspaceAuthGroups), group.GetClientId(), group.GetId())
		if err != nil {
			return nil, err
		}
		return nil, nil
	})
	return err
}

func (s *KVAuthService) CreateRole(role *model.Role) error {
	_, err := s.kv.Transact([]tuple.TupleElement{}, func(q db.Query) (interface{}, error) {
		err := q.SetProto(role, s.kv.Space(SubspacesAuthRoles), role.GetClientId(), role.GetId())
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
	creds, err := s.kv.Transact([]tuple.TupleElement{}, func(q db.Query) (interface{}, error) {
		creds := &model.APICredentials{
			AccessKeyId:     accessKey,
			AccessSecretKey: secretKey,
			CredentialType:  model.APICredentials_CREDENTIAL_TYPE_APPLICATION,
			ClientId:        application.GetClientId(),
			EntityId:        application.GetId(),
			IssuedDate:      now,
		}
		err := q.SetProto(creds, s.kv.Space(SubspaceAuthKeys), accessKey)
		return creds, err
	})
	return creds.(*model.APICredentials), err
}

func (s *KVAuthService) CreateUserCredentials(user *model.User) (*model.APICredentials, error) {
	now := time.Now().Unix()
	accessKey := genAccessKeyId()
	secretKey := genAccessSecretKey() // TODO: Encrypt this before saving, probably with the client ID as part of the salt
	creds, err := s.kv.Transact([]tuple.TupleElement{}, func(q db.Query) (interface{}, error) {
		// ensure user exists
		_, err := q.Get(s.kv.Space(SubspacesAuthUsers), user.GetClientId(), user.GetId()).Get()
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
			ClientId:        user.GetClientId(),
			EntityId:        user.GetId(),
			IssuedDate:      now,
		}
		err = q.SetProto(creds, s.kv.Space(SubspaceAuthKeys), accessKey)
		return creds, err
	})
	return creds.(*model.APICredentials), err
}

func (s *KVAuthService) GetClient(clientId string) (*model.Client, error) {
	client, err := s.kv.ReadTransact([]tuple.TupleElement{}, func(q db.ReadQuery) (interface{}, error) {
		client := &model.Client{}
		return client, q.GetAsProto(client, s.kv.Space(SubspaceClients), clientId)
	})
	if xerrors.Is(err, db.ErrNotFound) {
		return nil, ErrClientNotFound
	}
	if err != nil {
		return nil, err
	}
	return client.(*model.Client), nil
}

func (s *KVAuthService) GetUser(clientId, userId string) (*model.User, error) {
	user, err := s.kv.ReadTransact([]tuple.TupleElement{}, func(q db.ReadQuery) (interface{}, error) {
		client := &model.Client{}
		err := q.GetAsProto(client, s.kv.Space(SubspaceClients), clientId)
		if xerrors.Is(err, db.ErrNotFound) {
			return nil, ErrClientNotFound
		}
		if err != nil {
			return nil, err
		}
		user := &model.User{}
		err = q.GetAsProto(user, s.kv.Space(SubspacesAuthUsers), clientId, userId)
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

func (s *KVAuthService) GetGroup(clientId, groupId string) (*model.Group, error) {
	group, err := s.kv.ReadTransact([]tuple.TupleElement{}, func(q db.ReadQuery) (interface{}, error) {
		client := &model.Client{}
		err := q.GetAsProto(client, s.kv.Space(SubspaceClients), clientId)
		if xerrors.Is(err, db.ErrNotFound) {
			return nil, ErrClientNotFound
		}
		if err != nil {
			return nil, err
		}
		group := &model.Group{}
		err = q.GetAsProto(group, s.kv.Space(SubspaceAuthGroups), clientId, groupId)
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

func (s *KVAuthService) GetRole(clientId, roleId string) (*model.Role, error) {
	role, err := s.kv.ReadTransact([]tuple.TupleElement{}, func(q db.ReadQuery) (interface{}, error) {
		client := &model.Client{}
		err := q.GetAsProto(client, s.kv.Space(SubspaceClients), clientId)
		if xerrors.Is(err, db.ErrNotFound) {
			return nil, ErrClientNotFound
		}
		if err != nil {
			return nil, err
		}
		role := &model.Role{}
		err = q.GetAsProto(role, s.kv.Space(SubspacesAuthRoles), clientId, roleId)
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

func (s *KVAuthService) AssignRoleToUser(clientId, roleId, userId string) error {
	_, err := s.kv.Transact([]tuple.TupleElement{}, func(q db.Query) (interface{}, error) {
		// get user
		user := &model.User{}
		err := q.GetAsProto(user, s.kv.Space(SubspacesAuthUsers), clientId, userId)
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
		return nil, q.SetProto(user, s.kv.Space(SubspacesAuthUsers), clientId, userId)
	})
	return err
}

func (s *KVAuthService) AssignRoleToGroup(clientId, roleId, groupId string) error {
	_, err := s.kv.Transact([]tuple.TupleElement{}, func(q db.Query) (interface{}, error) {
		// get user
		group := &model.Group{}
		err := q.GetAsProto(group, s.kv.Space(SubspaceAuthGroups), clientId, groupId)
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
		return nil, q.SetProto(group, s.kv.Space(SubspaceAuthGroups), clientId, groupId)
	})
	return err
}

func (s *KVAuthService) GetAPICredentials(accessKey string) (*model.APICredentials, error) {
	credentials, err := s.kv.ReadTransact([]tuple.TupleElement{}, func(q db.ReadQuery) (interface{}, error) {
		// get the key pair for this key
		creds := &model.APICredentials{}
		err := q.GetAsProto(creds, s.kv.Space(SubspaceAuthKeys), accessKey)
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
	resp, err := s.kv.ReadTransact([]tuple.TupleElement{req.ClientID}, func(q db.ReadQuery) (interface{}, error) {
		// get the user
		user := &model.User{}
		err := q.GetAsProto(user, s.kv.Space(SubspacesAuthUsers), req.UserID)
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
			err := q.GetAsProto(role, s.kv.Space(SubspacesAuthRoles), rid)
			if xerrors.Is(err, db.ErrNotFound) {
				return nil, ErrRoleNotFound
			}
			if err != nil {
				return nil, err
			}
			roles[rid] = role
			for _, p := range role.GetPermissions() {
				// get permissions....
				if p.Intent == req.Intent && ArnMatch(p.SubjectArn, req.SubjectARN) {
					return &AuthorizationResponse{
						Allowed: true,
						Error:   nil,
					}, nil
				}
			}
		}

		// get all groups together
		groupIds := user.GetGroups()
		for _, g := range groupIds {
			group := &model.Group{}
			err := q.GetAsProto(group, s.kv.Space(SubspaceAuthGroups), g)
			if xerrors.Is(err, db.ErrNotFound) {
				return nil, ErrGroupNotFound
			}
			if err != nil {
				return nil, err
			}
			// get roles if we don't already have them from the user
			for _, rid := range group.GetRoles() {
				if _, exists := roles[rid]; exists {
					continue // we've already testsed that one
				}
				role := &model.Role{}
				err := q.GetAsProto(role, s.kv.Space(SubspacesAuthRoles), rid)
				if xerrors.Is(err, db.ErrNotFound) {
					return nil, ErrRoleNotFound
				}
				if err != nil {
					return nil, err
				}
				roles[rid] = role
				for _, p := range role.GetPermissions() {
					// get permissions....
					if p.Intent == req.Intent && ArnMatch(p.SubjectArn, req.SubjectARN) {
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
