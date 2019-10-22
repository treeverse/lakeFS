package auth

import (
	"fmt"
	"time"
	"versio-index/auth/model"
	"versio-index/db"

	"golang.org/x/xerrors"

	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
)

const (
	SubspaceAuthKeys   = "keys"
	SubspacesAuthUsers = "users"
	SubspacesAuthRoles = "roles"
)

var (
	AuthenticationError = xerrors.New("authentication failure")
	AuthorizationError  = xerrors.New("authorization failure")
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
	CreateAppCredentials(application *model.Application) (*model.APICredentials, error)
	CreateUserCredentials(user *model.User) (*model.APICredentials, error)
	GetAPICredentials(accessKey string) (*model.APICredentials, error)
	Authenticate(req *AuthenticationRequest) (*AuthenticationResponse, error)
	Authorize(req *AuthorizationRequest) (*AuthorizationResponse, error)
}

type KVAuthService struct {
	kv db.Store
}

func NewKVAuthService(kv db.Store) *KVAuthService {
	return &KVAuthService{kv: kv}
}

func genAccessKeyId() string {
	key := KeyGenerator(14)
	return fmt.Sprintf("%s%s%s", "AKIAJ", key, "Q")
}

func genAccessSecretKey() string {
	return Base64StringGenerator(30)
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
	secretKey := genAccessSecretKey() // TODO: Encrypt this?
	creds, err := s.kv.Transact([]tuple.TupleElement{}, func(q db.Query) (interface{}, error) {
		creds := &model.APICredentials{
			AccessKeyId:     accessKey,
			AccessSecretKey: secretKey,
			CredentialType:  model.APICredentials_CREDENTIAL_TYPE_USER,
			ClientId:        user.GetClientId(),
			EntityId:        user.GetId(),
			IssuedDate:      now,
		}
		err := q.SetProto(creds, s.kv.Space(SubspaceAuthKeys), accessKey)
		return creds, err
	})
	return creds.(*model.APICredentials), err
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
	s.kv.ReadTransact([]tuple.TupleElement{req.ClientID}, func(q db.ReadQuery) (interface{}, error) {
		// get the user
		user := &model.User{}
		err := q.GetAsProto(user, s.kv.Space(SubspacesAuthUsers), req.UserID)
		if err != nil {
			return nil, err
		}
		// resolve all user roles and user group roles
		roles := make(map[string]*model.Role)

		// get roles and check them
		for _, rid := range user.GetRoles() {
			role := &model.Role{}
			err := q.GetAsProto(role, s.kv.Space(SubspacesAuthRoles), rid)
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

		groupIds := user.GetGroups()
		for _, g := range groupIds {
			group := &model.Group{}
			err := q.GetAsProto(group, s.kv.Space(SubspacesAuthUsers), g)
			if err != nil {
				return nil, err
			}
		}

		return nil, nil
	})
	return nil, nil
}
