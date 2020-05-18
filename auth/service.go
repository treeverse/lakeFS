package auth

import (
	"fmt"
	"strings"
	"time"

	"github.com/treeverse/lakefs/logging"

	"github.com/treeverse/lakefs/auth/crypt"
	"github.com/treeverse/lakefs/auth/model"
	"github.com/treeverse/lakefs/db"
	"github.com/treeverse/lakefs/permissions"
)

var (
	ErrClientNotFound = fmt.Errorf("client: %w", db.ErrNotFound)
	ErrUserNotFound   = fmt.Errorf("user: %w", db.ErrNotFound)
	ErrGroupNotFound  = fmt.Errorf("group: %w", db.ErrNotFound)
	ErrRoleNotFound   = fmt.Errorf("role: %w", db.ErrNotFound)
)

// AuthenticationRequest/AuthenticationResponse are used for user login information
// This is encapsulated in the auth module to allow password management
type AuthenticationRequest struct {
}

type AuthenticationResponse struct {
}

type AuthorizationRequest struct {
	UserID     int
	Permission permissions.Permission
	SubjectARN string
}

type AuthorizationResponse struct {
	Allowed bool
	Error   error
}

type Service interface {
	SecretStore() crypt.SecretStore

	CreateUser(user *model.User) error
	CreateGroup(group *model.Group) error
	CreateRole(group *model.Role) error

	AssignRoleToUser(roleId, userId int) error
	AssignRoleToGroup(roleId, groupId int) error
	AssignPolicyToRole(roleId int, policy *model.Policy) error

	GetUser(userId int) (*model.User, error)
	GetFirstUser() (*model.User, error)
	GetGroup(groupId int) (*model.Group, error)
	GetRole(roleId int) (*model.Role, error)

	CreateAppCredentials(application *model.Application) (*model.Credential, error)
	CreateUserCredentials(user *model.User) (*model.Credential, error)
	GetAPICredentials(accessKey string) (*model.Credential, error)
	Authorize(req *AuthorizationRequest) (*AuthorizationResponse, error)
}

type DBAuthService struct {
	db          db.Database
	secretStore crypt.SecretStore
}

func NewDBAuthService(db db.Database, secretStore crypt.SecretStore) *DBAuthService {
	logging.Default().Info("initialized Auth service")
	return &DBAuthService{db: db, secretStore: secretStore}
}

func genAccessKeyId() string {
	key := KeyGenerator(14)
	return fmt.Sprintf("%s%s%s", "AKIAJ", key, "Q")
}

func genAccessSecretKey() string {
	return Base64StringGenerator(30)
}

func (s *DBAuthService) SecretStore() crypt.SecretStore {
	return s.secretStore
}

func (s *DBAuthService) decryptSecret(value []byte) (string, error) {
	decrypted, err := s.secretStore.Decrypt(value)
	if err != nil {
		return "", err
	}
	return string(decrypted), nil
}

func (s *DBAuthService) encryptSecret(secretAccessKey string) ([]byte, error) {
	encrypted, err := s.secretStore.Encrypt([]byte(secretAccessKey))
	if err != nil {
		return nil, err
	}
	return encrypted, nil
}

func (s *DBAuthService) CreateUser(user *model.User) error {
	_, err := s.db.Transact(func(tx db.Tx) (interface{}, error) {
		return nil, tx.Get(user, `INSERT INTO users (email, full_name) VALUES ($1, $2) RETURNING id`, user.Email, user.FullName)
	})
	return err
}

func (s *DBAuthService) CreateGroup(group *model.Group) error {
	_, err := s.db.Transact(func(tx db.Tx) (interface{}, error) {
		return nil, tx.Get(group, `INSERT INTO groups (display_name) VALUES ($1) RETURNING id`, group.DisplayName)
	})
	return err
}

func (s *DBAuthService) CreateRole(role *model.Role) error {
	_, err := s.db.Transact(func(tx db.Tx) (interface{}, error) {
		return nil, tx.Get(role, `INSERT INTO roles (display_name) VALUES ($1) RETURNING id`, role.DisplayName)
	})
	return err
}

func (s *DBAuthService) AssignPolicyToRole(roleId int, policy *model.Policy) error {
	_, err := s.db.Transact(func(tx db.Tx) (interface{}, error) {
		// create role
		err := tx.Get(policy, `INSERT INTO policies (permission, arn) VALUES ($1, $2) RETURNING id`,
			policy.Permission, policy.Arn)
		if err != nil {
			return nil, err
		}
		_, err = tx.Exec(`INSERT INTO role_policies (role_id, policy_id) VALUES ($1, $2)`,
			roleId, policy.Id)
		return nil, err
	})
	return err
}

func (s *DBAuthService) CreateAppCredentials(application *model.Application) (*model.Credential, error) {
	now := time.Now()
	accessKey := genAccessKeyId()
	secretKey := genAccessSecretKey() // TODO: Encrypt this?
	creds, err := s.db.Transact(func(tx db.Tx) (interface{}, error) {
		c := &model.Credential{
			AccessKeyId:     accessKey,
			AccessSecretKey: secretKey,
			Type:            model.CredentialTypeApplication,
			IssuedDate:      now,
			ApplicationId:   &application.Id,
		}
		_, err := tx.Exec(
			`INSERT INTO credentials (access_key_id, access_secret_key, credentials_type, issued_date, application_id)
					VALUES ($1, $2, $3, $4, $5)`,
			c.AccessKeyId,
			c.AccessSecretKey,
			c.Type,
			c.IssuedDate,
			c.ApplicationId,
		)
		return c, err
	})
	return creds.(*model.Credential), err
}

func (s *DBAuthService) CreateUserCredentials(user *model.User) (*model.Credential, error) {
	now := time.Now()
	accessKey := genAccessKeyId()
	secretKey := genAccessSecretKey()
	encryptedKey, err := s.encryptSecret(secretKey)
	if err != nil {
		return nil, err
	}
	creds, err := s.db.Transact(func(tx db.Tx) (interface{}, error) {
		c := &model.Credential{
			AccessKeyId:                   accessKey,
			AccessSecretKey:               secretKey,
			AccessSecretKeyEncryptedBytes: encryptedKey,
			Type:                          model.CredentialTypeUser,
			IssuedDate:                    now,
			UserId:                        &user.Id,
		}
		_, err := tx.Exec(
			`INSERT INTO credentials (access_key_id, access_secret_key, credentials_type, issued_date, user_id)
					VALUES ($1, $2, $3, $4, $5)`,
			c.AccessKeyId,
			encryptedKey,
			c.Type,
			c.IssuedDate,
			c.UserId,
		)
		return c, err
	})
	if err != nil {
		return nil, err
	}
	return creds.(*model.Credential), err
}

func (s *DBAuthService) GetUser(userId int) (*model.User, error) {
	user, err := s.db.Transact(func(tx db.Tx) (interface{}, error) {
		user := &model.User{}
		err := tx.Get(user, `SELECT * FROM users WHERE id = $1`, userId)
		return user, err
	}, db.ReadOnly())
	if err != nil {
		return nil, err
	}
	return user.(*model.User), nil
}

func (s *DBAuthService) GetFirstUser() (*model.User, error) {
	user, err := s.db.Transact(func(tx db.Tx) (interface{}, error) {
		user := &model.User{}
		err := tx.Get(user, `SELECT * FROM users ORDER BY id ASC LIMIT 1`)
		return user, err
	}, db.ReadOnly(), db.WithLogger(logging.Dummy()))
	if err != nil {
		return nil, err
	}
	return user.(*model.User), nil
}

func (s *DBAuthService) GetGroup(groupId int) (*model.Group, error) {
	group, err := s.db.Transact(func(tx db.Tx) (interface{}, error) {
		group := &model.Group{}
		err := tx.Get(group, `SELECT * FROM groups WHERE id = $1`, groupId)
		return group, err
	}, db.ReadOnly())
	if err != nil {
		return nil, err
	}
	return group.(*model.Group), nil
}

func (s *DBAuthService) GetRole(roleId int) (*model.Role, error) {
	role, err := s.db.Transact(func(tx db.Tx) (interface{}, error) {
		role := &model.Role{}
		err := tx.Get(role, `SELECT * FROM roles WHERE id = $1`, roleId)
		return role, err
	}, db.ReadOnly())
	if err != nil {
		return nil, err
	}
	return role.(*model.Role), nil
}

func (s *DBAuthService) AssignRoleToUser(roleId, userId int) error {
	_, err := s.db.Transact(func(tx db.Tx) (interface{}, error) {
		_, err := tx.Exec(`INSERT INTO user_roles (user_id, role_id) VALUES ($1, $2)`, userId, roleId)
		return nil, err
	})

	return err
}

func (s *DBAuthService) AssignRoleToGroup(roleId, groupId int) error {
	_, err := s.db.Transact(func(tx db.Tx) (interface{}, error) {
		_, err := tx.Exec(`INSERT INTO group_roles (group_id, role_id) VALUES ($1, $2)`, groupId, roleId)
		return nil, err
	})

	return err
}

func (s *DBAuthService) GetAPICredentials(accessKey string) (*model.Credential, error) {
	credentials, err := s.db.Transact(func(tx db.Tx) (interface{}, error) {
		credentials := &model.Credential{}
		err := tx.Get(credentials, `SELECT * FROM credentials WHERE access_key_id = $1`, accessKey)
		if err != nil {
			return nil, err
		}
		key, err := s.decryptSecret(credentials.AccessSecretKeyEncryptedBytes)
		if err != nil {
			return nil, err
		}
		credentials.AccessSecretKey = key
		return credentials, nil
	}, db.ReadOnly())

	if err != nil {
		return nil, err
	}
	return credentials.(*model.Credential), nil
}

func (s *DBAuthService) Authorize(req *AuthorizationRequest) (*AuthorizationResponse, error) {
	resp, err := s.db.Transact(func(tx db.Tx) (interface{}, error) {
		// resolve all policies attached to roles attached to the user
		var err error
		var userPolicies []*model.Policy
		err = tx.Select(&userPolicies, `
			SELECT distinct policies.id, policies.arn, policies.permission FROM policies
			INNER JOIN role_policies ON (policies.id = role_policies.policy_id)
			INNER JOIN roles ON (roles.id = role_policies.role_id)
			INNER JOIN user_roles ON (roles.id = user_roles.role_id)
			WHERE user_roles.user_id = $1`, req.UserID)
		if err != nil {
			return nil, err
		}
		for _, p := range userPolicies {
			// get permissions....
			if strings.EqualFold(p.Permission, string(req.Permission)) && ArnMatch(p.Arn, req.SubjectARN) {
				return &AuthorizationResponse{
					Allowed: true,
					Error:   nil,
				}, nil
			}
		}

		// resolve all policies attached to roles attached to groups attached to the user
		var groupRoles []*model.Policy
		err = tx.Select(groupRoles, `
			SELECT distinct policies.id, policies.arn, policies.permission FROM policies
			INNER JOIN role_policies ON (policies.id = role_policies.policy_id)
			INNER JOIN roles ON (roles.id = role_policies.role_id)
			INNER JOIN group_roles ON (roles.id = group_roles.role_id)
			INNER JOIN groups ON (groups.id = group_roles.group_id)
			INNER JOIN user_groups ON (user_groups.group_id = groups.id) 
			WHERE user_groups.user_id = $2`, req.UserID)
		if err != nil {
			return nil, err
		}
		for _, p := range userPolicies {
			// get permissions....
			if strings.EqualFold(p.Permission, string(req.Permission)) && ArnMatch(p.Arn, req.SubjectARN) {
				return &AuthorizationResponse{
					Allowed: true,
					Error:   nil,
				}, nil
			}
		}

		// otherwise, no permission
		return &AuthorizationResponse{
			Allowed: false,
			Error:   ErrInsufficientPermissions,
		}, nil
	}, db.ReadOnly())
	if err != nil {
		return nil, err
	}
	return resp.(*AuthorizationResponse), nil
}
