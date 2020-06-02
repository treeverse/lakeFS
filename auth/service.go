package auth

import (
	"fmt"
	"time"

	"github.com/treeverse/lakefs/logging"

	"github.com/treeverse/lakefs/auth/crypt"
	"github.com/treeverse/lakefs/auth/model"
	"github.com/treeverse/lakefs/db"
	"github.com/treeverse/lakefs/permissions"
)

type AuthorizationRequest struct {
	UserDisplayName string
	Permission      permissions.Permission
	SubjectARN      string
}

type AuthorizationResponse struct {
	Allowed bool
	Error   error
}

type Service interface {
	SecretStore() crypt.SecretStore

	// users
	CreateUser(user *model.User) error
	DeleteUser(userDisplayName string) error
	GetUserById(userId int) (*model.User, error)
	GetUser(userDisplayName string) (*model.User, error)
	GetFirstUser() (*model.User, error)
	ListUsers(params *model.PaginationParams) ([]*model.User, *model.Paginator, error)

	// groups
	CreateGroup(group *model.Group) error
	DeleteGroup(groupDisplayName string) error
	GetGroup(groupDisplayName string) (*model.Group, error)
	ListGroups(params *model.PaginationParams) ([]*model.Group, *model.Paginator, error)

	// roles
	CreateRole(role *model.Role) error
	DeleteRole(roleDisplayName string) error
	GetRole(roleDisplayName string) (*model.Role, error)
	ListRoles(params *model.PaginationParams) ([]*model.Role, *model.Paginator, error)

	// group<->user memberships
	AddUserToGroup(userDisplayName, groupDisplayName string) error
	RemoveUserFromGroup(userDisplayName, groupDisplayName string) error
	ListUserGroups(userDisplayName string, params *model.PaginationParams) ([]*model.Group, *model.Paginator, error)
	ListGroupUsers(groupDisplayName string, params *model.PaginationParams) ([]*model.User, *model.Paginator, error)

	// policies
	CreatePolicy(policy *model.Policy) error
	GetPolicy(policyDisplayName string) (*model.Policy, error)
	DeletePolicy(policyDisplayName string) error
	ListPolicies(params *model.PaginationParams) ([]*model.Policy, *model.Paginator, error)

	// credentials
	CreateCredentials(userDisplayName string) (*model.Credential, error)
	DeleteCredentials(userDisplayName, accessKeyId string) error
	GetCredentialsForUser(userDisplayName, accessKeyId string) (*model.Credential, error)
	GetCredentials(accessKeyId string) (*model.Credential, error)
	ListUserCredentials(userDisplayName string, params *model.PaginationParams) ([]*model.Credential, *model.Paginator, error)

	// role<->user attachments
	AttachRoleToUser(roleDisplayName, userDisplayName string) error
	DetachRoleFromUser(roleDisplayName, userDisplayName string) error
	ListUserRoles(userDisplayName string, params *model.PaginationParams) ([]*model.Role, *model.Paginator, error)

	// role<->group attachments
	AttachRoleToGroup(roleDisplayName, groupDisplayName string) error
	DetachRoleFromGroup(roleDisplayName, groupDisplayName string) error
	ListGroupRoles(groupDisplayName string, params *model.PaginationParams) ([]*model.Role, *model.Paginator, error)

	// policy<->role attachments
	AttachPolicyToRole(roleDisplayName string, policyDisplayName string) error
	DetachPolicyFromRole(roleDisplayName string, policyDisplayName string) error
	ListRolePolicies(roleDisplayName string, params *model.PaginationParams) ([]*model.Policy, *model.Paginator, error)

	// authorize user for an action
	Authorize(req *AuthorizationRequest) (*AuthorizationResponse, error)
}

func getUser(tx db.Tx, userDisplayName string) (*model.User, error) {
	user := &model.User{}
	err := tx.Get(user, `SELECT * FROM users WHERE display_name = $1`, userDisplayName)
	if err != nil {
		return nil, err
	}
	return user, nil
}

func deleteOrNotFound(tx db.Tx, stmt string, args ...interface{}) error {
	res, err := tx.Exec(stmt, args...)
	if err != nil {
		return err
	}
	numRows, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if numRows == 0 {
		return db.ErrNotFound
	}
	return nil
}

func genAccessKeyId() string {
	key := KeyGenerator(14)
	return fmt.Sprintf("%s%s%s", "AKIAJ", key, "Q")
}

func genAccessSecretKey() string {
	return Base64StringGenerator(30)
}

type DBAuthService struct {
	db          db.Database
	secretStore crypt.SecretStore
}

func NewDBAuthService(db db.Database, secretStore crypt.SecretStore) *DBAuthService {
	logging.Default().Info("initialized Auth service")
	return &DBAuthService{db: db, secretStore: secretStore}
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

func (s *DBAuthService) SecretStore() crypt.SecretStore {
	return s.secretStore
}

func (s *DBAuthService) CreateUser(user *model.User) error {
	_, err := s.db.Transact(func(tx db.Tx) (interface{}, error) {
		err := tx.Get(user, `INSERT INTO users (display_name, created_at) VALUES ($1, $2) RETURNING id`, user.DisplayName, user.CreatedAt)
		return nil, err
	})
	return err
}

func (s *DBAuthService) DeleteUser(userDisplayName string) error {
	_, err := s.db.Transact(func(tx db.Tx) (interface{}, error) {
		return nil, deleteOrNotFound(tx, `DELETE FROM users WHERE display_name = $1`, userDisplayName)
	})
	return err
}

func (s *DBAuthService) GetUser(userDisplayName string) (*model.User, error) {
	user, err := s.db.Transact(func(tx db.Tx) (interface{}, error) {
		return getUser(tx, userDisplayName)
	}, db.ReadOnly())
	if err != nil {
		return nil, err
	}
	return user.(*model.User), nil
}

func (s *DBAuthService) GetUserById(userId int) (*model.User, error) {
	user, err := s.db.Transact(func(tx db.Tx) (interface{}, error) {
		user := &model.User{}
		err := tx.Get(user, `SELECT * FROM users WHERE id = $1`, userId)
		if err != nil {
			return nil, err
		}
		return user, nil
	}, db.ReadOnly())
	if err != nil {
		return nil, err
	}
	return user.(*model.User), nil
}

func (s *DBAuthService) GetFirstUser() (*model.User, error) {
	user, err := s.db.Transact(func(tx db.Tx) (interface{}, error) {
		user := &model.User{}
		err := tx.Get(user, `SELECT * FROM users ORDER BY id LIMIT 1`)
		if err != nil {
			return nil, err
		}
		return user, nil
	}, db.ReadOnly())
	if err != nil {
		return nil, err
	}
	return user.(*model.User), nil
}

func (s *DBAuthService) ListUsers(params *model.PaginationParams) ([]*model.User, *model.Paginator, error) {
	type res struct {
		users     []*model.User
		paginator *model.Paginator
	}
	result, err := s.db.Transact(func(tx db.Tx) (interface{}, error) {
		users := make([]*model.User, 0)
		err := tx.Select(&users, `SELECT * FROM users ORDER BY display_name WHERE display_name > $1 LIMIT $2`,
			params.PageToken, params.Amount+1)
		if err != nil {
			return nil, err
		}
		p := &model.Paginator{}
		if len(users) == params.Amount+1 {
			// we have more pages
			p.Amount = params.Amount
			p.NextPageToken = users[len(users)-1].DisplayName
			return &res{users[0:params.Amount], p}, nil
		}
		p.Amount = len(users)
		return &res{users, p}, nil
	})

	if err != nil {
		return nil, nil, err
	}
	return result.(*res).users, result.(*res).paginator, nil
}

func (s *DBAuthService) ListUserCredentials(userDisplayName string, params *model.PaginationParams) ([]*model.Credential, *model.Paginator, error) {
	type res struct {
		credentials []*model.Credential
		paginator   *model.Paginator
	}
	result, err := s.db.Transact(func(tx db.Tx) (interface{}, error) {
		credentials := make([]*model.Credential, 0)
		err := tx.Select(&credentials, `
			SELECT * FROM credentials
				INNER JOIN users ON (credentials.user_id = users.id)
			ORDER BY credentials.access_key_id
			WHERE
				users.display_name = $1
				AND credentials.access_key_id > $2
			LIMIT $3`,
			userDisplayName, params.PageToken, params.Amount+1)
		if err != nil {
			return nil, err
		}
		p := &model.Paginator{}
		if len(credentials) == params.Amount+1 {
			// we have more pages
			p.Amount = params.Amount
			p.NextPageToken = credentials[len(credentials)-1].AccessKeyId
			return &res{credentials[0:params.Amount], p}, nil
		}
		p.Amount = len(credentials)
		return &res{credentials, p}, nil
	})

	if err != nil {
		return nil, nil, err
	}
	return result.(*res).credentials, result.(*res).paginator, nil
}

func (s *DBAuthService) ListUserRoles(userDisplayName string, params *model.PaginationParams) ([]*model.Role, *model.Paginator, error) {
	type res struct {
		roles     []*model.Role
		paginator *model.Paginator
	}
	result, err := s.db.Transact(func(tx db.Tx) (interface{}, error) {
		roles := make([]*model.Role, 0)
		err := tx.Select(&roles, `
			SELECT * FROM roles
				INNER JOIN user_roles ON (role.id = user_roles.role_id)
				INNER JOIN users ON (user_roles.user_id = users.id)
			ORDER BY roles.display_name
			WHERE
				users.display_name = $1
				AND roles.display_name > $2
			LIMIT $3`,
			userDisplayName, params.PageToken, params.Amount+1)
		if err != nil {
			return nil, err
		}
		p := &model.Paginator{}
		if len(roles) == params.Amount+1 {
			// we have more pages
			p.Amount = params.Amount
			p.NextPageToken = roles[len(roles)-1].DisplayName
			return &res{roles[0:params.Amount], p}, nil
		}
		p.Amount = len(roles)
		return &res{roles, p}, nil
	})

	if err != nil {
		return nil, nil, err
	}
	return result.(*res).roles, result.(*res).paginator, nil
}

func (s *DBAuthService) ListGroupRoles(groupDisplayName string, params *model.PaginationParams) ([]*model.Role, *model.Paginator, error) {
	type res struct {
		roles     []*model.Role
		paginator *model.Paginator
	}
	result, err := s.db.Transact(func(tx db.Tx) (interface{}, error) {
		roles := make([]*model.Role, 0)
		err := tx.Select(&roles, `
			SELECT * FROM roles
				INNER JOIN group_roles ON (role.id = group_roles.role_id)
				INNER JOIN groups ON (group_roles.group_id = groups.id)
			ORDER BY roles.display_name
			WHERE
				groups.display_name = $1
				AND roles.display_name > $2
			LIMIT $3`,
			groupDisplayName, params.PageToken, params.Amount+1)
		if err != nil {
			return nil, err
		}
		p := &model.Paginator{}
		if len(roles) == params.Amount+1 {
			// we have more pages
			p.Amount = params.Amount
			p.NextPageToken = roles[len(roles)-1].DisplayName
			return &res{roles[0:params.Amount], p}, nil
		}
		p.Amount = len(roles)
		return &res{roles, p}, nil
	})

	if err != nil {
		return nil, nil, err
	}
	return result.(*res).roles, result.(*res).paginator, nil
}

func (s *DBAuthService) ListRolePolicies(roleDisplayName string, params *model.PaginationParams) ([]*model.Policy, *model.Paginator, error) {
	type res struct {
		policies  []*model.Policy
		paginator *model.Paginator
	}
	result, err := s.db.Transact(func(tx db.Tx) (interface{}, error) {
		policies := make([]*model.PolicyDBImpl, 0)
		err := tx.Select(&policies, `
			SELECT * FROM policies
				INNER JOIN role_policies ON (role_policies.policy_id = policies.id)
				INNER JOIN roles ON (role_policies.role_id = role.id)
			ORDER BY display_name
			WHERE
				role.display_name = $1
				AND policies.display_name > $2
			LIMIT $3`,
			roleDisplayName, params.PageToken, params.Amount+1)
		if err != nil {
			return nil, err
		}
		p := &model.Paginator{}
		policyModels := make([]*model.Policy, len(policies))
		for i, policy := range policies {
			policyModels[i] = policy.ToModel()
		}
		if len(policies) == params.Amount+1 {
			// we have more pages
			p.Amount = params.Amount
			p.NextPageToken = policies[len(policies)-1].DisplayName
			return &res{policyModels[0:params.Amount], p}, nil
		}
		p.Amount = len(policies)
		return &res{policyModels, p}, nil
	})

	if err != nil {
		return nil, nil, err
	}
	return result.(*res).policies, result.(*res).paginator, nil
}

func (s *DBAuthService) CreateGroup(group *model.Group) error {
	_, err := s.db.Transact(func(tx db.Tx) (interface{}, error) {
		return nil, tx.Get(group, `INSERT INTO groups (display_name, created_at) VALUES ($1, $2) RETURNING id`,
			group.DisplayName, group.CreatedAt)
	})
	return err
}

func (s *DBAuthService) DeleteGroup(groupDisplayName string) error {
	_, err := s.db.Transact(func(tx db.Tx) (interface{}, error) {
		return nil, deleteOrNotFound(tx, `DELETE FROM groups WHERE display_name = $1`, groupDisplayName)
	})
	return err
}

func (s *DBAuthService) GetGroup(groupDisplayName string) (*model.Group, error) {
	group, err := s.db.Transact(func(tx db.Tx) (interface{}, error) {
		group := &model.Group{}
		err := tx.Get(group, `SELECT * FROM groups WHERE display_name = $1`, groupDisplayName)
		if err != nil {
			return nil, err
		}
		return group, nil
	}, db.ReadOnly())
	if err != nil {
		return nil, err
	}
	return group.(*model.Group), nil
}

func (s *DBAuthService) ListGroups(params *model.PaginationParams) ([]*model.Group, *model.Paginator, error) {
	type res struct {
		groups    []*model.Group
		paginator *model.Paginator
	}
	result, err := s.db.Transact(func(tx db.Tx) (interface{}, error) {
		groups := make([]*model.Group, 0)
		err := tx.Select(&groups, `SELECT * FROM groups ORDER BY display_name WHERE display_name > $1 LIMIT $2`,
			params.PageToken, params.Amount+1)
		if err != nil {
			return nil, err
		}
		p := &model.Paginator{}
		if len(groups) == params.Amount+1 {
			// we have more pages
			p.Amount = params.Amount
			p.NextPageToken = groups[len(groups)-1].DisplayName
			return &res{groups[0:params.Amount], p}, nil
		}
		p.Amount = len(groups)
		return &res{groups, p}, nil
	})

	if err != nil {
		return nil, nil, err
	}
	return result.(*res).groups, result.(*res).paginator, nil
}

func (s *DBAuthService) AddUserToGroup(userDisplayName, groupDisplayName string) error {
	_, err := s.db.Transact(func(tx db.Tx) (interface{}, error) {
		_, err := tx.Exec(`
			INSERT INTO user_groups (user_id, group_id)
			VALUES (
				(SELECT id FROM users WHERE display_name = $1),
				(SELECT id FROM groups WHERE display_name = $2)
			)`, userDisplayName, groupDisplayName)
		return nil, err
	})
	return err
}

func (s *DBAuthService) RemoveUserFromGroup(userDisplayName, groupDisplayName string) error {
	_, err := s.db.Transact(func(tx db.Tx) (interface{}, error) {
		return nil, deleteOrNotFound(tx, `
			DELETE FROM user_groups USING users, groups
			WHERE user_groups.user_id = users.id
				AND user_groups.group_id = groups.id
				AND users.display_name = $1
				AND groups.display_name = $2`,
			userDisplayName, groupDisplayName)
	})
	return err
}

func (s *DBAuthService) ListUserGroups(userDisplayName string, params *model.PaginationParams) ([]*model.Group, *model.Paginator, error) {
	type res struct {
		groups    []*model.Group
		paginator *model.Paginator
	}
	result, err := s.db.Transact(func(tx db.Tx) (interface{}, error) {
		groups := make([]*model.Group, 0)
		err := tx.Select(&groups, `
			SELECT * FROM groups
				INNER JOIN user_groups ON (groups.id = user_groups.group_id)
				INNER JOIN users ON (user_groups.user_id = users.id)
			ORDER BY groups.display_name
			WHERE
				users.display_name = $1
				AND groups.display_name > $2
			LIMIT $3`,
			userDisplayName, params.PageToken, params.Amount+1)
		if err != nil {
			return nil, err
		}
		p := &model.Paginator{}
		if len(groups) == params.Amount+1 {
			// we have more pages
			p.Amount = params.Amount
			p.NextPageToken = groups[len(groups)-1].DisplayName
			return &res{groups[0:params.Amount], p}, nil
		}
		p.Amount = len(groups)
		return &res{groups, p}, nil
	})

	if err != nil {
		return nil, nil, err
	}
	return result.(*res).groups, result.(*res).paginator, nil
}

func (s *DBAuthService) ListGroupUsers(groupDisplayName string, params *model.PaginationParams) ([]*model.User, *model.Paginator, error) {
	type res struct {
		users     []*model.User
		paginator *model.Paginator
	}
	result, err := s.db.Transact(func(tx db.Tx) (interface{}, error) {
		users := make([]*model.User, 0)
		err := tx.Select(&users, `
			SELECT * FROM users
				INNER JOIN user_groups ON (users.id = user_groups.user_id)
				INNER JOIN groups ON (user_groups.group_id = groups.id)
			ORDER BY groups.display_name
			WHERE
				groups.display_name = $1
				AND users.display_name > $2
			LIMIT $3`,
			groupDisplayName, params.PageToken, params.Amount+1)
		if err != nil {
			return nil, err
		}
		p := &model.Paginator{}
		if len(users) == params.Amount+1 {
			// we have more pages
			p.Amount = params.Amount
			p.NextPageToken = users[len(users)-1].DisplayName
			return &res{users[0:params.Amount], p}, nil
		}
		p.Amount = len(users)
		return &res{users, p}, nil
	})

	if err != nil {
		return nil, nil, err
	}
	return result.(*res).users, result.(*res).paginator, nil
}

func (s *DBAuthService) CreateRole(role *model.Role) error {
	_, err := s.db.Transact(func(tx db.Tx) (interface{}, error) {
		return nil, tx.Get(role, `INSERT INTO roles (display_name, created_at) VALUES ($1, $2) RETURNING id`,
			role.DisplayName, role.CreatedAt)
	})
	return err
}

func (s *DBAuthService) DeleteRole(roleDisplayName string) error {
	_, err := s.db.Transact(func(tx db.Tx) (interface{}, error) {
		return nil, deleteOrNotFound(tx, `DELETE FROM roles WHERE display_name = $1`, roleDisplayName)
	})
	return err
}

func (s *DBAuthService) GetRole(roleDisplayName string) (*model.Role, error) {
	role, err := s.db.Transact(func(tx db.Tx) (interface{}, error) {
		role := &model.Role{}
		err := tx.Get(role, `SELECT * FROM roles WHERE display_name = $1`, roleDisplayName)
		if err != nil {
			return nil, err
		}
		return role, nil
	}, db.ReadOnly())
	if err != nil {
		return nil, err
	}
	return role.(*model.Role), nil
}

func (s *DBAuthService) ListRoles(params *model.PaginationParams) ([]*model.Role, *model.Paginator, error) {
	type res struct {
		roles     []*model.Role
		paginator *model.Paginator
	}
	result, err := s.db.Transact(func(tx db.Tx) (interface{}, error) {
		roles := make([]*model.Role, 0)
		err := tx.Select(&roles, `SELECT * FROM roles ORDER BY display_name WHERE display_name > $1 LIMIT $2`,
			params.PageToken, params.Amount+1)
		if err != nil {
			return nil, err
		}
		p := &model.Paginator{}
		if len(roles) == params.Amount+1 {
			// we have more pages
			p.Amount = params.Amount
			p.NextPageToken = roles[len(roles)-1].DisplayName
			return &res{roles[0:params.Amount], p}, nil
		}
		p.Amount = len(roles)
		return &res{roles, p}, nil
	})

	if err != nil {
		return nil, nil, err
	}
	return result.(*res).roles, result.(*res).paginator, nil
}

func (s *DBAuthService) CreatePolicy(policy *model.Policy) error {
	_, err := s.db.Transact(func(tx db.Tx) (interface{}, error) {
		p := policy.ToDBImpl()
		return nil, tx.Get(policy, `INSERT INTO policies (display_name, created_at, action, resource, effect) VALUES ($1, $2, $3, $4, $5) RETURNING id`,
			p.DisplayName, p.CreatedAt, p.Action, p.Resource, p.Effect)
	})
	return err
}

func (s *DBAuthService) GetPolicy(policyDisplayName string) (*model.Policy, error) {
	policy, err := s.db.Transact(func(tx db.Tx) (interface{}, error) {
		policy := &model.PolicyDBImpl{}
		err := tx.Get(policy, `SELECT * FROM policies WHERE display_name = $1`, policyDisplayName)
		if err != nil {
			return nil, err
		}
		return policy.ToModel(), nil
	}, db.ReadOnly())
	if err != nil {
		return nil, err
	}
	return policy.(*model.Policy), nil
}

func (s *DBAuthService) DeletePolicy(policyDisplayName string) error {
	_, err := s.db.Transact(func(tx db.Tx) (interface{}, error) {
		return nil, deleteOrNotFound(tx, `DELETE FROM policies WHERE display_name = $1`, policyDisplayName)
	})
	return err
}

func (s *DBAuthService) ListPolicies(params *model.PaginationParams) ([]*model.Policy, *model.Paginator, error) {
	type res struct {
		policies  []*model.Policy
		paginator *model.Paginator
	}
	result, err := s.db.Transact(func(tx db.Tx) (interface{}, error) {
		policies := make([]*model.PolicyDBImpl, 0)
		err := tx.Select(&policies, `
			SELECT *
			FROM policies
			ORDER BY display_name
			WHERE display_name > $1
			LIMIT $2`,
			params.PageToken, params.Amount+1)
		if err != nil {
			return nil, err
		}
		p := &model.Paginator{}
		policyModels := make([]*model.Policy, len(policies))
		for i, policy := range policies {
			policyModels[i] = policy.ToModel()
		}
		if len(policies) == params.Amount+1 {
			// we have more pages
			p.Amount = params.Amount
			p.NextPageToken = policies[len(policies)-1].DisplayName
			return &res{policyModels[0:params.Amount], p}, nil
		}
		p.Amount = len(policies)
		return &res{policyModels, p}, nil
	})

	if err != nil {
		return nil, nil, err
	}
	return result.(*res).policies, result.(*res).paginator, nil
}

func (s *DBAuthService) CreateCredentials(userDisplayName string) (*model.Credential, error) {
	now := time.Now()
	accessKey := genAccessKeyId()
	secretKey := genAccessSecretKey()
	encryptedKey, err := s.encryptSecret(secretKey)
	if err != nil {
		return nil, err
	}
	credentials, err := s.db.Transact(func(tx db.Tx) (interface{}, error) {
		user, err := getUser(tx, userDisplayName)
		if err != nil {
			return nil, err
		}
		c := &model.Credential{
			AccessKeyId:                   accessKey,
			AccessSecretKey:               secretKey,
			AccessSecretKeyEncryptedBytes: encryptedKey,
			IssuedDate:                    now,
			UserId:                        user.Id,
		}
		_, err = tx.Exec(`
			INSERT INTO credentials (access_key_id, access_secret_key, issued_date, user_id)
			VALUES ($1, $2, $3, $4)`,
			c.AccessKeyId,
			encryptedKey,
			c.IssuedDate,
			c.UserId,
		)
		return c, err
	})
	if err != nil {
		return nil, err
	}
	return credentials.(*model.Credential), err
}

func (s *DBAuthService) DeleteCredentials(userDisplayName, accessKeyId string) error {
	_, err := s.db.Transact(func(tx db.Tx) (interface{}, error) {
		return nil, deleteOrNotFound(tx, `
			DELETE FROM credentials USING users
			WHERE credentials.user_id = users.id
				AND users.display_name = $1
				AND credentials.access_key_id = $2`,
			userDisplayName, accessKeyId)
	})
	return err
}

func (s *DBAuthService) AttachRoleToUser(roleDisplayName, userDisplayName string) error {
	_, err := s.db.Transact(func(tx db.Tx) (interface{}, error) {
		_, err := tx.Exec(`
			INSERT INTO user_roles (user_id, role_id)
			VALUES (
				(SELECT id FROM users WHERE display_name = $1),
				(SELECT id FROM roles WHERE display_name = $2)
			)`, userDisplayName, roleDisplayName)
		return nil, err
	})
	return err
}

func (s *DBAuthService) DetachRoleFromUser(roleDisplayName, userDisplayName string) error {
	_, err := s.db.Transact(func(tx db.Tx) (interface{}, error) {
		return nil, deleteOrNotFound(tx, `
			DELETE FROM user_roles USING users, roles
			WHERE user_roles.user_id = users.id
				AND user_roles.role_id = roles.id
				AND users.display_name = $1
				AND roles.display_name = $2`,
			userDisplayName, roleDisplayName)
	})
	return err
}

func (s *DBAuthService) AttachRoleToGroup(roleDisplayName, groupDisplayName string) error {
	_, err := s.db.Transact(func(tx db.Tx) (interface{}, error) {
		_, err := tx.Exec(`
			INSERT INTO group_roles (group_id, role_id)
			VALUES (
				(SELECT id FROM groups WHERE display_name = $1),
				(SELECT id FROM roles WHERE display_name = $2)
			)`, groupDisplayName, roleDisplayName)
		return nil, err
	})
	return err
}

func (s *DBAuthService) DetachRoleFromGroup(roleDisplayName, groupDisplayName string) error {
	_, err := s.db.Transact(func(tx db.Tx) (interface{}, error) {
		return nil, deleteOrNotFound(tx, `
			DELETE FROM group_roles USING groups, roles
			WHERE group_roles.group_id = groups.id
				AND user_roles.role_id = roles.id
				AND groups.display_name = $1
				AND roles.display_name = $2`,
			groupDisplayName, roleDisplayName)
	})
	return err
}

func (s *DBAuthService) AttachPolicyToRole(roleDisplayName string, policyDisplayName string) error {
	_, err := s.db.Transact(func(tx db.Tx) (interface{}, error) {
		_, err := tx.Exec(`
			INSERT INTO role_policies (policy_id, role_id)
			VALUES (
				(SELECT id FROM policies WHERE display_name = $1),
				(SELECT id FROM roles WHERE display_name = $2)
			)`, policyDisplayName, roleDisplayName)
		return nil, err
	})
	return err
}

func (s *DBAuthService) DetachPolicyFromRole(roleDisplayName string, policyDisplayName string) error {
	_, err := s.db.Transact(func(tx db.Tx) (interface{}, error) {
		return nil, deleteOrNotFound(tx, `
			DELETE FROM role_policies USING roles, policies
			WHERE role_policies.role_id = roles.id
				AND role_policies.policy_id = policies.id
				AND policies.display_name = $1
				AND roles.display_name = $2`,
			policyDisplayName, roleDisplayName)
	})
	return err
}

func (s *DBAuthService) GetCredentialsForUser(userDisplayName, accessKeyId string) (*model.Credential, error) {
	credentials, err := s.db.Transact(func(tx db.Tx) (interface{}, error) {
		credentials := &model.Credential{}
		err := tx.Get(credentials, `
			SELECT *
			FROM credentials
			INNER JOIN users ON (credentials.user_id = users.id)
			WHERE credentials.access_key_id = $1
				AND users.display_name = $2`, accessKeyId, userDisplayName)
		if err != nil {
			return nil, err
		}
		return credentials, nil
	})
	if err != nil {
		return nil, err
	}
	return credentials.(*model.Credential), nil
}

func (s *DBAuthService) GetCredentials(accessKeyId string) (*model.Credential, error) {
	credentials, err := s.db.Transact(func(tx db.Tx) (interface{}, error) {
		credentials := &model.Credential{}
		err := tx.Get(credentials, `
			SELECT * FROM credentials WHERE credentials.access_key_id = $1`, accessKeyId)
		if err != nil {
			return nil, err
		}
		key, err := s.decryptSecret(credentials.AccessSecretKeyEncryptedBytes)
		if err != nil {
			return nil, err
		}
		credentials.AccessSecretKey = key
		return credentials, nil
	})
	if err != nil {
		return nil, err
	}
	return credentials.(*model.Credential), nil
}

func (s *DBAuthService) Authorize(req *AuthorizationRequest) (*AuthorizationResponse, error) {
	resp, err := s.db.Transact(func(tx db.Tx) (interface{}, error) {
		// resolve all policies attached to roles attached to the user
		var err error
		var policies []*model.PolicyDBImpl

		// resolve all policies
		// language=sql
		var resolvePoliciesStmt = `
		SELECT policies.id, policies.action, policies.resource, policies.effect
		FROM policies
			INNER JOIN role_policies ON (policies.id = role_policies.policy_id)
			INNER JOIN roles ON (roles.id = role_policies.role_id)
			INNER JOIN user_roles ON (roles.id = user_roles.role_id)
			INNER JOIN users ON (users.id = user_roles.user_id)
		WHERE users.display_name = $1
		UNION
		SELECT policies.id, policies.action, policies.resource, policies.effect
		FROM policies
			INNER JOIN role_policies ON (policies.id = role_policies.policy_id)
			INNER JOIN roles ON (roles.id = role_policies.role_id)
			INNER JOIN group_roles ON (roles.id = group_roles.role_id)
			INNER JOIN groups ON (groups.id = group_roles.group_id)
			INNER JOIN user_groups ON (user_groups.group_id = groups.id) 
			INNER JOIN users ON (users.id = user_groups.user_id)
		WHERE users.display_name = $1
		`
		err = tx.Select(&policies, resolvePoliciesStmt, req.UserDisplayName)
		if err != nil {
			return nil, err
		}
		allowed := false
		for _, p := range policies {
			policy := p.ToModel()
			if !ArnMatch(policy.Resource, req.SubjectARN) {
				continue
			}
			for _, action := range policy.Action {
				if action == string(req.Permission) && !policy.Effect {
					// this is a "Deny" and it takes precedence
					return &AuthorizationResponse{
						Allowed: false,
						Error:   ErrInsufficientPermissions,
					}, nil
				} else if action == string(req.Permission) {
					allowed = true
				}
			}
		}

		if !allowed {
			return &AuthorizationResponse{
				Allowed: false,
				Error:   ErrInsufficientPermissions,
			}, nil
		}

		// we're allowed!
		return &AuthorizationResponse{Allowed: true}, nil
	}, db.ReadOnly())
	if err != nil {
		return nil, err
	}
	return resp.(*AuthorizationResponse), nil
}
