package auth

import (
	"fmt"
	"strings"
	"time"

	"github.com/jmoiron/sqlx"

	"github.com/treeverse/lakefs/auth/crypt"
	"github.com/treeverse/lakefs/auth/model"
	"github.com/treeverse/lakefs/auth/params"
	"github.com/treeverse/lakefs/auth/wildcard"
	"github.com/treeverse/lakefs/db"
	"github.com/treeverse/lakefs/logging"
	"github.com/treeverse/lakefs/permissions"
)

type AuthorizationRequest struct {
	UserDisplayName     string
	RequiredPermissions []permissions.Permission
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
	GetUserByID(userID int) (*model.User, error)
	GetUser(userDisplayName string) (*model.User, error)
	ListUsers(params *model.PaginationParams) ([]*model.User, *model.Paginator, error)

	// groups
	CreateGroup(group *model.Group) error
	DeleteGroup(groupDisplayName string) error
	GetGroup(groupDisplayName string) (*model.Group, error)
	ListGroups(params *model.PaginationParams) ([]*model.Group, *model.Paginator, error)

	// group<->user memberships
	AddUserToGroup(userDisplayName, groupDisplayName string) error
	RemoveUserFromGroup(userDisplayName, groupDisplayName string) error
	ListUserGroups(userDisplayName string, params *model.PaginationParams) ([]*model.Group, *model.Paginator, error)
	ListGroupUsers(groupDisplayName string, params *model.PaginationParams) ([]*model.User, *model.Paginator, error)

	// policies
	WritePolicy(policy *model.Policy) error
	GetPolicy(policyDisplayName string) (*model.Policy, error)
	DeletePolicy(policyDisplayName string) error
	ListPolicies(params *model.PaginationParams) ([]*model.Policy, *model.Paginator, error)

	// credentials
	CreateCredentials(userDisplayName string) (*model.Credential, error)
	DeleteCredentials(userDisplayName, accessKeyID string) error
	GetCredentialsForUser(userDisplayName, accessKeyID string) (*model.Credential, error)
	GetCredentials(accessKeyID string) (*model.Credential, error)
	ListUserCredentials(userDisplayName string, params *model.PaginationParams) ([]*model.Credential, *model.Paginator, error)

	// policy<->user attachments
	AttachPolicyToUser(policyDisplayName, userDisplayName string) error
	DetachPolicyFromUser(policyDisplayName, userDisplayName string) error
	ListUserPolicies(userDisplayName string, params *model.PaginationParams) ([]*model.Policy, *model.Paginator, error)
	ListEffectivePolicies(userDisplayName string, params *model.PaginationParams) ([]*model.Policy, *model.Paginator, error)

	// policy<->group attachments
	AttachPolicyToGroup(policyDisplayName, groupDisplayName string) error
	DetachPolicyFromGroup(policyDisplayName, groupDisplayName string) error
	ListGroupPolicies(groupDisplayName string, params *model.PaginationParams) ([]*model.Policy, *model.Paginator, error)

	// authorize user for an action
	Authorize(req *AuthorizationRequest) (*AuthorizationResponse, error)
}

func getUser(tx db.Tx, userDisplayName string) (*model.User, error) {
	user := &model.User{}
	err := tx.Get(user, `SELECT * FROM auth_users WHERE display_name = $1`, userDisplayName)
	if err != nil {
		return nil, err
	}
	return user, nil
}

func getGroup(tx db.Tx, groupDisplayName string) (*model.Group, error) {
	group := &model.Group{}
	err := tx.Get(group, `SELECT * FROM auth_groups WHERE display_name = $1`, groupDisplayName)
	if err != nil {
		return nil, err
	}
	return group, nil
}

func getPolicy(tx db.Tx, policyDisplayName string) (*model.Policy, error) {
	policy := &model.Policy{}
	err := tx.Get(policy, `SELECT * FROM auth_policies WHERE display_name = $1`, policyDisplayName)
	if err != nil {
		return nil, err
	}
	return policy, nil
}

func deleteOrNotFound(tx sqlx.Execer, stmt string, args ...interface{}) error {
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

func genAccessKeyID() string {
	const accessKeyLength = 14
	key := KeyGenerator(accessKeyLength)
	return fmt.Sprintf("%s%s%s", "AKIAJ", key, "Q")
}

func genAccessSecretKey() string {
	const secretKeyLength = 30
	return Base64StringGenerator(secretKeyLength)
}

type DBAuthService struct {
	db          db.Database
	secretStore crypt.SecretStore
	cache       Cache
}

func NewDBAuthService(db db.Database, secretStore crypt.SecretStore, cacheConf params.ServiceCache) *DBAuthService {
	logging.Default().Info("initialized Auth service")
	var cache Cache
	if cacheConf.Enabled {
		cache = NewLRUCache(cacheConf.Size, cacheConf.TTL, cacheConf.EvictionJitter)
	} else {
		cache = &DummyCache{}
	}
	return &DBAuthService{
		db:          db,
		secretStore: secretStore,
		cache:       cache,
	}
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

func (s *DBAuthService) DB() db.Database {
	return s.db
}

func (s *DBAuthService) CreateUser(user *model.User) error {
	_, err := s.db.Transact(func(tx db.Tx) (interface{}, error) {
		if err := model.ValidateAuthEntityID(user.DisplayName); err != nil {
			return nil, err
		}
		err := tx.Get(user, `INSERT INTO auth_users (display_name, created_at) VALUES ($1, $2) RETURNING id`, user.DisplayName, user.CreatedAt)
		return nil, err
	})
	return err
}

func (s *DBAuthService) DeleteUser(userDisplayName string) error {
	_, err := s.db.Transact(func(tx db.Tx) (interface{}, error) {
		return nil, deleteOrNotFound(tx, `DELETE FROM auth_users WHERE display_name = $1`, userDisplayName)
	})
	return err
}

func (s *DBAuthService) GetUser(userDisplayName string) (*model.User, error) {
	return s.cache.GetUser(userDisplayName, func() (*model.User, error) {
		user, err := s.db.Transact(func(tx db.Tx) (interface{}, error) {
			return getUser(tx, userDisplayName)
		}, db.ReadOnly())
		if err != nil {
			return nil, err
		}
		return user.(*model.User), nil
	})
}

func (s *DBAuthService) GetUserByID(userID int) (*model.User, error) {
	return s.cache.GetUserByID(userID, func() (*model.User, error) {
		user, err := s.db.Transact(func(tx db.Tx) (interface{}, error) {
			user := &model.User{}
			err := tx.Get(user, `SELECT * FROM auth_users WHERE id = $1`, userID)
			if err != nil {
				return nil, err
			}
			return user, nil
		}, db.ReadOnly())
		if err != nil {
			return nil, err
		}
		return user.(*model.User), nil
	})
}

func (s *DBAuthService) ListUsers(params *model.PaginationParams) ([]*model.User, *model.Paginator, error) {
	type res struct {
		users     []*model.User
		paginator *model.Paginator
	}
	result, err := s.db.Transact(func(tx db.Tx) (interface{}, error) {
		users := make([]*model.User, 0)
		err := tx.Select(&users, `SELECT * FROM auth_users WHERE display_name > $1 ORDER BY display_name LIMIT $2`,
			params.After, params.Amount+1)
		if err != nil {
			return nil, err
		}
		p := &model.Paginator{}
		if len(users) == params.Amount+1 {
			// we have more pages
			users = users[0:params.Amount]
			p.Amount = params.Amount
			p.NextPageToken = users[len(users)-1].DisplayName
			return &res{users, p}, nil
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
		if _, err := getUser(tx, userDisplayName); err != nil {
			return nil, err
		}
		credentials := make([]*model.Credential, 0)
		err := tx.Select(&credentials, `
			SELECT auth_credentials.* FROM auth_credentials
				INNER JOIN auth_users ON (auth_credentials.user_id = auth_users.id)
			WHERE
				auth_users.display_name = $1
				AND auth_credentials.access_key_id > $2
			ORDER BY auth_credentials.access_key_id
			LIMIT $3`,
			userDisplayName, params.After, params.Amount+1)
		if err != nil {
			return nil, err
		}
		p := &model.Paginator{}
		if len(credentials) == params.Amount+1 {
			// we have more pages
			credentials = credentials[0:params.Amount]
			p.Amount = params.Amount
			p.NextPageToken = credentials[len(credentials)-1].AccessKeyID
			return &res{credentials, p}, nil
		}
		p.Amount = len(credentials)
		return &res{credentials, p}, nil
	})

	if err != nil {
		return nil, nil, err
	}
	return result.(*res).credentials, result.(*res).paginator, nil
}

func (s *DBAuthService) AttachPolicyToUser(policyDisplayName, userDisplayName string) error {
	_, err := s.db.Transact(func(tx db.Tx) (interface{}, error) {
		if _, err := getUser(tx, userDisplayName); err != nil {
			return nil, fmt.Errorf("%s: %w", userDisplayName, err)
		}
		if _, err := getPolicy(tx, policyDisplayName); err != nil {
			return nil, fmt.Errorf("%s: %w", policyDisplayName, err)
		}
		_, err := tx.Exec(`
			INSERT INTO auth_user_policies (user_id, policy_id)
			VALUES (
				(SELECT id FROM auth_users WHERE display_name = $1),
				(SELECT id FROM auth_policies WHERE display_name = $2)
			)`, userDisplayName, policyDisplayName)
		if db.IsUniqueViolation(err) {
			return nil, fmt.Errorf("policy attachment: %w", db.ErrAlreadyExists)
		}
		return nil, err
	})
	return err
}

func (s *DBAuthService) DetachPolicyFromUser(policyDisplayName, userDisplayName string) error {
	_, err := s.db.Transact(func(tx db.Tx) (interface{}, error) {
		if _, err := getUser(tx, userDisplayName); err != nil {
			return nil, fmt.Errorf("%s: %w", userDisplayName, err)
		}
		if _, err := getPolicy(tx, policyDisplayName); err != nil {
			return nil, fmt.Errorf("%s: %w", policyDisplayName, err)
		}
		return nil, deleteOrNotFound(tx, `
			DELETE FROM auth_user_policies USING auth_users, auth_policies
			WHERE auth_user_policies.user_id = auth_users.id
				AND auth_user_policies.policy_id = auth_policies.id
				AND auth_users.display_name = $1
				AND auth_policies.display_name = $2`,
			userDisplayName, policyDisplayName)
	})
	return err
}

func (s *DBAuthService) ListUserPolicies(userDisplayName string, params *model.PaginationParams) ([]*model.Policy, *model.Paginator, error) {
	type res struct {
		policies  []*model.Policy
		paginator *model.Paginator
	}
	result, err := s.db.Transact(func(tx db.Tx) (interface{}, error) {
		if _, err := getUser(tx, userDisplayName); err != nil {
			return nil, err
		}
		policies := make([]*model.Policy, 0)
		err := tx.Select(&policies, `
			SELECT auth_policies.* FROM auth_policies
				INNER JOIN auth_user_policies ON (auth_policies.id = auth_user_policies.policy_id)
				INNER JOIN auth_users ON (auth_user_policies.user_id = auth_users.id)
			WHERE
				auth_users.display_name = $1
				AND auth_policies.display_name > $2
			ORDER BY auth_policies.display_name
			LIMIT $3`,
			userDisplayName, params.After, params.Amount+1)
		if err != nil {
			return nil, err
		}
		p := &model.Paginator{}

		if len(policies) == params.Amount+1 {
			// we have more pages
			policies = policies[0:params.Amount]
			p.Amount = params.Amount
			p.NextPageToken = policies[len(policies)-1].DisplayName
			return &res{policies, p}, nil
		}
		p.Amount = len(policies)
		return &res{policies, p}, nil
	})

	if err != nil {
		return nil, nil, err
	}
	return result.(*res).policies, result.(*res).paginator, nil
}

func (s *DBAuthService) getEffectivePolicies(userDisplayName string, params *model.PaginationParams) ([]*model.Policy, *model.Paginator, error) {
	type res struct {
		policies  []*model.Policy
		paginator *model.Paginator
	}
	result, err := s.db.Transact(func(tx db.Tx) (interface{}, error) {
		// resolve all policies attached to the user and its groups
		var err error
		var policies []*model.Policy

		// resolve all policies
		// language=sql
		const resolvePoliciesStmt = `
		SELECT p.id, p.created_at, p.display_name, p.statement
		FROM (
			SELECT auth_policies.id, auth_policies.created_at, auth_policies.display_name, auth_policies.statement
			FROM auth_policies
				INNER JOIN auth_user_policies ON (auth_policies.id = auth_user_policies.policy_id)
				INNER JOIN auth_users ON (auth_users.id = auth_user_policies.user_id)
			WHERE auth_users.display_name = $1
			UNION
			SELECT auth_policies.id, auth_policies.created_at, auth_policies.display_name, auth_policies.statement
			FROM auth_policies
				INNER JOIN auth_group_policies ON (auth_policies.id = auth_group_policies.policy_id)
				INNER JOIN auth_groups ON (auth_groups.id = auth_group_policies.group_id)
				INNER JOIN auth_user_groups ON (auth_user_groups.group_id = auth_groups.id) 
				INNER JOIN auth_users ON (auth_users.id = auth_user_groups.user_id)
			WHERE
				auth_users.display_name = $1
		) p `

		const resolvePoliciesPaginated = resolvePoliciesStmt + `WHERE p.display_name > $2 ORDER BY p.display_name LIMIT $3`
		const resolvePoliciesAll = resolvePoliciesStmt + `ORDER BY p.display_name`

		if params.Amount != -1 {
			err = tx.Select(&policies, resolvePoliciesPaginated, userDisplayName, params.After, params.Amount+1)
		} else {
			err = tx.Select(&policies, resolvePoliciesAll, userDisplayName)
		}
		if err != nil {
			return nil, err
		}

		p := &model.Paginator{}

		if params.Amount != -1 && len(policies) == params.Amount+1 {
			// we have more pages
			policies = policies[0:params.Amount]
			p.Amount = params.Amount
			p.NextPageToken = policies[len(policies)-1].DisplayName
			return &res{policies, p}, nil
		}
		p.Amount = len(policies)
		return &res{policies, p}, nil
	}, db.ReadOnly())
	if err != nil {
		return nil, nil, err
	}
	return result.(*res).policies, result.(*res).paginator, nil
}

func (s *DBAuthService) ListEffectivePolicies(userDisplayName string, params *model.PaginationParams) ([]*model.Policy, *model.Paginator, error) {
	if params.Amount == -1 {
		// read through the cache when requesting the full list
		policies, err := s.cache.GetUserPolicies(userDisplayName, func() ([]*model.Policy, error) {
			policies, _, err := s.getEffectivePolicies(userDisplayName, params)
			return policies, err
		})
		if err != nil {
			return nil, nil, err
		}
		return policies, &model.Paginator{Amount: len(policies)}, nil
	}

	return s.getEffectivePolicies(userDisplayName, params)
}

func (s *DBAuthService) ListGroupPolicies(groupDisplayName string, params *model.PaginationParams) ([]*model.Policy, *model.Paginator, error) {
	type res struct {
		policies  []*model.Policy
		paginator *model.Paginator
	}
	result, err := s.db.Transact(func(tx db.Tx) (interface{}, error) {
		if _, err := getGroup(tx, groupDisplayName); err != nil {
			return nil, err
		}
		policies := make([]*model.Policy, 0)
		err := tx.Select(&policies, `
			SELECT auth_policies.* FROM auth_policies
				INNER JOIN auth_group_policies ON (auth_policies.id = auth_group_policies.policy_id)
				INNER JOIN auth_groups ON (auth_group_policies.group_id = auth_groups.id)
			WHERE
				auth_groups.display_name = $1
				AND auth_policies.display_name > $2
			ORDER BY auth_policies.display_name
			LIMIT $3`,
			groupDisplayName, params.After, params.Amount+1)
		if err != nil {
			return nil, err
		}
		p := &model.Paginator{}

		if len(policies) == params.Amount+1 {
			// we have more pages
			policies = policies[0:params.Amount]
			p.Amount = params.Amount
			p.NextPageToken = policies[len(policies)-1].DisplayName
			return &res{policies, p}, nil
		}
		p.Amount = len(policies)
		return &res{policies, p}, nil
	})

	if err != nil {
		return nil, nil, err
	}
	return result.(*res).policies, result.(*res).paginator, nil
}

func (s *DBAuthService) CreateGroup(group *model.Group) error {
	_, err := s.db.Transact(func(tx db.Tx) (interface{}, error) {
		if err := model.ValidateAuthEntityID(group.DisplayName); err != nil {
			return nil, err
		}
		return nil, tx.Get(group, `INSERT INTO auth_groups (display_name, created_at) VALUES ($1, $2) RETURNING id`,
			group.DisplayName, group.CreatedAt)
	})
	return err
}

func (s *DBAuthService) DeleteGroup(groupDisplayName string) error {
	_, err := s.db.Transact(func(tx db.Tx) (interface{}, error) {
		return nil, deleteOrNotFound(tx, `DELETE FROM auth_groups WHERE display_name = $1`, groupDisplayName)
	})
	return err
}

func (s *DBAuthService) GetGroup(groupDisplayName string) (*model.Group, error) {
	group, err := s.db.Transact(func(tx db.Tx) (interface{}, error) {
		return getGroup(tx, groupDisplayName)
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
		err := tx.Select(&groups, `SELECT * FROM auth_groups WHERE display_name > $1 ORDER BY display_name LIMIT $2`,
			params.After, params.Amount+1)
		if err != nil {
			return nil, err
		}
		p := &model.Paginator{}
		if len(groups) == params.Amount+1 {
			// we have more pages
			groups = groups[0:params.Amount]
			p.Amount = params.Amount
			p.NextPageToken = groups[len(groups)-1].DisplayName
			return &res{groups, p}, nil
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
		if _, err := getUser(tx, userDisplayName); err != nil {
			return nil, fmt.Errorf("%s: %w", userDisplayName, err)
		}
		if _, err := getGroup(tx, groupDisplayName); err != nil {
			return nil, fmt.Errorf("%s: %w", groupDisplayName, err)
		}
		_, err := tx.Exec(`
			INSERT INTO auth_user_groups (user_id, group_id)
			VALUES (
				(SELECT id FROM auth_users WHERE display_name = $1),
				(SELECT id FROM auth_groups WHERE display_name = $2)
			)`, userDisplayName, groupDisplayName)
		if db.IsUniqueViolation(err) {
			return nil, fmt.Errorf("group membership: %w", db.ErrAlreadyExists)
		}
		return nil, err
	})
	return err
}

func (s *DBAuthService) RemoveUserFromGroup(userDisplayName, groupDisplayName string) error {
	_, err := s.db.Transact(func(tx db.Tx) (interface{}, error) {
		if _, err := getUser(tx, userDisplayName); err != nil {
			return nil, fmt.Errorf("%s: %w", userDisplayName, err)
		}
		if _, err := getGroup(tx, groupDisplayName); err != nil {
			return nil, fmt.Errorf("%s: %w", groupDisplayName, err)
		}
		return nil, deleteOrNotFound(tx, `
			DELETE FROM auth_user_groups USING auth_users, auth_groups
			WHERE auth_user_groups.user_id = auth_users.id
				AND auth_user_groups.group_id = auth_groups.id
				AND auth_users.display_name = $1
				AND auth_groups.display_name = $2`,
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
		if _, err := getUser(tx, userDisplayName); err != nil {
			return nil, err
		}
		groups := make([]*model.Group, 0)
		err := tx.Select(&groups, `
			SELECT auth_groups.* FROM auth_groups
				INNER JOIN auth_user_groups ON (auth_groups.id = auth_user_groups.group_id)
				INNER JOIN auth_users ON (auth_user_groups.user_id = auth_users.id)
			WHERE
				auth_users.display_name = $1
				AND auth_groups.display_name > $2
			ORDER BY auth_groups.display_name
			LIMIT $3`,
			userDisplayName, params.After, params.Amount+1)
		if err != nil {
			return nil, err
		}
		p := &model.Paginator{}
		if len(groups) == params.Amount+1 {
			// we have more pages
			groups = groups[0:params.Amount]
			p.Amount = params.Amount
			p.NextPageToken = groups[len(groups)-1].DisplayName
			return &res{groups, p}, nil
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
		if _, err := getGroup(tx, groupDisplayName); err != nil {
			return nil, err
		}
		users := make([]*model.User, 0)
		err := tx.Select(&users, `
			SELECT auth_users.* FROM auth_users
				INNER JOIN auth_user_groups ON (auth_users.id = auth_user_groups.user_id)
				INNER JOIN auth_groups ON (auth_user_groups.group_id = auth_groups.id)
			WHERE
				auth_groups.display_name = $1
				AND auth_users.display_name > $2
			ORDER BY auth_groups.display_name
			LIMIT $3`,
			groupDisplayName, params.After, params.Amount+1)
		if err != nil {
			return nil, err
		}
		p := &model.Paginator{}
		if len(users) == params.Amount+1 {
			// we have more pages
			users = users[0:params.Amount]
			p.Amount = params.Amount
			p.NextPageToken = users[len(users)-1].DisplayName
			return &res{users, p}, nil
		}
		p.Amount = len(users)
		return &res{users, p}, nil
	})

	if err != nil {
		return nil, nil, err
	}
	return result.(*res).users, result.(*res).paginator, nil
}

func (s *DBAuthService) WritePolicy(policy *model.Policy) error {
	_, err := s.db.Transact(func(tx db.Tx) (interface{}, error) {
		if err := model.ValidateAuthEntityID(policy.DisplayName); err != nil {
			return nil, err
		}
		for _, stmt := range policy.Statement {
			for _, action := range stmt.Action {
				if err := model.ValidateActionName(action); err != nil {
					return nil, err
				}
			}
			if err := model.ValidateArn(stmt.Resource); err != nil {
				return nil, err
			}
			if err := model.ValidateStatementEffect(stmt.Effect); err != nil {
				return nil, err
			}
		}

		return nil, tx.Get(policy, `
			INSERT INTO auth_policies (display_name, created_at, statement)
			VALUES ($1, $2, $3)
			ON CONFLICT (display_name) DO UPDATE SET statement = $3
			RETURNING id`,
			policy.DisplayName, policy.CreatedAt, policy.Statement)
	})
	return err
}

func (s *DBAuthService) GetPolicy(policyDisplayName string) (*model.Policy, error) {
	policy, err := s.db.Transact(func(tx db.Tx) (interface{}, error) {
		return getPolicy(tx, policyDisplayName)
	}, db.ReadOnly())
	if err != nil {
		return nil, err
	}
	return policy.(*model.Policy), nil
}

func (s *DBAuthService) DeletePolicy(policyDisplayName string) error {
	_, err := s.db.Transact(func(tx db.Tx) (interface{}, error) {
		return nil, deleteOrNotFound(tx, `DELETE FROM auth_policies WHERE display_name = $1`, policyDisplayName)
	})
	return err
}

func (s *DBAuthService) ListPolicies(params *model.PaginationParams) ([]*model.Policy, *model.Paginator, error) {
	type res struct {
		policies  []*model.Policy
		paginator *model.Paginator
	}
	result, err := s.db.Transact(func(tx db.Tx) (interface{}, error) {
		policies := make([]*model.Policy, 0)
		err := tx.Select(&policies, `
			SELECT *
			FROM auth_policies
			WHERE display_name > $1
			ORDER BY display_name
			LIMIT $2`,
			params.After, params.Amount+1)
		if err != nil {
			return nil, err
		}
		p := &model.Paginator{}

		if len(policies) == params.Amount+1 {
			// we have more pages
			policies = policies[0:params.Amount]
			p.Amount = params.Amount
			p.NextPageToken = policies[len(policies)-1].DisplayName
			return &res{policies, p}, nil
		}
		p.Amount = len(policies)
		return &res{policies, p}, nil
	})

	if err != nil {
		return nil, nil, err
	}
	return result.(*res).policies, result.(*res).paginator, nil
}

func (s *DBAuthService) CreateCredentials(userDisplayName string) (*model.Credential, error) {
	now := time.Now()
	accessKey := genAccessKeyID()
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
			AccessKeyID:                   accessKey,
			AccessSecretKey:               secretKey,
			AccessSecretKeyEncryptedBytes: encryptedKey,
			IssuedDate:                    now,
			UserID:                        user.ID,
		}
		_, err = tx.Exec(`
			INSERT INTO auth_credentials (access_key_id, access_secret_key, issued_date, user_id)
			VALUES ($1, $2, $3, $4)`,
			c.AccessKeyID,
			encryptedKey,
			c.IssuedDate,
			c.UserID,
		)
		return c, err
	})
	if err != nil {
		return nil, err
	}
	return credentials.(*model.Credential), err
}

func (s *DBAuthService) DeleteCredentials(userDisplayName, accessKeyID string) error {
	_, err := s.db.Transact(func(tx db.Tx) (interface{}, error) {
		return nil, deleteOrNotFound(tx, `
			DELETE FROM auth_credentials USING auth_users
			WHERE auth_credentials.user_id = auth_users.id
				AND auth_users.display_name = $1
				AND auth_credentials.access_key_id = $2`,
			userDisplayName, accessKeyID)
	})
	return err
}

func (s *DBAuthService) AttachPolicyToGroup(policyDisplayName, groupDisplayName string) error {
	_, err := s.db.Transact(func(tx db.Tx) (interface{}, error) {
		if _, err := getGroup(tx, groupDisplayName); err != nil {
			return nil, fmt.Errorf("%s: %w", groupDisplayName, err)
		}
		if _, err := getPolicy(tx, policyDisplayName); err != nil {
			return nil, fmt.Errorf("%s: %w", policyDisplayName, err)
		}
		_, err := tx.Exec(`
			INSERT INTO auth_group_policies (group_id, policy_id)
			VALUES (
				(SELECT id FROM auth_groups WHERE display_name = $1),
				(SELECT id FROM auth_policies WHERE display_name = $2)
			)`, groupDisplayName, policyDisplayName)
		if db.IsUniqueViolation(err) {
			return nil, fmt.Errorf("policy attachment: %w", db.ErrAlreadyExists)
		}
		return nil, err
	})
	return err
}

func (s *DBAuthService) DetachPolicyFromGroup(policyDisplayName, groupDisplayName string) error {
	_, err := s.db.Transact(func(tx db.Tx) (interface{}, error) {
		if _, err := getGroup(tx, groupDisplayName); err != nil {
			return nil, fmt.Errorf("%s: %w", groupDisplayName, err)
		}
		if _, err := getPolicy(tx, policyDisplayName); err != nil {
			return nil, fmt.Errorf("%s: %w", policyDisplayName, err)
		}
		return nil, deleteOrNotFound(tx, `
			DELETE FROM auth_group_policies USING auth_groups, auth_policies
			WHERE auth_group_policies.group_id = auth_groups.id
				AND auth_group_policies.policy_id = auth_policies.id
				AND auth_groups.display_name = $1
				AND auth_policies.display_name = $2`,
			groupDisplayName, policyDisplayName)
	})
	return err
}

func (s *DBAuthService) GetCredentialsForUser(userDisplayName, accessKeyID string) (*model.Credential, error) {
	credentials, err := s.db.Transact(func(tx db.Tx) (interface{}, error) {
		if _, err := getUser(tx, userDisplayName); err != nil {
			return nil, err
		}
		credentials := &model.Credential{}
		err := tx.Get(credentials, `
			SELECT auth_credentials.*
			FROM auth_credentials
			INNER JOIN auth_users ON (auth_credentials.user_id = auth_users.id)
			WHERE auth_credentials.access_key_id = $1
				AND auth_users.display_name = $2`, accessKeyID, userDisplayName)
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

func (s *DBAuthService) GetCredentials(accessKeyID string) (*model.Credential, error) {
	return s.cache.GetCredential(accessKeyID, func() (*model.Credential, error) {
		credentials, err := s.db.Transact(func(tx db.Tx) (interface{}, error) {
			credentials := &model.Credential{}
			err := tx.Get(credentials, `
			SELECT * FROM auth_credentials WHERE auth_credentials.access_key_id = $1`, accessKeyID)
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
	})
}

func interpolateUser(resource string, userDisplayName string) string {
	return strings.ReplaceAll(resource, "${user}", userDisplayName)
}

func (s *DBAuthService) Authorize(req *AuthorizationRequest) (*AuthorizationResponse, error) {
	policies, _, err := s.ListEffectivePolicies(req.UserDisplayName, &model.PaginationParams{
		After:  "", // all
		Amount: -1, // all
	})

	if err != nil {
		return nil, err
	}
	allowed := false
	for _, perm := range req.RequiredPermissions {
		for _, policy := range policies {
			for _, stmt := range policy.Statement {
				resource := interpolateUser(stmt.Resource, req.UserDisplayName)
				if !ArnMatch(resource, perm.Resource) {
					continue
				}
				for _, action := range stmt.Action {
					if !wildcard.Match(action, perm.Action) {
						continue // not a matching action
					}

					if stmt.Effect == model.StatementEffectDeny {
						// this is a "Deny" and it takes precedence
						return &AuthorizationResponse{
							Allowed: false,
							Error:   ErrInsufficientPermissions,
						}, nil
					}

					allowed = true
				}
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
}
