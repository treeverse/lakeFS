package auth

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"time"

	sq "github.com/Masterminds/squirrel"
	"github.com/georgysavva/scany/pgxscan"

	"github.com/treeverse/lakefs/pkg/auth/crypt"
	"github.com/treeverse/lakefs/pkg/auth/model"
	"github.com/treeverse/lakefs/pkg/auth/params"
	"github.com/treeverse/lakefs/pkg/auth/wildcard"
	"github.com/treeverse/lakefs/pkg/db"
	"github.com/treeverse/lakefs/pkg/logging"
	"github.com/treeverse/lakefs/pkg/permissions"
)

type AuthorizationRequest struct {
	Username            string
	RequiredPermissions []permissions.Permission
}

type AuthorizationResponse struct {
	Allowed bool
	Error   error
}

type Service interface {
	SecretStore() crypt.SecretStore

	// users
	CreateUser(ctx context.Context, user *model.User) error
	DeleteUser(ctx context.Context, username string) error
	GetUserByID(ctx context.Context, userID int) (*model.User, error)
	GetUser(ctx context.Context, username string) (*model.User, error)
	ListUsers(ctx context.Context, params *model.PaginationParams) ([]*model.User, *model.Paginator, error)

	// groups
	CreateGroup(ctx context.Context, group *model.Group) error
	DeleteGroup(ctx context.Context, groupDisplayName string) error
	GetGroup(ctx context.Context, groupDisplayName string) (*model.Group, error)
	ListGroups(ctx context.Context, params *model.PaginationParams) ([]*model.Group, *model.Paginator, error)

	// group<->user memberships
	AddUserToGroup(ctx context.Context, username, groupDisplayName string) error
	RemoveUserFromGroup(ctx context.Context, username, groupDisplayName string) error
	ListUserGroups(ctx context.Context, username string, params *model.PaginationParams) ([]*model.Group, *model.Paginator, error)
	ListGroupUsers(ctx context.Context, groupDisplayName string, params *model.PaginationParams) ([]*model.User, *model.Paginator, error)

	// policies
	WritePolicy(ctx context.Context, policy *model.Policy) error
	GetPolicy(ctx context.Context, policyDisplayName string) (*model.Policy, error)
	DeletePolicy(ctx context.Context, policyDisplayName string) error
	ListPolicies(ctx context.Context, params *model.PaginationParams) ([]*model.Policy, *model.Paginator, error)

	// credentials
	CreateCredentials(ctx context.Context, username string) (*model.Credential, error)
	AddCredentials(ctx context.Context, username, accessKeyID, secretAccessKey string) (*model.Credential, error)
	DeleteCredentials(ctx context.Context, username, accessKeyID string) error
	GetCredentialsForUser(ctx context.Context, username, accessKeyID string) (*model.Credential, error)
	GetCredentials(ctx context.Context, accessKeyID string) (*model.Credential, error)
	ListUserCredentials(ctx context.Context, username string, params *model.PaginationParams) ([]*model.Credential, *model.Paginator, error)

	// policy<->user attachments
	AttachPolicyToUser(ctx context.Context, policyDisplayName, username string) error
	DetachPolicyFromUser(ctx context.Context, policyDisplayName, username string) error
	ListUserPolicies(ctx context.Context, username string, params *model.PaginationParams) ([]*model.Policy, *model.Paginator, error)
	ListEffectivePolicies(ctx context.Context, username string, params *model.PaginationParams) ([]*model.Policy, *model.Paginator, error)

	// policy<->group attachments
	AttachPolicyToGroup(ctx context.Context, policyDisplayName, groupDisplayName string) error
	DetachPolicyFromGroup(ctx context.Context, policyDisplayName, groupDisplayName string) error
	ListGroupPolicies(ctx context.Context, groupDisplayName string, params *model.PaginationParams) ([]*model.Policy, *model.Paginator, error)

	// authorize user for an action
	Authorize(ctx context.Context, req *AuthorizationRequest) (*AuthorizationResponse, error)
}

var psql = sq.StatementBuilder.PlaceholderFormat(sq.Dollar)

func ListPaged(ctx context.Context, db db.Querier, retType reflect.Type, params *model.PaginationParams, tokenColumnName string, queryBuilder sq.SelectBuilder) (*reflect.Value, *model.Paginator, error) {
	ptrType := reflect.PtrTo(retType)
	slice := reflect.MakeSlice(reflect.SliceOf(ptrType), 0, 0)
	queryBuilder = queryBuilder.OrderBy(tokenColumnName)
	if params != nil {
		queryBuilder = queryBuilder.Where(sq.Gt{tokenColumnName: params.After})
		if params.Amount >= 0 {
			queryBuilder = queryBuilder.Limit(uint64(params.Amount) + 1)
		}
	}
	query, args, err := queryBuilder.ToSql()
	if err != nil {
		return nil, nil, fmt.Errorf("convert to SQL: %w", err)
	}
	rows, err := db.Query(ctx, query, args...)
	if err != nil {
		return nil, nil, fmt.Errorf("query DB: %w", err)
	}
	rowScanner := pgxscan.NewRowScanner(rows)
	for rows.Next() {
		value := reflect.New(retType)
		if err = rowScanner.Scan(value.Interface()); err != nil {
			return nil, nil, fmt.Errorf("scan value from DB: %w", err)
		}
		slice = reflect.Append(slice, value)
	}
	p := &model.Paginator{}
	if params != nil && params.Amount >= 0 && slice.Len() == params.Amount+1 {
		// we have more pages
		slice = slice.Slice(0, params.Amount)
		p.Amount = params.Amount
		p.NextPageToken = slice.Index(slice.Len() - 1).Elem().FieldByName(tokenColumnName).String()
		return &slice, p, nil
	}
	p.Amount = slice.Len()
	return &slice, p, nil
}

func getUser(tx db.Tx, username string) (*model.User, error) {
	user := &model.User{}
	err := tx.Get(user, `SELECT * FROM auth_users WHERE display_name = $1`, username)
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

func deleteOrNotFound(tx db.Tx, stmt string, args ...interface{}) error {
	res, err := tx.Exec(stmt, args...)
	if err != nil {
		return err
	}
	numRows := res.RowsAffected()
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

func (s *DBAuthService) CreateUser(ctx context.Context, user *model.User) error {
	_, err := s.db.Transact(ctx, func(tx db.Tx) (interface{}, error) {
		if err := model.ValidateAuthEntityID(user.Username); err != nil {
			return nil, err
		}
		err := tx.Get(user,
			`INSERT INTO auth_users (display_name, created_at) VALUES ($1, $2) RETURNING id`, user.Username, user.CreatedAt)
		return nil, err
	})
	return err
}

func (s *DBAuthService) DeleteUser(ctx context.Context, username string) error {
	_, err := s.db.Transact(ctx, func(tx db.Tx) (interface{}, error) {
		return nil, deleteOrNotFound(tx, `DELETE FROM auth_users WHERE display_name = $1`, username)
	})
	return err
}

func (s *DBAuthService) GetUser(ctx context.Context, username string) (*model.User, error) {
	return s.cache.GetUser(username, func() (*model.User, error) {
		user, err := s.db.Transact(ctx, func(tx db.Tx) (interface{}, error) {
			return getUser(tx, username)
		}, db.ReadOnly())
		if err != nil {
			return nil, err
		}
		return user.(*model.User), nil
	})
}

func (s *DBAuthService) GetUserByID(ctx context.Context, userID int) (*model.User, error) {
	return s.cache.GetUserByID(userID, func() (*model.User, error) {
		user, err := s.db.Transact(ctx, func(tx db.Tx) (interface{}, error) {
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

func (s *DBAuthService) ListUsers(ctx context.Context, params *model.PaginationParams) ([]*model.User, *model.Paginator, error) {
	var user model.User
	slice, paginator, err := ListPaged(ctx, s.db, reflect.TypeOf(user), params, "display_name", psql.Select("*").From("auth_users"))
	if slice == nil {
		return nil, paginator, err
	}
	return slice.Interface().([]*model.User), paginator, err
}

func (s *DBAuthService) ListUserCredentials(ctx context.Context, username string, params *model.PaginationParams) ([]*model.Credential, *model.Paginator, error) {
	var credential model.Credential
	slice, paginator, err := ListPaged(ctx, s.db, reflect.TypeOf(credential), params, "auth_credentials.access_key_id", psql.Select("auth_credentials.*").
		From("auth_credentials").
		Join("auth_users ON (auth_credentials.user_id = auth_users.id)").
		Where(sq.Eq{"auth_users.display_name": username}))
	if slice == nil {
		return nil, paginator, err
	}
	return slice.Interface().([]*model.Credential), paginator, err
}

func (s *DBAuthService) AttachPolicyToUser(ctx context.Context, policyDisplayName, username string) error {
	_, err := s.db.Transact(ctx, func(tx db.Tx) (interface{}, error) {
		if _, err := getUser(tx, username); err != nil {
			return nil, fmt.Errorf("%s: %w", username, err)
		}
		if _, err := getPolicy(tx, policyDisplayName); err != nil {
			return nil, fmt.Errorf("%s: %w", policyDisplayName, err)
		}
		_, err := tx.Exec(`
			INSERT INTO auth_user_policies (user_id, policy_id)
			VALUES (
				(SELECT id FROM auth_users WHERE display_name = $1),
				(SELECT id FROM auth_policies WHERE display_name = $2)
			)`, username, policyDisplayName)
		if db.IsUniqueViolation(err) {
			return nil, fmt.Errorf("policy attachment: %w", db.ErrAlreadyExists)
		}
		return nil, err
	})
	return err
}

func (s *DBAuthService) DetachPolicyFromUser(ctx context.Context, policyDisplayName, username string) error {
	_, err := s.db.Transact(ctx, func(tx db.Tx) (interface{}, error) {
		if _, err := getUser(tx, username); err != nil {
			return nil, fmt.Errorf("%s: %w", username, err)
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
			username, policyDisplayName)
	})
	return err
}

func (s *DBAuthService) ListUserPolicies(ctx context.Context, username string, params *model.PaginationParams) ([]*model.Policy, *model.Paginator, error) {
	var policy model.Policy
	slice, paginator, err := ListPaged(ctx, s.db, reflect.TypeOf(policy), params, "auth_policies.display_name", psql.Select("auth_policies.*").
		From("auth_policies").
		Join("auth_user_policies ON (auth_policies.id = auth_user_policies.policy_id)").
		Join("auth_users ON (auth_user_policies.user_id = auth_users.id)").
		Where(sq.Eq{"auth_users.display_name": username}))
	if slice == nil {
		return nil, paginator, err
	}
	return slice.Interface().([]*model.Policy), paginator, nil
}

func (s *DBAuthService) getEffectivePolicies(ctx context.Context, username string, params *model.PaginationParams) ([]*model.Policy, *model.Paginator, error) {
	// resolve all policies attached to the user and its groups
	resolvedCte := `
	    WITH resolved_policies_view AS (
                SELECT auth_policies.id, auth_policies.created_at, auth_policies.display_name, auth_policies.statement, auth_users.display_name AS user_display_name
                FROM auth_policies INNER JOIN
                     auth_user_policies ON (auth_policies.id = auth_user_policies.policy_id) INNER JOIN
		     auth_users ON (auth_users.id = auth_user_policies.user_id)
                UNION
		SELECT auth_policies.id, auth_policies.created_at, auth_policies.display_name, auth_policies.statement, auth_users.display_name AS user_display_name
		FROM auth_policies INNER JOIN
		     auth_group_policies ON (auth_policies.id = auth_group_policies.policy_id) INNER JOIN
		     auth_groups ON (auth_groups.id = auth_group_policies.group_id) INNER JOIN
		     auth_user_groups ON (auth_user_groups.group_id = auth_groups.id) INNER JOIN
		     auth_users ON (auth_users.id = auth_user_groups.user_id)
	    )`
	var policy model.Policy
	slice, paginator, err := ListPaged(ctx,
		s.db, reflect.TypeOf(policy), params, "display_name",
		psql.Select("id", "created_at", "display_name", "statement").
			Prefix(resolvedCte).
			From("resolved_policies_view").
			Where(sq.Eq{"user_display_name": username}))

	if slice == nil {
		return nil, paginator, err
	}
	return slice.Interface().([]*model.Policy), paginator, nil
}

func (s *DBAuthService) ListEffectivePolicies(ctx context.Context, username string, params *model.PaginationParams) ([]*model.Policy, *model.Paginator, error) {
	if params.Amount == -1 {
		// read through the cache when requesting the full list
		policies, err := s.cache.GetUserPolicies(username, func() ([]*model.Policy, error) {
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

func (s *DBAuthService) ListGroupPolicies(ctx context.Context, groupDisplayName string, params *model.PaginationParams) ([]*model.Policy, *model.Paginator, error) {
	var policy model.Policy
	slice, paginator, err := ListPaged(ctx, s.db, reflect.TypeOf(policy), params, "auth_policies.display_name",
		psql.Select("auth_policies.*").
			From("auth_policies").
			Join("auth_group_policies ON (auth_policies.id = auth_group_policies.policy_id)").
			Join("auth_groups ON (auth_group_policies.group_id = auth_groups.id)").
			Where(sq.Eq{"auth_groups.display_name": groupDisplayName}))
	if err != nil {
		return nil, paginator, err
	}
	return slice.Interface().([]*model.Policy), paginator, nil
}

func (s *DBAuthService) CreateGroup(ctx context.Context, group *model.Group) error {
	_, err := s.db.Transact(ctx, func(tx db.Tx) (interface{}, error) {
		if err := model.ValidateAuthEntityID(group.DisplayName); err != nil {
			return nil, err
		}
		return nil, tx.Get(group, `INSERT INTO auth_groups (display_name, created_at) VALUES ($1, $2) RETURNING id`,
			group.DisplayName, group.CreatedAt)
	})
	return err
}

func (s *DBAuthService) DeleteGroup(ctx context.Context, groupDisplayName string) error {
	_, err := s.db.Transact(ctx, func(tx db.Tx) (interface{}, error) {
		return nil, deleteOrNotFound(tx, `DELETE FROM auth_groups WHERE display_name = $1`, groupDisplayName)
	})
	return err
}

func (s *DBAuthService) GetGroup(ctx context.Context, groupDisplayName string) (*model.Group, error) {
	group, err := s.db.Transact(ctx, func(tx db.Tx) (interface{}, error) {
		return getGroup(tx, groupDisplayName)
	}, db.ReadOnly())
	if err != nil {
		return nil, err
	}
	return group.(*model.Group), nil
}

func (s *DBAuthService) ListGroups(ctx context.Context, params *model.PaginationParams) ([]*model.Group, *model.Paginator, error) {
	var group model.Group
	slice, paginator, err := ListPaged(ctx, s.db, reflect.TypeOf(group), params, "display_name",
		psql.Select("*").From("auth_groups"))
	if err != nil {
		return nil, paginator, err
	}
	return slice.Interface().([]*model.Group), paginator, nil
}

func (s *DBAuthService) AddUserToGroup(ctx context.Context, username, groupDisplayName string) error {
	_, err := s.db.Transact(ctx, func(tx db.Tx) (interface{}, error) {
		if _, err := getUser(tx, username); err != nil {
			return nil, fmt.Errorf("%s: %w", username, err)
		}
		if _, err := getGroup(tx, groupDisplayName); err != nil {
			return nil, fmt.Errorf("%s: %w", groupDisplayName, err)
		}
		_, err := tx.Exec(`
			INSERT INTO auth_user_groups (user_id, group_id)
			VALUES (
				(SELECT id FROM auth_users WHERE display_name = $1),
				(SELECT id FROM auth_groups WHERE display_name = $2)
			)`, username, groupDisplayName)
		if db.IsUniqueViolation(err) {
			return nil, fmt.Errorf("group membership: %w", db.ErrAlreadyExists)
		}
		return nil, err
	})
	return err
}

func (s *DBAuthService) RemoveUserFromGroup(ctx context.Context, username, groupDisplayName string) error {
	_, err := s.db.Transact(ctx, func(tx db.Tx) (interface{}, error) {
		if _, err := getUser(tx, username); err != nil {
			return nil, fmt.Errorf("%s: %w", username, err)
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
			username, groupDisplayName)
	})
	return err
}

func (s *DBAuthService) ListUserGroups(ctx context.Context, username string, params *model.PaginationParams) ([]*model.Group, *model.Paginator, error) {
	type res struct {
		groups    []*model.Group
		paginator *model.Paginator
	}
	result, err := s.db.Transact(ctx, func(tx db.Tx) (interface{}, error) {
		if _, err := getUser(tx, username); err != nil {
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
			username, params.After, params.Amount+1)
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

func (s *DBAuthService) ListGroupUsers(ctx context.Context, groupDisplayName string, params *model.PaginationParams) ([]*model.User, *model.Paginator, error) {
	type res struct {
		users     []*model.User
		paginator *model.Paginator
	}
	result, err := s.db.Transact(ctx, func(tx db.Tx) (interface{}, error) {
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
			p.NextPageToken = users[len(users)-1].Username
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

func (s *DBAuthService) WritePolicy(ctx context.Context, policy *model.Policy) error {
	_, err := s.db.Transact(ctx, func(tx db.Tx) (interface{}, error) {
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

func (s *DBAuthService) GetPolicy(ctx context.Context, policyDisplayName string) (*model.Policy, error) {
	policy, err := s.db.Transact(ctx, func(tx db.Tx) (interface{}, error) {
		return getPolicy(tx, policyDisplayName)
	}, db.ReadOnly())
	if err != nil {
		return nil, err
	}
	return policy.(*model.Policy), nil
}

func (s *DBAuthService) DeletePolicy(ctx context.Context, policyDisplayName string) error {
	_, err := s.db.Transact(ctx, func(tx db.Tx) (interface{}, error) {
		return nil, deleteOrNotFound(tx, `DELETE FROM auth_policies WHERE display_name = $1`, policyDisplayName)
	})
	return err
}

func (s *DBAuthService) ListPolicies(ctx context.Context, params *model.PaginationParams) ([]*model.Policy, *model.Paginator, error) {
	type res struct {
		policies  []*model.Policy
		paginator *model.Paginator
	}
	result, err := s.db.Transact(ctx, func(tx db.Tx) (interface{}, error) {
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

func (s *DBAuthService) CreateCredentials(ctx context.Context, username string) (*model.Credential, error) {
	accessKeyID := genAccessKeyID()
	secretAccessKey := genAccessSecretKey()
	return s.AddCredentials(ctx, username, accessKeyID, secretAccessKey)
}

func (s *DBAuthService) AddCredentials(ctx context.Context, username, accessKeyID, secretAccessKey string) (*model.Credential, error) {
	now := time.Now()
	encryptedKey, err := s.encryptSecret(secretAccessKey)
	if err != nil {
		return nil, err
	}
	credentials, err := s.db.Transact(ctx, func(tx db.Tx) (interface{}, error) {
		user, err := getUser(tx, username)
		if err != nil {
			return nil, err
		}
		c := &model.Credential{
			AccessKeyID:                   accessKeyID,
			AccessSecretKey:               secretAccessKey,
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

func (s *DBAuthService) DeleteCredentials(ctx context.Context, username, accessKeyID string) error {
	_, err := s.db.Transact(ctx, func(tx db.Tx) (interface{}, error) {
		return nil, deleteOrNotFound(tx, `
			DELETE FROM auth_credentials USING auth_users
			WHERE auth_credentials.user_id = auth_users.id
				AND auth_users.display_name = $1
				AND auth_credentials.access_key_id = $2`,
			username, accessKeyID)
	})
	return err
}

func (s *DBAuthService) AttachPolicyToGroup(ctx context.Context, policyDisplayName, groupDisplayName string) error {
	_, err := s.db.Transact(ctx, func(tx db.Tx) (interface{}, error) {
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

func (s *DBAuthService) DetachPolicyFromGroup(ctx context.Context, policyDisplayName, groupDisplayName string) error {
	_, err := s.db.Transact(ctx, func(tx db.Tx) (interface{}, error) {
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

func (s *DBAuthService) GetCredentialsForUser(ctx context.Context, username, accessKeyID string) (*model.Credential, error) {
	credentials, err := s.db.Transact(ctx, func(tx db.Tx) (interface{}, error) {
		if _, err := getUser(tx, username); err != nil {
			return nil, err
		}
		credentials := &model.Credential{}
		err := tx.Get(credentials, `
			SELECT auth_credentials.*
			FROM auth_credentials
			INNER JOIN auth_users ON (auth_credentials.user_id = auth_users.id)
			WHERE auth_credentials.access_key_id = $1
				AND auth_users.display_name = $2`, accessKeyID, username)
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

func (s *DBAuthService) GetCredentials(ctx context.Context, accessKeyID string) (*model.Credential, error) {
	return s.cache.GetCredential(accessKeyID, func() (*model.Credential, error) {
		credentials, err := s.db.Transact(ctx, func(tx db.Tx) (interface{}, error) {
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

func interpolateUser(resource string, username string) string {
	return strings.ReplaceAll(resource, "${user}", username)
}

func (s *DBAuthService) Authorize(ctx context.Context, req *AuthorizationRequest) (*AuthorizationResponse, error) {
	policies, _, err := s.ListEffectivePolicies(ctx, req.Username, &model.PaginationParams{
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
				resource := interpolateUser(stmt.Resource, req.Username)
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
