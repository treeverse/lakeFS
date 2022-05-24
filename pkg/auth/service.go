package auth

//go:generate oapi-codegen -package auth -generate "types,client"  -o client.gen.go ../../api/authorization.yml

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"reflect"
	"strings"
	"time"

	sq "github.com/Masterminds/squirrel"
	"github.com/deepmap/oapi-codegen/pkg/securityprovider"
	"github.com/georgysavva/scany/pgxscan"
	"github.com/getkin/kin-openapi/openapi3filter"
	"github.com/go-openapi/swag"
	"github.com/treeverse/lakefs/pkg/auth/crypt"
	"github.com/treeverse/lakefs/pkg/auth/keys"
	"github.com/treeverse/lakefs/pkg/auth/model"
	"github.com/treeverse/lakefs/pkg/auth/params"
	"github.com/treeverse/lakefs/pkg/auth/wildcard"
	"github.com/treeverse/lakefs/pkg/db"
	"github.com/treeverse/lakefs/pkg/logging"
	"github.com/treeverse/lakefs/pkg/permissions"
	"golang.org/x/crypto/bcrypt"
)

type AuthorizationRequest struct {
	Username            string
	RequiredPermissions permissions.Node
}

type AuthorizationResponse struct {
	Allowed bool
	Error   error
}

const InvalidUserID = -1

type GatewayService interface {
	GetCredentials(_ context.Context, accessKey string) (*model.Credential, error)
	GetUserByID(ctx context.Context, userID int64) (*model.User, error)
	Authorize(_ context.Context, req *AuthorizationRequest) (*AuthorizationResponse, error)
}

type Service interface {
	SecretStore() crypt.SecretStore

	// users
	CreateUser(ctx context.Context, user *model.User) (int64, error)
	DeleteUser(ctx context.Context, username string) error
	GetUserByID(ctx context.Context, userID int64) (*model.User, error)
	GetUser(ctx context.Context, username string) (*model.User, error)
	GetUserByEmail(ctx context.Context, email string) (*model.User, error)
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
	HashAndUpdatePassword(ctx context.Context, username string, password string) error

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

	ClaimTokenIDOnce(ctx context.Context, tokenID string, expiresAt int64) error
}

var psql = sq.StatementBuilder.PlaceholderFormat(sq.Dollar)

// fieldNameByTag returns the name of the field of t that is tagged tag on key, or an empty string.
func fieldByTag(t reflect.Type, key, tag string) string {
	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		if l, ok := field.Tag.Lookup(key); ok {
			if l == tag {
				return field.Name
			}
		}
	}
	return ""
}

const maxPage = 1000

func ListPaged(ctx context.Context, db db.Querier, retType reflect.Type, params *model.PaginationParams, tokenColumnName string, queryBuilder sq.SelectBuilder) (*reflect.Value, *model.Paginator, error) {
	ptrType := reflect.PtrTo(retType)
	tokenField := fieldByTag(retType, "db", tokenColumnName)
	if tokenField == "" {
		return nil, nil, fmt.Errorf("[I] no field %s: %w", tokenColumnName, ErrNoField)
	}
	slice := reflect.MakeSlice(reflect.SliceOf(ptrType), 0, 0)
	queryBuilder = queryBuilder.OrderBy(tokenColumnName)
	amount := 0
	if params != nil {
		queryBuilder = queryBuilder.Where(sq.Gt{tokenColumnName: params.After})
		if params.Amount >= 0 {
			amount = params.Amount + 1
		}
	}
	if amount > maxPage {
		amount = maxPage
	}
	if amount > 0 {
		queryBuilder = queryBuilder.Limit(uint64(amount))
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
		lastElem := slice.Index(slice.Len() - 1).Elem()
		p.NextPageToken = lastElem.FieldByName(tokenField).String()
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

func getUserByEmail(tx db.Tx, email string) (*model.User, error) {
	user := &model.User{}
	err := tx.Get(user, `SELECT * FROM auth_users WHERE email = $1`, email)
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
		return ErrNotFound
	}
	return nil
}

type DBAuthService struct {
	db          db.Database
	secretStore crypt.SecretStore
	cache       Cache
	log         logging.Logger
}

func NewDBAuthService(db db.Database, secretStore crypt.SecretStore, cacheConf params.ServiceCache, logger logging.Logger) *DBAuthService {
	logger.Info("initialized Auth service")
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
		log:         logger,
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

func (s *DBAuthService) CreateUser(ctx context.Context, user *model.User) (int64, error) {
	id, err := s.db.Transact(ctx, func(tx db.Tx) (interface{}, error) {
		if err := model.ValidateAuthEntityID(user.Username); err != nil {
			return nil, err
		}
		var id int64
		err := tx.Get(&id,
			`INSERT INTO auth_users (display_name, created_at, friendly_name, source, email) VALUES ($1, $2, $3, $4, $5) RETURNING id`,
			user.Username, user.CreatedAt, user.FriendlyName, user.Source, user.Email)
		return id, err
	})
	if err != nil {
		return InvalidUserID, err
	}
	return id.(int64), err
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

func (s *DBAuthService) GetUserByEmail(ctx context.Context, email string) (*model.User, error) {
	user, err := s.db.Transact(ctx, func(tx db.Tx) (interface{}, error) {
		return getUserByEmail(tx, email)
	}, db.ReadOnly())
	if err != nil {
		return nil, err
	}
	return user.(*model.User), nil
}

func (s *DBAuthService) GetUserByID(ctx context.Context, userID int64) (*model.User, error) {
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
	slice, paginator, err := ListPaged(ctx, s.db, reflect.TypeOf(user), params, "display_name",
		psql.Select("*").
			From("auth_users").
			Where(sq.Like{"display_name": fmt.Sprint(params.Prefix, "%")}))
	if slice == nil {
		return nil, paginator, err
	}
	return slice.Interface().([]*model.User), paginator, err
}

func (s *DBAuthService) ListUserCredentials(ctx context.Context, username string, params *model.PaginationParams) ([]*model.Credential, *model.Paginator, error) {
	var credential model.Credential
	slice, paginator, err := ListPaged(ctx, s.db, reflect.TypeOf(credential), params, "access_key_id", psql.Select("auth_credentials.*").
		From("auth_credentials").
		Join("auth_users ON (auth_credentials.user_id = auth_users.id)").
		Where(sq.And{
			sq.Eq{"auth_users.display_name": username},
			sq.Like{"display_name": fmt.Sprint(params.Prefix, "%")},
		}))
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
		if errors.Is(err, db.ErrAlreadyExists) {
			return nil, fmt.Errorf("policy attachment: %w", ErrAlreadyExists)
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
	sub := psql.Select("auth_policies.*").
		From("auth_policies").
		Join("auth_user_policies ON (auth_policies.id = auth_user_policies.policy_id)").
		Join("auth_users ON (auth_user_policies.user_id = auth_users.id)").
		Where(sq.And{
			sq.Eq{"auth_users.display_name": username},
			sq.Like{"auth_policies.display_name": fmt.Sprint(params.Prefix, "%")},
		})
	slice, paginator, err := ListPaged(ctx, s.db, reflect.TypeOf(policy), params, "display_name",
		psql.Select("*").FromSelect(sub, "p"))
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
			Where(sq.And{
				sq.Eq{"user_display_name": username},
				sq.Like{"display_name": fmt.Sprint(params.Prefix, "%")},
			}))

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
	query := psql.Select("auth_policies.*").
		From("auth_policies").
		Join("auth_group_policies ON (auth_policies.id = auth_group_policies.policy_id)").
		Join("auth_groups ON (auth_group_policies.group_id = auth_groups.id)").
		Where(sq.And{
			sq.Eq{"auth_groups.display_name": groupDisplayName},
			sq.Like{"auth_policies.display_name": fmt.Sprint(params.Prefix, "%")},
		})
	slice, paginator, err := ListPaged(ctx, s.db, reflect.TypeOf(policy), params, "display_name",
		psql.Select("*").FromSelect(query, "p"))
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
		psql.Select("*").
			From("auth_groups").
			Where(sq.Like{"display_name": fmt.Sprint(params.Prefix, "%")}))
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
		if errors.Is(err, db.ErrAlreadyExists) {
			return nil, fmt.Errorf("group membership: %w", ErrAlreadyExists)
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
				AND auth_groups.display_name like $3
			ORDER BY auth_groups.display_name
			LIMIT $4`,
			username, params.After, fmt.Sprint(params.Prefix, "%"), params.Amount+1)
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
				AND auth_users.display_name like $3
			ORDER BY auth_groups.display_name
			LIMIT $4`,
			groupDisplayName, params.After, fmt.Sprint(params.Prefix, "%"), params.Amount+1)
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
			AND display_name LIKE $2
			ORDER BY display_name
			LIMIT $3`,
			params.After, fmt.Sprint(params.Prefix, "%"), params.Amount+1)
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
	accessKeyID := keys.GenAccessKeyID()
	secretAccessKey := keys.GenSecretAccessKey()
	return s.AddCredentials(ctx, username, accessKeyID, secretAccessKey)
}

func (s *DBAuthService) AddCredentials(ctx context.Context, username, accessKeyID, secretAccessKey string) (*model.Credential, error) {
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
	credentials, err := s.db.Transact(ctx, func(tx db.Tx) (interface{}, error) {
		user, err := getUser(tx, username)
		if err != nil {
			return nil, err
		}
		c := &model.Credential{
			AccessKeyID:                   accessKeyID,
			SecretAccessKey:               secretAccessKey,
			SecretAccessKeyEncryptedBytes: encryptedKey,
			IssuedDate:                    now,
			UserID:                        user.ID,
		}
		_, err = tx.Exec(`
			INSERT INTO auth_credentials (access_key_id, secret_access_key, issued_date, user_id)
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

func IsValidAccessKeyID(key string) bool {
	l := len(key)
	return l >= 3 && l <= 20
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
		if errors.Is(err, db.ErrAlreadyExists) {
			return nil, fmt.Errorf("policy attachment: %w", ErrAlreadyExists)
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
			err := tx.Get(credentials, `SELECT * FROM auth_credentials WHERE auth_credentials.access_key_id = $1`,
				accessKeyID)
			if err != nil {
				return nil, err
			}
			key, err := s.decryptSecret(credentials.SecretAccessKeyEncryptedBytes)
			if err != nil {
				return nil, err
			}
			credentials.SecretAccessKey = key
			return credentials, nil
		})
		if err != nil {
			return nil, err
		}
		return credentials.(*model.Credential), nil
	})
}

func (s *DBAuthService) HashAndUpdatePassword(ctx context.Context, username string, password string) error {
	pw, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
	if err != nil {
		return err
	}
	_, err = s.db.Exec(ctx, `UPDATE auth_users SET encrypted_password = $1 WHERE display_name = $2`, pw, username)
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

func checkPermissions(node permissions.Node, username string, policies []*model.Policy) CheckResult {
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

func (s *DBAuthService) Authorize(ctx context.Context, req *AuthorizationRequest) (*AuthorizationResponse, error) {
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

func (s *DBAuthService) ClaimTokenIDOnce(ctx context.Context, tokenID string, expiresAt int64) error {
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
func (s *DBAuthService) markTokenSingleUse(ctx context.Context, tokenID string, tokenExpiresAt time.Time) (bool, error) {
	res, err := s.db.Exec(ctx, `INSERT INTO auth_expired_tokens (token_id, token_expires_at) VALUES ($1,$2) ON CONFLICT DO NOTHING`,
		tokenID, tokenExpiresAt)
	if err != nil {
		return false, err
	}
	canUseToken := res.RowsAffected() == 1
	// cleanup old tokens
	_, err = s.db.Exec(ctx, `DELETE FROM auth_expired_tokens WHERE token_expires_at < $1`, time.Now())
	if err != nil {
		s.log.WithError(err).Error("delete expired tokens")
	}
	return canUseToken, nil
}

type APIAuthService struct {
	apiClient   *ClientWithResponses
	secretStore crypt.SecretStore
	cache       Cache
}

func (a *APIAuthService) SecretStore() crypt.SecretStore {
	return a.secretStore
}

func (a *APIAuthService) CreateUser(ctx context.Context, user *model.User) (int64, error) {
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

	return resp.JSON201.Id, nil
}

func (a *APIAuthService) DeleteUser(ctx context.Context, username string) error {
	resp, err := a.apiClient.DeleteUserWithResponse(ctx, username)
	if err != nil {
		return err
	}
	return a.validateResponse(resp, http.StatusNoContent)
}

func (a *APIAuthService) GetUserByID(ctx context.Context, userID int64) (*model.User, error) {
	return a.cache.GetUserByID(userID, func() (*model.User, error) {
		resp, err := a.apiClient.ListUsersWithResponse(ctx, &ListUsersParams{Id: &userID})
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
			ID:                u.Id,
			CreatedAt:         time.Unix(u.CreationDate, 0),
			Username:          u.Name,
			FriendlyName:      u.FriendlyName,
			Email:             u.Email,
			EncryptedPassword: nil,
			Source:            "",
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
			ID:                u.Id,
			CreatedAt:         time.Unix(u.CreationDate, 0),
			Username:          u.Name,
			FriendlyName:      u.FriendlyName,
			Email:             u.Email,
			EncryptedPassword: u.EncryptedPassword,
			Source:            swag.StringValue(u.Source),
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
			ID:                u.Id,
			CreatedAt:         time.Unix(u.CreationDate, 0),
			Username:          u.Name,
			FriendlyName:      u.FriendlyName,
			Email:             u.Email,
			EncryptedPassword: u.EncryptedPassword,
			Source:            swag.StringValue(u.Source),
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
			ID:                0,
			CreatedAt:         time.Unix(r.CreationDate, 0),
			Username:          r.Name,
			FriendlyName:      r.FriendlyName,
			Email:             r.Email,
			EncryptedPassword: nil,
			Source:            swag.StringValue(r.Source),
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

func (a *APIAuthService) CreateGroup(ctx context.Context, group *model.Group) error {
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
		CreatedAt:   time.Unix(resp.JSON200.CreationDate, 0),
		DisplayName: resp.JSON200.Name,
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
			ID:          0,
			CreatedAt:   time.Unix(r.CreationDate, 0),
			DisplayName: r.Name,
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
			ID:          0,
			CreatedAt:   time.Unix(r.CreationDate, 0),
			DisplayName: r.Name,
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
			ID:           0,
			CreatedAt:    time.Unix(r.CreationDate, 0),
			Username:     r.Name,
			FriendlyName: r.FriendlyName,
			Email:        r.Email,
		}
	}
	return members, toPagination(resp.JSON200.Pagination), nil
}

func (a *APIAuthService) WritePolicy(ctx context.Context, policy *model.Policy) error {
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

func serializePolicyToModalPolicy(p Policy) *model.Policy {
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
	return &model.Policy{
		ID:          0,
		CreatedAt:   creationTime,
		DisplayName: p.Name,
		Statement:   stmts,
	}
}

func (a *APIAuthService) GetPolicy(ctx context.Context, policyDisplayName string) (*model.Policy, error) {
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

func (a *APIAuthService) ListPolicies(ctx context.Context, params *model.PaginationParams) ([]*model.Policy, *model.Paginator, error) {
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
	policies := make([]*model.Policy, len(resp.JSON200.Results))

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
		UserID:          0,
		AccessKeyID:     credentials.AccessKeyId,
		SecretAccessKey: credentials.SecretAccessKey,
		IssuedDate:      time.Unix(credentials.CreationDate, 0),
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
		UserID:          0,
		AccessKeyID:     credentials.AccessKeyId,
		SecretAccessKey: credentials.SecretAccessKey,
		IssuedDate:      time.Unix(credentials.CreationDate, 0),
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
		AccessKeyID: credentials.AccessKeyId,
		IssuedDate:  time.Unix(credentials.CreationDate, 0),
		UserID:      0,
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
			AccessKeyID:                   credentials.AccessKeyId,
			SecretAccessKey:               credentials.SecretAccessKey,
			SecretAccessKeyEncryptedBytes: nil,
			IssuedDate:                    time.Unix(credentials.CreationDate, 0),
			UserID:                        credentials.UserId,
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
			AccessKeyID: r.AccessKeyId,
			IssuedDate:  time.Unix(r.CreationDate, 0),
			UserID:      0,
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

func (a *APIAuthService) listUserPolicies(ctx context.Context, username string, params *model.PaginationParams, effective bool) ([]*model.Policy, *model.Paginator, error) {
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
	policies := make([]*model.Policy, len(resp.JSON200.Results))

	for i, r := range resp.JSON200.Results {
		policies[i] = serializePolicyToModalPolicy(r)
	}
	return policies, toPagination(resp.JSON200.Pagination), nil
}

func (a *APIAuthService) ListUserPolicies(ctx context.Context, username string, params *model.PaginationParams) ([]*model.Policy, *model.Paginator, error) {
	return a.listUserPolicies(ctx, username, params, false)
}

func (a *APIAuthService) listAllEffectivePolicies(ctx context.Context, username string) ([]*model.Policy, error) {
	hasMore := true
	after := ""
	amount := maxPage
	policies := make([]*model.Policy, 0)
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

func (a *APIAuthService) ListEffectivePolicies(ctx context.Context, username string, params *model.PaginationParams) ([]*model.Policy, *model.Paginator, error) {
	if params.Amount == -1 {
		// read through the cache when requesting the full list
		policies, err := a.cache.GetUserPolicies(username, func() ([]*model.Policy, error) {
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

func (a *APIAuthService) ListGroupPolicies(ctx context.Context, groupDisplayName string, params *model.PaginationParams) ([]*model.Policy, *model.Paginator, error) {
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
	policies := make([]*model.Policy, len(resp.JSON200.Results))

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
