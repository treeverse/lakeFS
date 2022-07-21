package auth

//go:generate oapi-codegen -package auth -generate "types,client"  -o client.gen.go ../../api/authorization.yml

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"reflect"
	"time"

	sq "github.com/Masterminds/squirrel"
	"github.com/georgysavva/scany/pgxscan"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/treeverse/lakefs/pkg/auth/crypt"
	"github.com/treeverse/lakefs/pkg/auth/email"
	"github.com/treeverse/lakefs/pkg/auth/keys"
	"github.com/treeverse/lakefs/pkg/auth/model"
	"github.com/treeverse/lakefs/pkg/auth/params"
	"github.com/treeverse/lakefs/pkg/db"
	"github.com/treeverse/lakefs/pkg/kv"
	kvpg "github.com/treeverse/lakefs/pkg/kv/postgres"
	"github.com/treeverse/lakefs/pkg/logging"
	"github.com/treeverse/lakefs/pkg/version"
	"golang.org/x/crypto/bcrypt"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type UserIDToName map[int64]string

type IDToName map[int]string

var ErrExportedEntNotFound = errors.New("previously exported entity not found")

//nolint:gochecknoinits
func init() {
	tablesToDrop := []string{
		"auth_users",
		"auth_groups",
		"auth_policies",
		"auth_user_groups",
		"auth_user_policies",
		"auth_group_policies",
		"auth_credentials",
		"auth_expired_tokens",
		"auth_installation_metadata",
	}
	kvpg.RegisterMigrate(model.PackageName, Migrate, tablesToDrop)
}

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

func getDBUser(tx db.Tx, username string) (*model.DBUser, error) {
	user := &model.DBUser{}
	err := tx.Get(user, `SELECT * FROM auth_users WHERE display_name = $1`, username)
	if err != nil {
		return nil, err
	}
	return user, nil
}

func getDBUserByEmail(tx db.Tx, email string) (*model.DBUser, error) {
	user := &model.DBUser{}
	err := tx.Get(user, `SELECT * FROM auth_users WHERE email = $1`, email)
	if err != nil {
		return nil, err
	}
	return user, nil
}

func getDBUserByExternalID(tx db.Tx, externalID string) (*model.DBUser, error) {
	user := &model.DBUser{}
	err := tx.Get(user, `SELECT * FROM auth_users WHERE external_id = $1`, externalID)
	if err != nil {
		return nil, err
	}
	return user, nil
}

func getDBGroup(tx db.Tx, groupDisplayName string) (*model.DBGroup, error) {
	group := &model.DBGroup{}
	err := tx.Get(group, `SELECT * FROM auth_groups WHERE display_name = $1`, groupDisplayName)
	if err != nil {
		return nil, err
	}
	return group, nil
}

func getDBPolicy(tx db.Tx, policyDisplayName string) (*model.DBPolicy, error) {
	policy := &model.DBPolicy{}
	err := tx.Get(policy, `SELECT * FROM auth_policies WHERE display_name = $1`, policyDisplayName)
	if err != nil {
		return nil, err
	}
	return policy, nil
}

func deleteOrNotFoundDB(tx db.Tx, stmt string, args ...interface{}) error {
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
	*EmailInviteHandler
	db          db.Database
	secretStore crypt.SecretStore
	cache       Cache
	log         logging.Logger
}

func NewDBAuthService(db db.Database, secretStore crypt.SecretStore, emailer *email.Emailer, cacheConf params.ServiceCache, logger logging.Logger) *DBAuthService {
	logger.Info("initialized Auth service")
	var cache Cache
	if cacheConf.Enabled {
		cache = NewLRUCache(cacheConf.Size, cacheConf.TTL, cacheConf.EvictionJitter)
	} else {
		cache = &DummyCache{}
	}
	d := &DBAuthService{
		db:          db,
		secretStore: secretStore,
		cache:       cache,
		log:         logger,
	}
	d.EmailInviteHandler = NewEmailInviteHandler(d, logger, emailer)
	return d
}

func (s *DBAuthService) SecretStore() crypt.SecretStore {
	return s.secretStore
}

func (s *DBAuthService) Cache() Cache {
	return s.cache
}

func (s *DBAuthService) DB() db.Database {
	return s.db
}

func (s *DBAuthService) CreateUser(ctx context.Context, user *model.User) (string, error) {
	id, err := s.db.Transact(ctx, func(tx db.Tx) (interface{}, error) {
		if err := model.ValidateAuthEntityID(user.Username); err != nil {
			return nil, err
		}
		var id int64
		err := tx.Get(&id,
			`INSERT INTO auth_users (display_name, created_at, friendly_name, source, email, external_id) VALUES ($1, $2, $3, $4, $5, $6) RETURNING id`,
			user.Username, user.CreatedAt, user.FriendlyName, user.Source, user.Email, user.ExternalID)
		return id, err
	})
	if err != nil {
		return InvalidUserID, err
	}
	return fmt.Sprint(id), nil
}

func (s *DBAuthService) DeleteUser(ctx context.Context, username string) error {
	_, err := s.db.Transact(ctx, func(tx db.Tx) (interface{}, error) {
		return nil, deleteOrNotFoundDB(tx, `DELETE FROM auth_users WHERE display_name = $1`, username)
	})
	return err
}

func (s *DBAuthService) GetUser(ctx context.Context, username string) (*model.User, error) {
	return s.cache.GetUser(&userKey{username: username}, func() (*model.User, error) {
		res, err := s.db.Transact(ctx, func(tx db.Tx) (interface{}, error) {
			return getDBUser(tx, username)
		}, db.ReadOnly())
		if err != nil {
			return nil, err
		}
		user := res.(*model.DBUser)
		return &user.User, nil
	})
}

// GetUserByEmail returns a user by their email.
// It doesn't cache the result in order to avoid a stale user after password reset.
func (s *DBAuthService) GetUserByEmail(ctx context.Context, email string) (*model.User, error) {
	res, err := s.db.Transact(ctx, func(tx db.Tx) (interface{}, error) {
		return getDBUserByEmail(tx, email)
	}, db.ReadOnly())
	if err != nil {
		return nil, err
	}
	user := res.(*model.DBUser)
	return &user.User, nil
}

func (s *DBAuthService) GetUserByExternalID(ctx context.Context, externalID string) (*model.User, error) {
	return s.cache.GetUser(&userKey{externalID: externalID}, func() (*model.User, error) {
		res, err := s.db.Transact(ctx, func(tx db.Tx) (interface{}, error) {
			return getDBUserByExternalID(tx, externalID)
		}, db.ReadOnly())
		if err != nil {
			return nil, err
		}
		user := res.(*model.DBUser)
		return &user.User, nil
	})
}

func (s *DBAuthService) GetUserByID(ctx context.Context, userID string) (*model.User, error) {
	return s.cache.GetUser(&userKey{id: userID}, func() (*model.User, error) {
		res, err := s.db.Transact(ctx, func(tx db.Tx) (interface{}, error) {
			user := &model.DBUser{}
			err := tx.Get(user, `SELECT * FROM auth_users WHERE id = $1`, userID)
			if err != nil {
				return nil, err
			}
			return user, nil
		}, db.ReadOnly())
		if err != nil {
			return nil, err
		}

		user := res.(*model.DBUser)
		return &user.User, nil
	})
}

func (s *DBAuthService) ListUsers(ctx context.Context, params *model.PaginationParams) ([]*model.User, *model.Paginator, error) {
	var user model.DBUser
	slice, paginator, err := ListPaged(ctx, s.db, reflect.TypeOf(user), params, "display_name",
		psql.Select("*").
			From("auth_users").
			Where(sq.Like{"display_name": fmt.Sprint(params.Prefix, "%")}))
	if slice == nil {
		return nil, paginator, err
	}
	return model.ConvertUsersList(slice.Interface().([]*model.DBUser)), paginator, err
}

func (s *DBAuthService) ListUserCredentials(ctx context.Context, username string, params *model.PaginationParams) ([]*model.Credential, *model.Paginator, error) {
	var credential model.DBCredential
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
	return model.ConvertCredList(slice.Interface().([]*model.DBCredential), username), paginator, err
}

func (s *DBAuthService) AttachPolicyToUser(ctx context.Context, policyDisplayName, username string) error {
	_, err := s.db.Transact(ctx, func(tx db.Tx) (interface{}, error) {
		if _, err := getDBUser(tx, username); err != nil {
			return nil, fmt.Errorf("%s: %w", username, err)
		}
		if _, err := getDBPolicy(tx, policyDisplayName); err != nil {
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
		if _, err := getDBUser(tx, username); err != nil {
			return nil, fmt.Errorf("%s: %w", username, err)
		}
		if _, err := getDBPolicy(tx, policyDisplayName); err != nil {
			return nil, fmt.Errorf("%s: %w", policyDisplayName, err)
		}
		return nil, deleteOrNotFoundDB(tx, `
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
	var policy model.DBPolicy
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
	policies := slice.Interface().([]*model.DBPolicy)
	res := make([]*model.Policy, 0, len(policies))
	for _, p := range policies {
		res = append(res, &p.Policy)
	}
	return res, paginator, nil
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
	var policy model.DBPolicy
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
	policies := slice.Interface().([]*model.DBPolicy)
	res := make([]*model.Policy, 0, len(policies))
	for _, p := range policies {
		res = append(res, &p.Policy)
	}
	return res, paginator, nil
}

func (s *DBAuthService) ListEffectivePolicies(ctx context.Context, username string, params *model.PaginationParams) ([]*model.Policy, *model.Paginator, error) {
	return ListEffectivePolicies(ctx, username, params, s.getEffectivePolicies, s.cache)
}

func (s *DBAuthService) ListGroupPolicies(ctx context.Context, groupDisplayName string, params *model.PaginationParams) ([]*model.Policy, *model.Paginator, error) {
	var policy model.DBPolicy
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
	dbPolicies := slice.Interface().([]*model.DBPolicy)
	policies := make([]*model.Policy, len(dbPolicies))
	for i := range dbPolicies {
		policies[i] = &dbPolicies[i].Policy
	}
	return policies, paginator, nil
}

func (s *DBAuthService) CreateGroup(ctx context.Context, group *model.Group) error {
	var id int
	_, err := s.db.Transact(ctx, func(tx db.Tx) (interface{}, error) {
		if err := model.ValidateAuthEntityID(group.DisplayName); err != nil {
			return nil, err
		}
		return nil, tx.Get(&id, `INSERT INTO auth_groups (display_name, created_at) VALUES ($1, $2) RETURNING id`,
			group.DisplayName, group.CreatedAt)
	})
	return err
}

func (s *DBAuthService) DeleteGroup(ctx context.Context, groupDisplayName string) error {
	_, err := s.db.Transact(ctx, func(tx db.Tx) (interface{}, error) {
		return nil, deleteOrNotFoundDB(tx, `DELETE FROM auth_groups WHERE display_name = $1`, groupDisplayName)
	})
	return err
}

func (s *DBAuthService) GetGroup(ctx context.Context, groupDisplayName string) (*model.Group, error) {
	res, err := s.db.Transact(ctx, func(tx db.Tx) (interface{}, error) {
		return getDBGroup(tx, groupDisplayName)
	}, db.ReadOnly())
	if err != nil {
		return nil, err
	}
	group := res.(*model.DBGroup)
	return &group.Group, nil
}

func (s *DBAuthService) ListGroups(ctx context.Context, params *model.PaginationParams) ([]*model.Group, *model.Paginator, error) {
	var group model.DBGroup
	slice, paginator, err := ListPaged(ctx, s.db, reflect.TypeOf(group), params, "display_name",
		psql.Select("*").
			From("auth_groups").
			Where(sq.Like{"display_name": fmt.Sprint(params.Prefix, "%")}))
	if err != nil {
		return nil, paginator, err
	}
	groups := slice.Interface().([]*model.DBGroup)
	return model.ConvertGroupList(groups), paginator, nil
}

func (s *DBAuthService) AddUserToGroup(ctx context.Context, username, groupDisplayName string) error {
	_, err := s.db.Transact(ctx, func(tx db.Tx) (interface{}, error) {
		if _, err := getDBUser(tx, username); err != nil {
			return nil, fmt.Errorf("%s: %w", username, err)
		}
		if _, err := getDBGroup(tx, groupDisplayName); err != nil {
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
		if _, err := getDBUser(tx, username); err != nil {
			return nil, fmt.Errorf("%s: %w", username, err)
		}
		if _, err := getDBGroup(tx, groupDisplayName); err != nil {
			return nil, fmt.Errorf("%s: %w", groupDisplayName, err)
		}
		return nil, deleteOrNotFoundDB(tx, `
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
		if _, err := getDBUser(tx, username); err != nil {
			return nil, err
		}
		dbGroups := make([]*model.DBGroup, 0)
		err := tx.Select(&dbGroups, `
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
		groups := make([]*model.Group, len(dbGroups))
		for i := range dbGroups {
			groups[i] = &dbGroups[i].Group
		}
		p := &model.Paginator{}
		if len(groups) == params.Amount+1 {
			// we have more pages
			groups = groups[0:params.Amount]
			p.Amount = params.Amount
			p.NextPageToken = groups[len(dbGroups)-1].DisplayName
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
		if _, err := getDBGroup(tx, groupDisplayName); err != nil {
			return nil, err
		}
		dbUsers := make([]*model.DBUser, 0)
		err := tx.Select(&dbUsers, `
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
		users := make([]*model.User, len(dbUsers))
		for i := range dbUsers {
			users[i] = &dbUsers[i].User
		}
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
		if err := ValidatePolicy(policy); err != nil {
			return nil, err
		}
		var id int64
		return nil, tx.Get(&id, `
			INSERT INTO auth_policies (display_name, created_at, statement)
			VALUES ($1, $2, $3)`,
			policy.DisplayName, policy.CreatedAt, policy.Statement)
	})
	return err
}

func (s *DBAuthService) GetPolicy(ctx context.Context, policyDisplayName string) (*model.Policy, error) {
	policy, err := s.db.Transact(ctx, func(tx db.Tx) (interface{}, error) {
		return getDBPolicy(tx, policyDisplayName)
	}, db.ReadOnly())
	if err != nil {
		return nil, err
	}
	return &policy.(*model.DBPolicy).Policy, nil
}

func (s *DBAuthService) DeletePolicy(ctx context.Context, policyDisplayName string) error {
	_, err := s.db.Transact(ctx, func(tx db.Tx) (interface{}, error) {
		return nil, deleteOrNotFoundDB(tx, `DELETE FROM auth_policies WHERE display_name = $1`, policyDisplayName)
	})
	return err
}

func (s *DBAuthService) ListPolicies(ctx context.Context, params *model.PaginationParams) ([]*model.Policy, *model.Paginator, error) {
	type res struct {
		policies  []*model.Policy
		paginator *model.Paginator
	}
	result, err := s.db.Transact(ctx, func(tx db.Tx) (interface{}, error) {
		dbPolicies := make([]*model.DBPolicy, 0)
		err := tx.Select(&dbPolicies, `
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
		policies := make([]*model.Policy, len(dbPolicies))
		for i := range dbPolicies {
			policies[i] = &dbPolicies[i].Policy
		}
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
	encryptedKey, err := model.EncryptSecret(s.secretStore, secretAccessKey)
	if err != nil {
		return nil, err
	}
	credentials, err := s.db.Transact(ctx, func(tx db.Tx) (interface{}, error) {
		user, err := getDBUser(tx, username)
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

		// A DB user must have an int ID
		intID := user.ID
		_, err = tx.Exec(`
			INSERT INTO auth_credentials (access_key_id, secret_access_key, issued_date, user_id)
			VALUES ($1, $2, $3, $4)`,
			c.AccessKeyID,
			encryptedKey,
			c.IssuedDate,
			intID,
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
		return nil, deleteOrNotFoundDB(tx, `
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
		if _, err := getDBGroup(tx, groupDisplayName); err != nil {
			return nil, fmt.Errorf("%s: %w", groupDisplayName, err)
		}
		if _, err := getDBPolicy(tx, policyDisplayName); err != nil {
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
		if _, err := getDBGroup(tx, groupDisplayName); err != nil {
			return nil, fmt.Errorf("%s: %w", groupDisplayName, err)
		}
		if _, err := getDBPolicy(tx, policyDisplayName); err != nil {
			return nil, fmt.Errorf("%s: %w", policyDisplayName, err)
		}
		return nil, deleteOrNotFoundDB(tx, `
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
	var (
		user *model.DBUser
		err  error
	)
	credentials, err := s.db.Transact(ctx, func(tx db.Tx) (interface{}, error) {
		user, err = getDBUser(tx, username)
		if err != nil {
			return nil, err
		}
		credentials := &model.DBCredential{}
		err = tx.Get(credentials, `
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
	return &model.Credential{
		Username:       user.Username,
		BaseCredential: credentials.(*model.DBCredential).BaseCredential,
	}, nil
}

func (s *DBAuthService) GetCredentials(ctx context.Context, accessKeyID string) (*model.Credential, error) {
	return s.cache.GetCredential(accessKeyID, func() (*model.Credential, error) {
		credentials, err := s.db.Transact(ctx, func(tx db.Tx) (interface{}, error) {
			credentials := &model.DBCredential{}
			err := tx.Get(credentials, `SELECT * FROM auth_credentials WHERE auth_credentials.access_key_id = $1`,
				accessKeyID)
			if err != nil {
				return nil, err
			}
			key, err := model.DecryptSecret(s.secretStore, credentials.SecretAccessKeyEncryptedBytes)
			if err != nil {
				return nil, err
			}
			credentials.SecretAccessKey = key
			return credentials, nil
		})
		if err != nil {
			return nil, err
		}
		user, err := s.GetUserByID(ctx, model.ConvertDBID(credentials.(*model.DBCredential).UserID))
		if err != nil {
			return nil, err
		}
		return &model.Credential{
			Username:       user.Username,
			BaseCredential: credentials.(*model.DBCredential).BaseCredential,
		}, nil
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
	return claimTokenIDOnce(ctx, tokenID, expiresAt, s.markTokenSingleUse)
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

func Migrate(ctx context.Context, d *pgxpool.Pool, writer io.Writer) error {
	je := json.NewEncoder(writer)
	// Create header
	if err := je.Encode(kv.Header{
		LakeFSVersion:   version.Version,
		PackageName:     model.PackageName,
		DBSchemaVersion: kv.InitialMigrateVersion,
		CreatedAt:       time.Now().UTC(),
	}); err != nil {
		return err
	}

	userIDToDetails, err := exportUsers(ctx, d, je)
	if err != nil {
		return err
	}

	groupIDToName, err := exportGroups(ctx, d, je)
	if err != nil {
		return err
	}

	policyIDToName, err := exportPolicies(ctx, d, je)
	if err != nil {
		return err
	}

	if err = exportUserGroups(ctx, d, je, userIDToDetails, groupIDToName); err != nil {
		return err
	}

	if err = exportUserPolicies(ctx, d, je, userIDToDetails, policyIDToName); err != nil {
		return err
	}

	if err = exportGroupPolicies(ctx, d, je, groupIDToName, policyIDToName); err != nil {
		return err
	}

	if err = exportCredentials(ctx, d, je, userIDToDetails); err != nil {
		return err
	}

	if err = exportExpiredTokens(ctx, d, je); err != nil {
		return err
	}

	if err = exportMetadata(ctx, d, je); err != nil {
		return err
	}

	return nil
}

func exportUsers(ctx context.Context, d *pgxpool.Pool, je *json.Encoder) (UserIDToName, error) {
	rows, err := d.Query(ctx, "SELECT * FROM auth_users ORDER BY created_at ASC")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	users := make(UserIDToName)
	scanner := pgxscan.NewRowScanner(rows)
	for rows.Next() {
		dbUser := model.DBUser{}
		err = scanner.Scan(&dbUser)
		if err != nil {
			return nil, err
		}
		key := model.UserPath(dbUser.Username)
		value, err := proto.Marshal(model.ProtoFromUser(&dbUser.User))
		if err != nil {
			return nil, err
		}

		if err = je.Encode(kv.Entry{
			PartitionKey: []byte(model.PartitionKey),
			Key:          key,
			Value:        value,
		}); err != nil {
			return nil, err
		}

		users[dbUser.ID] = dbUser.Username
	}
	return users, nil
}

func exportGroups(ctx context.Context, d *pgxpool.Pool, je *json.Encoder) (IDToName, error) {
	rows, err := d.Query(ctx, "SELECT * FROM auth_groups ORDER BY created_at ASC")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	groupIDToName := make(IDToName)
	scanner := pgxscan.NewRowScanner(rows)
	for rows.Next() {
		dbGroup := model.DBGroup{}
		err = scanner.Scan(&dbGroup)
		if err != nil {
			return nil, err
		}
		key := model.GroupPath(dbGroup.DisplayName)
		value, err := proto.Marshal(model.ProtoFromGroup(&dbGroup.Group))
		if err != nil {
			return nil, err
		}
		if err = je.Encode(kv.Entry{
			PartitionKey: []byte(model.PartitionKey),
			Key:          key,
			Value:        value,
		}); err != nil {
			return nil, err
		}
		groupIDToName[dbGroup.ID] = dbGroup.DisplayName
	}
	return groupIDToName, nil
}

func exportPolicies(ctx context.Context, d *pgxpool.Pool, je *json.Encoder) (IDToName, error) {
	rows, err := d.Query(ctx, "SELECT * FROM auth_policies ORDER BY created_at ASC")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	policyIDToName := make(IDToName)
	scanner := pgxscan.NewRowScanner(rows)
	for rows.Next() {
		dbPolicy := model.DBPolicy{}
		err = scanner.Scan(&dbPolicy)
		if err != nil {
			return nil, err
		}
		key := model.PolicyPath(dbPolicy.DisplayName)
		value, err := proto.Marshal(model.ProtoFromPolicy(&dbPolicy.Policy))
		if err != nil {
			return nil, err
		}
		if err = je.Encode(kv.Entry{
			PartitionKey: []byte(model.PartitionKey),
			Key:          key,
			Value:        value,
		}); err != nil {
			return nil, err
		}
		policyIDToName[dbPolicy.ID] = dbPolicy.DisplayName
	}
	return policyIDToName, nil
}

func exportUserGroups(ctx context.Context, d *pgxpool.Pool, je *json.Encoder, users UserIDToName, groupsNames IDToName) error {
	type relation struct {
		UserID  int64 `db:"user_id"`
		GroupID int   `db:"group_id"`
	}
	rows, err := d.Query(ctx, "SELECT * FROM auth_user_groups")
	if err != nil {
		return err
	}
	defer rows.Close()
	scanner := pgxscan.NewRowScanner(rows)
	for rows.Next() {
		ug := relation{}
		err = scanner.Scan(&ug)
		if err != nil {
			return err
		}
		username, ok := users[ug.UserID]
		if !ok {
			return fmt.Errorf("user ID %d: %w", ug.UserID, ErrExportedEntNotFound)
		}
		groupName, ok := groupsNames[ug.GroupID]
		if !ok {
			return fmt.Errorf("group ID %d: %w", ug.GroupID, ErrExportedEntNotFound)
		}
		key := model.GroupUserPath(groupName, username)
		secIndex := kv.SecondaryIndex{PrimaryKey: model.UserPath(username)}
		value, err := proto.Marshal(&secIndex)
		if err != nil {
			return err
		}
		if err = je.Encode(kv.Entry{
			PartitionKey: []byte(model.PartitionKey),
			Key:          key,
			Value:        value,
		}); err != nil {
			return err
		}
	}
	return nil
}

func exportUserPolicies(ctx context.Context, d *pgxpool.Pool, je *json.Encoder, users UserIDToName, policiesNames IDToName) error {
	type relation struct {
		UserID   int64 `db:"user_id"`
		PolicyID int   `db:"policy_id"`
	}
	rows, err := d.Query(ctx, "SELECT * FROM auth_user_policies")
	if err != nil {
		return err
	}
	defer rows.Close()
	scanner := pgxscan.NewRowScanner(rows)
	for rows.Next() {
		up := relation{}
		err = scanner.Scan(&up)
		if err != nil {
			return err
		}
		policyName, ok := policiesNames[up.PolicyID]
		if !ok {
			return fmt.Errorf("policy ID %d: %w", up.PolicyID, ErrExportedEntNotFound)
		}
		username, ok := users[up.UserID]
		if !ok {
			return fmt.Errorf("user ID %d: %w", up.UserID, ErrExportedEntNotFound)
		}
		key := model.UserPolicyPath(username, policyName)
		secIndex := kv.SecondaryIndex{PrimaryKey: model.PolicyPath(policyName)}
		value, err := proto.Marshal(&secIndex)
		if err != nil {
			return err
		}
		if err = je.Encode(kv.Entry{
			PartitionKey: []byte(model.PartitionKey),
			Key:          key,
			Value:        value,
		}); err != nil {
			return err
		}
	}
	return nil
}

func exportGroupPolicies(ctx context.Context, d *pgxpool.Pool, je *json.Encoder, groupsNames, policiesNames IDToName) error {
	type relation struct {
		GroupID  int `db:"group_id"`
		PolicyID int `db:"policy_id"`
	}
	rows, err := d.Query(ctx, "SELECT * FROM auth_group_policies")
	if err != nil {
		return err
	}
	defer rows.Close()
	scanner := pgxscan.NewRowScanner(rows)
	for rows.Next() {
		gp := relation{}
		err = scanner.Scan(&gp)
		if err != nil {
			return err
		}
		policyName, ok := policiesNames[gp.PolicyID]
		if !ok {
			return fmt.Errorf("policy ID %d: %w", gp.PolicyID, ErrExportedEntNotFound)
		}
		groupName, ok := groupsNames[gp.GroupID]
		if !ok {
			return fmt.Errorf("group ID %d: %w", gp.GroupID, ErrExportedEntNotFound)
		}
		key := model.GroupPolicyPath(groupName, policyName)
		secIndex := kv.SecondaryIndex{PrimaryKey: model.PolicyPath(policyName)}
		value, err := proto.Marshal(&secIndex)
		if err != nil {
			return err
		}
		if err = je.Encode(kv.Entry{
			PartitionKey: []byte(model.PartitionKey),
			Key:          key,
			Value:        value,
		}); err != nil {
			return err
		}
	}
	return nil
}

func exportCredentials(ctx context.Context, d *pgxpool.Pool, je *json.Encoder, userIDToDetails UserIDToName) error {
	rows, err := d.Query(ctx, "SELECT * from auth_credentials")
	if err != nil {
		return err
	}
	defer rows.Close()
	scanner := pgxscan.NewRowScanner(rows)
	for rows.Next() {
		dbCred := model.DBCredential{}
		err := scanner.Scan(&dbCred)
		if err != nil {
			return err
		}
		username, ok := userIDToDetails[dbCred.UserID]
		if !ok {
			return fmt.Errorf("user ID %d: %w", dbCred.UserID, ErrExportedEntNotFound)
		}
		kvCred := &model.Credential{
			Username:       username,
			BaseCredential: dbCred.BaseCredential,
		}
		key := model.CredentialPath(username, dbCred.AccessKeyID)
		value, err := proto.Marshal(model.ProtoFromCredential(kvCred))
		if err != nil {
			return err
		}
		if err = je.Encode(kv.Entry{
			PartitionKey: []byte(model.PartitionKey),
			Key:          key,
			Value:        value,
		}); err != nil {
			return err
		}
	}
	return nil
}

func exportExpiredTokens(ctx context.Context, d *pgxpool.Pool, je *json.Encoder) error {
	type tokenData struct {
		TokenID         string    `db:"token_id"`
		TokenExpiration time.Time `db:"token_expires_at"`
	}
	rows, err := d.Query(ctx, "SELECT * from auth_expired_tokens")
	if err != nil {
		return err
	}
	defer rows.Close()
	scanner := pgxscan.NewRowScanner(rows)
	for rows.Next() {
		token := tokenData{}
		err := scanner.Scan(&token)
		if err != nil {
			return err
		}
		kvToken := &model.TokenData{
			TokenId:   token.TokenID,
			ExpiredAt: timestamppb.New(token.TokenExpiration),
		}
		key := model.ExpiredTokenPath(token.TokenID)
		value, err := proto.Marshal(kvToken)
		if err != nil {
			return err
		}
		if err = je.Encode(kv.Entry{
			PartitionKey: []byte(model.PartitionKey),
			Key:          key,
			Value:        value,
		}); err != nil {
			return err
		}
	}
	return nil
}
