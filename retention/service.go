package retention

import (
	"errors"
	"fmt"
	"time"

	"github.com/treeverse/lakefs/api/gen/models"
	"github.com/treeverse/lakefs/catalog"
	"github.com/treeverse/lakefs/db"
)

const dbConfigKey = "retentionPolicy"

var ErrPolicyNotFound = errors.New("policy not found")

type DBRetentionService struct {
	db db.Database
}

func NewDBRetentionService(db db.Database) *DBRetentionService {
	return &DBRetentionService{db: db}
}

func (s *DBRetentionService) GetPolicy(repositoryName string) (*catalog.PolicyWithCreationTime, error) {
	o, err := s.db.Transact(func(tx db.Tx) (interface{}, error) {
		var policy catalog.PolicyWithCreationTime
		err := tx.GetStruct(
			&policy,
			`SELECT description, (value::json)->'Rules' as rules, created_at FROM catalog_repositories_config WHERE repository_id IN (SELECT id FROM catalog_repositories WHERE name = $1) AND key = $2`,
			repositoryName,
			dbConfigKey,
		)
		if errors.Is(err, db.ErrNotFound) {
			return nil, nil
		}
		return policy, err
	})
	if err != nil || o == nil {
		return nil, err
	}
	policy := o.(catalog.PolicyWithCreationTime)
	return &policy, nil
}

func (s *DBRetentionService) SetPolicy(repositoryName string, policy *catalog.Policy, creationDate time.Time) error {
	_, err := s.db.Transact(func(tx db.Tx) (interface{}, error) {
		return tx.Exec(
			`INSERT INTO catalog_repositories_config
                         SELECT id AS repository_id, $2 AS key, $3 AS value, $4 AS description, $5 AS created_at
                         FROM catalog_repositories WHERE name=$1
                         ON CONFLICT (repository_id, key)
                         DO UPDATE SET (value, description, created_at) = (EXCLUDED.value, EXCLUDED.description, EXCLUDED.created_at)`,
			repositoryName, dbConfigKey, &catalog.RulesHolder{Rules: policy.Rules}, policy.Description, creationDate,
		)
	})
	return err
}

type Service interface {
	GetPolicy(repositoryID string) (*models.RetentionPolicyWithCreationDate, error)
	UpdatePolicy(repositoryID string, modelPolicy *models.RetentionPolicy) error
}

type ModelService struct {
	dbService *DBRetentionService
}

func (ts *ModelService) GetPolicy(repositoryID string) (*models.RetentionPolicyWithCreationDate, error) {
	dbPolicy, err := ts.dbService.GetPolicy(repositoryID)
	if err != nil {
		return nil, err
	}
	if dbPolicy == nil {
		return nil, fmt.Errorf("%w: repository %s", ErrPolicyNotFound, repositoryID)
	}
	return RenderPolicyWithCreationDate(dbPolicy), nil
}

func (ts *ModelService) UpdatePolicy(repositoryID string, modelPolicy *models.RetentionPolicy) error {
	policy, err := ParsePolicy(*modelPolicy)
	if err != nil {
		return err
	}
	return ts.dbService.SetPolicy(repositoryID, policy, time.Now())
}

func NewService(db db.Database) *ModelService {
	return &ModelService{dbService: NewDBRetentionService(db)}
}
