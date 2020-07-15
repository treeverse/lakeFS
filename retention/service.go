package retention

import (
	"time"

	"github.com/treeverse/lakefs/api/gen/models"
	"github.com/treeverse/lakefs/db"
)

const dbConfigKey = "retentionPolicy"

type DBRetentionService struct {
	db db.Database
}

func NewDBRetentionService(db db.Database) *DBRetentionService {
	return &DBRetentionService{db: db}
}

func (s *DBRetentionService) GetPolicy(repositoryName string) (*PolicyWithCreationTime, error) {
	o, err := s.db.Transact(func(tx db.Tx) (interface{}, error) {
		var policy PolicyWithCreationTime
		err := tx.Get(
			&policy,
			`SELECT description, (value::json)->'Rules' as rules, created_at FROM repositories_config WHERE repository_id IN (SELECT repository_id FROM repositories WHERE name = $1) AND key = $2`,
			repositoryName,
			dbConfigKey,
		)
		return policy, err
	})
	if err != nil {
		return nil, err
	}
	policy := o.(PolicyWithCreationTime)
	return &policy, nil
}

func (s *DBRetentionService) SetPolicy(repositoryName string, policy *Policy, creationDate time.Time) error {
	_, err := s.db.Transact(func(tx db.Tx) (interface{}, error) {
		return tx.Exec(
			`INSERT INTO repositories_config
                         SELECT id AS repository_id, $2 AS key, $3 AS value, $4 AS description, $5 AS created_at
                         FROM repositories WHERE name=$1
                         ON CONFLICT (repository_id, key)
                         DO UPDATE SET (value, description, created_at) = (EXCLUDED.value, EXCLUDED.description, EXCLUDED.created_at)`,
			repositoryName, dbConfigKey, &RulesHolder{Rules: policy.Rules}, policy.Description, creationDate,
		)
	})
	return err
}

type Service interface {
	GetPolicy(repositoryId string) (*models.RetentionPolicyWithCreationDate, error)
	UpdatePolicy(repositoryId string, modelPolicy *models.RetentionPolicy) error
}

type ModelService struct {
	dbService *DBRetentionService
}

func (ts *ModelService) GetPolicy(repositoryId string) (*models.RetentionPolicyWithCreationDate, error) {
	dbPolicy, err := ts.dbService.GetPolicy(repositoryId)
	if err != nil {
		return nil, err
	}
	return RenderPolicyWithCreationDate(dbPolicy), nil
}

func (ts *ModelService) UpdatePolicy(repositoryId string, modelPolicy *models.RetentionPolicy) error {
	policy, err := ParsePolicy(*modelPolicy)
	if err != nil {
		return err
	}
	return ts.dbService.SetPolicy(repositoryId, policy, time.Now())
}

func NewService(db db.Database) *ModelService {
	return &ModelService{dbService: NewDBRetentionService(db)}
}
