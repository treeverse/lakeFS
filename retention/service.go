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

func (s *DBRetentionService) GetPolicy(repositoryName string) (*Policy, error) {
	o, err := s.db.Transact(func(tx db.Tx) (interface{}, error) {
		policy := Policy{}
		err := tx.Get(
			&policy,
			`SELECT value FROM repositories_config WHERE repository_id IN (SELECT repository_id FROM repositories WHERE name = $1) AND key = $2`,
			repositoryName,
			dbConfigKey,
		)
		if err != nil {
			return nil, err
		}
		return policy, nil
	})
	if err != nil {
		return nil, err
	}
	policy := o.(Policy)
	return &policy, nil
}

func (s *DBRetentionService) SetPolicy(repositoryName string, policy *Policy, comment string, creationDate time.Time) error {
	_, err := s.db.Transact(func(tx db.Tx) (interface{}, error) {
		return tx.Exec(
			`INSERT INTO repositories_config
                         SELECT id AS repository_id, $2 AS key, $3 AS value, $4 AS comment, $5 AS creation_date
                         FROM repositories WHERE name=$1
                         ON CONFLICT (repository_id, key)
                         DO UPDATE SET (value, comment, creation_date) = (EXCLUDED.value, EXCLUDED.comment, EXCLUDED.creation_date)`,
			repositoryName, dbConfigKey, policy, comment, creationDate,
		)
	})
	return err
}

type Service interface {
	GetPolicy(repositoryId string) (*models.RetentionPolicy, error)
	UpdatePolicy(repositoryId string, modelPolicy *models.RetentionPolicy) error
}
