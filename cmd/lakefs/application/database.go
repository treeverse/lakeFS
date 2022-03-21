package application

import (
	"errors"
	"github.com/dlmiddlecote/sqlstats"
	"github.com/golang-migrate/migrate/v4"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/treeverse/lakefs/pkg/catalog"
	"github.com/treeverse/lakefs/pkg/db"
	"github.com/treeverse/lakefs/pkg/db/params"
	"github.com/treeverse/lakefs/pkg/gateway/multiparts"
)

type DatabaseService struct {
	dbPool     db.Database
	dbParams   params.Database
	lockDBPool db.Database
	migrator   *db.DatabaseMigrator
}

func (databaseService DatabaseService) Close() {
	databaseService.dbPool.Close()
	databaseService.lockDBPool.Close()
}

func (databaseService DatabaseService) RegisterPrometheusCollector() error {
	collector := sqlstats.NewStatsCollector("lakefs", databaseService.dbPool)
	return prometheus.Register(collector)
}

func (databaseService DatabaseService) NewCatalog(cmd LakeFsCmd) (*catalog.Catalog, error) {
	return catalog.New(cmd.ctx,
		catalog.Config{
			Config: cmd.cfg,
			DB:     databaseService.dbPool,
			LockDB: databaseService.lockDBPool,
		},
	)
}

func (databaseService DatabaseService) NewMultipartTracker() multiparts.Tracker {
	return multiparts.NewTracker(databaseService.dbPool)
}

func NewDatabaseService(cmd LakeFsCmd) (*DatabaseService, error) {
	dbParams := cmd.cfg.GetDatabaseParams()
	dbPool := db.BuildDatabaseConnection(cmd.ctx, dbParams)
	err := db.ValidateSchemaUpToDate(cmd.ctx, dbPool, dbParams)
	switch {
	case errors.Is(err, db.ErrSchemaNotCompatible):
		cmd.logger.WithError(err).Fatal("Migration version mismatch, for more information see https://docs.lakefs.io/deploying-aws/upgrade.html")
		return nil, err
	case errors.Is(err, migrate.ErrNilVersion):
		cmd.logger.Debug("No migration, setup required")
		return nil, err
	case err != nil:
		cmd.logger.WithError(err).Warn("Failed on schema validation")
		return nil, err
	}
	lockDBPool := db.BuildDatabaseConnection(cmd.ctx, dbParams)
	migrator := db.NewDatabaseMigrator(dbParams)
	return &DatabaseService{
		dbPool,
		dbParams,
		lockDBPool,
		migrator,
	}, nil
}
