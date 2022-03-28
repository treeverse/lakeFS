package application

import (
	"context"
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

func (databaseService DatabaseService) NewCatalog(cmd LakeFsCmdContext) (*catalog.Catalog, error) {
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

func NewDatabaseService(lakeFsCmdCtx LakeFsCmdContext) *DatabaseService {
	dbParams := lakeFsCmdCtx.cfg.GetDatabaseParams()
	dbPool := db.BuildDatabaseConnection(lakeFsCmdCtx.ctx, dbParams)

	lockDBPool := db.BuildDatabaseConnection(lakeFsCmdCtx.ctx, dbParams)
	migrator := db.NewDatabaseMigrator(dbParams)
	return &DatabaseService{
		dbPool,
		dbParams,
		lockDBPool,
		migrator,
	}
}

func (databaseService DatabaseService) ValidateSchemaIsUpToDate(lakeFsCmdCtx LakeFsCmdContext) error {
	err := db.ValidateSchemaUpToDate(lakeFsCmdCtx.ctx, databaseService.dbPool, databaseService.dbParams)
	switch {
	case errors.Is(err, db.ErrSchemaNotCompatible):
		lakeFsCmdCtx.logger.WithError(err).Fatal("Migration version mismatch, for more information see https://docs.lakefs.io/deploying-aws/upgrade.html")
		return err
	case errors.Is(err, migrate.ErrNilVersion):
		lakeFsCmdCtx.logger.Debug("No migration, setup required")
		return err
	case err != nil:
		lakeFsCmdCtx.logger.WithError(err).Warn("Failed on schema validation")
		return err
	}
	return nil
}

// TODO: maybe better name? GetMigrateVersion?
func (databaseService DatabaseService) MigrateVersion(ctx context.Context) (uint, bool, error) {
	return db.MigrateVersion(ctx, databaseService.dbPool, databaseService.dbParams)
}

func (databaseService DatabaseService) Migrate(ctx context.Context) error {
	return databaseService.migrator.Migrate(ctx)
}
