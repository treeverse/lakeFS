package catalog

import (
	"context"
	"testing"

	"github.com/cloudfoundry/clock"
	"github.com/treeverse/lakefs/db"
	"github.com/treeverse/lakefs/logging"
	"github.com/treeverse/lakefs/testutil"
)

func Test_cataloger_CreateRepo(t *testing.T) {
	testClock := clock.NewClock()
	ctx := context.Background()
	log := logging.Dummy()
	cdb, _ := testutil.GetDB(t, databaseURI, "lakefs_catalog")

	type fields struct {
		Clock clock.Clock
		ctx   context.Context
		log   logging.Logger
		db    db.Database
	}
	type args struct {
		name   string
		bucket string
		branch string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		{
			name:    "basic",
			fields:  fields{Clock: testClock, ctx: ctx, log: log, db: cdb},
			args:    args{name: "repo1", bucket: "bucket1", branch: "master"},
			wantErr: false,
		},
		{
			name:    "invalid bucket",
			fields:  fields{Clock: testClock, ctx: ctx, log: log, db: cdb},
			args:    args{name: "repo2", bucket: "b", branch: "master"},
			wantErr: true,
		},
		{
			name:    "missing branch",
			fields:  fields{Clock: testClock, ctx: ctx, log: log, db: cdb},
			args:    args{name: "repo3", bucket: "bucket3", branch: ""},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &cataloger{
				Clock: tt.fields.Clock,
				ctx:   tt.fields.ctx,
				log:   tt.fields.log,
				db:    tt.fields.db,
			}
			got, err := c.CreateRepo(tt.args.name, tt.args.bucket, tt.args.branch)
			if (err != nil) != tt.wantErr {
				t.Errorf("CreateRepo() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if err == nil && got < 1 {
				t.Errorf("CreateRepo() got = %v, want valid repo id when on no error", got)
			}
		})
	}
}
