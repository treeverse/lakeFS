package catalog

import (
	"context"
	"time"

	"github.com/treeverse/lakefs/logging"

	"github.com/benbjohnson/clock"
	"github.com/treeverse/lakefs/db"
)

type MvccCataloger struct {
	Clock clock.Clock
	ctx   context.Context
	store Store
	log   logging.Logger
}

func NewMvccCataloger(db db.Database) *MvccCataloger {
	cataloger := &MvccCataloger{
		Clock: clock.New(),
		store: NewDBStore(db),
	}
	return cataloger.WithContext(context.Background())
}

func (m *MvccCataloger) WithContext(ctx context.Context) *MvccCataloger {
	log := logging.FromContext(ctx).WithField("service_name", "cataloger")
	store := NewLoggingStore(m.store, log)
	return &MvccCataloger{
		Clock: m.Clock,
		ctx:   ctx,
		store: store,
		log:   log,
	}
}

func (m *MvccCataloger) Context() context.Context {
	return m.ctx
}

func (m *MvccCataloger) CreateRepo(repoID string, bucket string, branch string) (int, error) {
	panic("implement me")
}

func (m *MvccCataloger) ListRepos(amount int, after string) ([]*Repo, bool, error) {
	panic("implement me")
}

func (m *MvccCataloger) GetRepo(repoID int) (*Repo, error) {
	panic("implement me")
}

func (m *MvccCataloger) GetRepoByName(repoName string) (*Repo, error) {
	panic("implement me")
}

func (m *MvccCataloger) DeleteRepo(repoID int) error {
	panic("implement me")
}

func (m *MvccCataloger) GetRepoCommitLog(repoID int, fromCommitID string, results int, after string) ([]*Commit, bool, error) {
	panic("implement me")
}

func (m *MvccCataloger) CreateBranch(repoID int, branch string, sourceBranchID int) (*Branch, error) {
	panic("implement me")
}

func (m *MvccCataloger) GetBranch(branchID string) (*Branch, error) {
	panic("implement me")
}

func (m *MvccCataloger) DeleteBranch(branchID int) error {
	panic("implement me")
}

func (m *MvccCataloger) GetBranchCommitLog(branchID int, fromCommitID string, results int, after string) ([]*Commit, bool, error) {
	panic("implement me")
}

func (m *MvccCataloger) ListBranchesByPrefix(repoID string, prefix string, amount int, after string) ([]*Branch, bool, error) {
	panic("implement me")
}

func (m *MvccCataloger) Commit(branchID int, message, committer string, metadata map[string]string) (*Commit, error) {
	panic("implement me")
}

func (m *MvccCataloger) ReadEntry(branchID int, path string, readOptions EntryReadOptions) (*Entry, error) {
	panic("implement me")
}

func (m *MvccCataloger) WriteEntry(branchID int, path string, entry *Entry) error {
	panic("implement me")
}

func (m *MvccCataloger) ListEntriesByPrefix(branchID int, path, after string, results int, readOptions EntryReadOptions, descend bool) ([]*Entry, bool, error) {
	panic("implement me")
}

func (m *MvccCataloger) Diff(leftBranchID, rightBranchID int) (Differences, error) {
	panic("implement me")
}

func (m *MvccCataloger) Merge(sourceBranchID, destinationBranchID int, userID string) (Differences, error) {
	panic("implement me")
}

func (m *MvccCataloger) RevertBranch(branchID int) error {
	panic("implement me")
}

func (m *MvccCataloger) RevertCommit(branchID int, commit string) error {
	panic("implement me")
}

func (m *MvccCataloger) RevertPath(branchID int, path string) error {
	panic("implement me")
}

func (m *MvccCataloger) RevertEntry(branchID int, path string) error {
	panic("implement me")
}

func (m *MvccCataloger) CreateDedupEntryIfNone(repoID int, dedupID string, physicalAddress string) (string, error) {
	panic("implement me")
}

func (m *MvccCataloger) CreateMultiPartUpload(repoID int, path, physicalAddress string, creationTime time.Time) error {
	panic("implement me")
}

func (m *MvccCataloger) ReadMultiPartUpload(repoID int, uploadID string) (*MultipartUpload, error) {
	panic("implement me")
}

func (m *MvccCataloger) DeleteMultiPartUpload(repoID int, uploadID string) error {
	panic("implement me")
}
