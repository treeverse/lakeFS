package migrate

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"time"

	"github.com/hashicorp/go-multierror"
	"github.com/jackc/pgx/v4"
	"github.com/treeverse/lakefs/catalog"
	"github.com/treeverse/lakefs/catalog/mvcc"
	"github.com/treeverse/lakefs/catalog/rocks"
	"github.com/treeverse/lakefs/db"
	"github.com/treeverse/lakefs/graveler"
	"github.com/treeverse/lakefs/logging"
)

type Migrate struct {
	db            db.Database
	mvccCataloger catalog.Cataloger
	entryCatalog  *rocks.EntryCatalog
	repositoryRe  *regexp.Regexp
	log           logging.Logger

	// per repository state
	lastCommit graveler.CommitID
	branches   map[graveler.BranchID]graveler.CommitID
	tags       map[graveler.TagID]graveler.CommitID
}

type commitRecord struct {
	BranchID              int64
	BranchName            string
	CommitID              int64
	PreviousCommitID      int64
	Committer             string
	Message               string
	CreationDate          time.Time
	Metadata              catalog.Metadata
	MergeType             string
	MergeSourceBranch     *int64
	MergeSourceBranchName string
	MergeSourceCommit     *int64
}

const (
	migrateFetchSize = 1000

	initialCommitMessage = "Create empty new branch for migrate"
)

func (m *Migrate) Close() error {
	return m.mvccCataloger.Close()
}

func NewMigrate(db db.Database, entryCatalog *rocks.EntryCatalog, mvccCataloger catalog.Cataloger) (*Migrate, error) {
	return &Migrate{
		db:            db,
		entryCatalog:  entryCatalog,
		mvccCataloger: mvccCataloger,
		log:           logging.Default(),
	}, nil
}

func (m *Migrate) FilterRepository(expr string) error {
	if expr == "" {
		m.repositoryRe = nil
		return nil
	}
	repositoryRe, err := regexp.Compile(expr)
	if err != nil {
		return err
	}
	m.repositoryRe = repositoryRe
	return nil
}

func (m *Migrate) Run() error {
	ctx := context.Background()
	repos, err := m.listRepositories(ctx)
	if err != nil {
		return fmt.Errorf("list repositories: %w", err)
	}

	m.log.WithField("repositories_len", len(repos)).Info("Start migrate")
	var merr error
	for i, repo := range repos {
		m.log.WithFields(logging.Fields{
			"step":              i + 1,
			"total":             len(repos),
			"repository":        repo.Name,
			"storage_namespace": repo.StorageNamespace,
			"default_branch":    repo.DefaultBranch,
		}).Info("Start repository migrate")
		_, err := m.migrateRepository(ctx, repo)
		if err != nil {
			m.log.WithError(err).WithField("repository", repo.Name).Error("Migrate repository")
			merr = multierror.Append(merr, err)
		}
	}

	if err = m.postMigrate(); err != nil {
		m.log.WithError(err).Error("Post migrate")
		merr = multierror.Append(merr, err)
	}
	return merr
}

func (m *Migrate) listRepositories(ctx context.Context) ([]*catalog.Repository, error) {
	var repos []*catalog.Repository
	var after string
	for {
		reposPage, hasMore, err := m.mvccCataloger.ListRepositories(ctx, 100, after)
		if err != nil {
			return nil, err
		}
		for _, repo := range reposPage {
			if m.repositoryRe == nil || m.repositoryRe.MatchString(repo.Name) {
				repos = append(repos, repo)
			}
		}
		if !hasMore || len(reposPage) == 0 {
			break
		}
		after = reposPage[len(reposPage)-1].Name
	}
	return repos, nil
}

func (m *Migrate) migrateRepository(ctx context.Context, repository *catalog.Repository) (*graveler.RepositoryRecord, error) {
	// initialize per repository state
	m.branches = make(map[graveler.BranchID]graveler.CommitID)
	m.tags = make(map[graveler.TagID]graveler.CommitID)
	m.lastCommit = ""

	// get or create repository
	repo, err := m.getOrCreateTargetRepository(ctx, repository)
	if err != nil {
		return nil, err
	}

	// scan mvcc repository commits by order and reproduce on entry catalog
	err = m.migrateCommits(ctx, repo)
	if err != nil {
		return nil, fmt.Errorf("repository %s: %w", repo.RepositoryID, err)
	}
	return repo, nil
}

func (m *Migrate) getOrCreateTargetRepository(ctx context.Context, repository *catalog.Repository) (*graveler.RepositoryRecord, error) {
	repoID := graveler.RepositoryID(repository.Name)
	branchID := graveler.BranchID(repository.DefaultBranch)

	// check and create repository if needed
	repo, err := m.entryCatalog.GetRepository(ctx, repoID)
	if errors.Is(err, graveler.ErrRepositoryNotFound) {
		repo, err = m.entryCatalog.CreateRepository(ctx, repoID, graveler.StorageNamespace(repository.StorageNamespace), branchID)
		if err != nil {
			return nil, fmt.Errorf("create repository: %w", err)
		}
	}
	if err != nil {
		return nil, err
	}

	// check default branch includes at least one commit - compatibility issue
	branch, err := m.entryCatalog.GetBranch(ctx, repoID, branchID)
	if err != nil {
		return nil, err
	}
	m.lastCommit = branch.CommitID
	if m.lastCommit == "" {
		metaRangeID, err := m.entryCatalog.WriteMetaRange(ctx, repoID, newEmptyIterator())
		if err != nil {
			return nil, err
		}

		commit := graveler.Commit{
			Committer:    catalog.DefaultCommitter,
			Message:      initialCommitMessage,
			MetaRangeID:  *metaRangeID,
			CreationDate: repository.CreationDate.Add(-time.Second), // make sure the commit will be first
			Parents:      make(graveler.CommitParents, 0),
			Metadata:     make(graveler.Metadata),
		}
		commitID, err := m.entryCatalog.AddCommitNoLock(ctx, repoID, commit)
		if err != nil {
			return nil, fmt.Errorf("initial commit (%s): %w", repoID, err)
		}
		m.lastCommit = commitID
	}
	m.branches[branchID] = m.lastCommit

	return &graveler.RepositoryRecord{
		RepositoryID: graveler.RepositoryID(repository.Name),
		Repository:   repo,
	}, nil
}

// migrateCommit migrate single commit from MVCC based repository to EntryCatalog format
func (m *Migrate) migrateCommit(ctx context.Context, repo *graveler.RepositoryRecord, commit commitRecord) error {
	mvccRef := mvcc.MakeReference(commit.BranchName, mvcc.CommitID(commit.CommitID))

	// lookup tag, skip commit if we already tagged this commit
	tagID := graveler.TagID(mvccRef)
	tagCommitID, err := m.entryCatalog.GetTag(ctx, repo.RepositoryID, tagID)
	if err != nil && !errors.Is(err, graveler.ErrTagNotFound) {
		return fmt.Errorf("tag lookup %s: %w", tagID, err)
	}
	branchID := graveler.BranchID(commit.BranchName)
	if tagCommitID != nil {
		m.lastCommit = *tagCommitID
		m.branches[branchID] = m.lastCommit
		m.tags[tagID] = m.lastCommit
		return nil
	}

	// check and create target branch if needed
	if _, ok := m.branches[branchID]; !ok {
		_, err := m.entryCatalog.CreateBranch(ctx, repo.RepositoryID, branchID, graveler.Ref(m.lastCommit))
		if err != nil && !errors.Is(err, graveler.ErrBranchExists) {
			return fmt.Errorf("create branch %s: %w", branchID, err)
		}
		m.branches[branchID] = m.lastCommit
	}

	// iterator for mvcc entries, write meta range
	it := NewIterator(ctx, m.db, commit.BranchID, commit.CommitID, migrateFetchSize)
	// scan entries
	metaRangeID, err := m.entryCatalog.WriteMetaRange(ctx, repo.RepositoryID, it)
	if err != nil {
		return fmt.Errorf("write meta range: %w", err)
	}

	// commit new meta range
	newCommit := graveler.Commit{
		Committer:    commit.Committer,
		Message:      commit.Message,
		MetaRangeID:  *metaRangeID,
		CreationDate: commit.CreationDate,
		Parents:      make(graveler.CommitParents, 0),
		Metadata:     graveler.Metadata(commit.Metadata),
	}

	// first parent - lookup based on previous commit
	parentCommitID := m.lastCommit
	if commit.PreviousCommitID > 0 {
		mvccParentRef := mvcc.MakeReference(commit.BranchName, mvcc.CommitID(commit.PreviousCommitID))
		parentTagID := graveler.TagID(mvccParentRef)
		if commitID, ok := m.tags[parentTagID]; ok {
			parentCommitID = commitID
		}
	}
	newCommit.Parents = append(newCommit.Parents, parentCommitID)

	// second parent - commit merge
	if commit.MergeSourceCommit != nil && commit.MergeSourceBranch != nil {
		secondParentRef := mvcc.MakeReference(commit.MergeSourceBranchName, mvcc.CommitID(*commit.MergeSourceCommit))
		secondParentTag := graveler.TagID(secondParentRef)
		if commitID, ok := m.tags[secondParentTag]; ok && commitID != parentCommitID {
			newCommit.Parents = append(newCommit.Parents, commitID)
		}
	}

	commitID, err := m.entryCatalog.AddCommitNoLock(ctx, repo.RepositoryID, newCommit)
	if err != nil {
		return fmt.Errorf("commit existing meta range (commit %d): %w", commit.CommitID, err)
	}

	// tag the commit id (backward compatibility and a way to skip commit in multiple runs)
	err = m.entryCatalog.CreateTag(ctx, repo.RepositoryID, tagID, commitID)
	if err != nil {
		return fmt.Errorf("create tag %s (branch %s, commit %d): %w", mvccRef, branchID, commit.CommitID, err)
	}

	m.lastCommit = commitID
	m.branches[branchID] = commitID
	m.tags[tagID] = commitID
	return nil
}

func (m *Migrate) migrateCommits(ctx context.Context, repo *graveler.RepositoryRecord) error {
	// select all repository commits and import
	rows, err := m.selectRepoCommits(ctx, repo)
	if err != nil {
		return fmt.Errorf("select commits: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var commit commitRecord
		err := commit.Scan(rows)
		if err != nil {
			return fmt.Errorf("scan commit record: %w", err)
		}
		m.log.WithFields(logging.Fields{
			"repository":    string(repo.RepositoryID),
			"commit_id":     commit.CommitID,
			"branch_name":   commit.BranchName,
			"committer":     commit.Committer,
			"creation_date": commit.CreationDate,
		}).Info("Commit migrate")
		if err := m.migrateCommit(ctx, repo, commit); err != nil {
			return fmt.Errorf("import commit %d: %w", commit.CommitID, err)
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("read commits: %w", err)
	}

	// branches - update latest commit
	for branchID, commitID := range m.branches {
		_, err := m.entryCatalog.UpdateBranch(ctx, repo.RepositoryID, branchID, commitID.Ref())
		if err != nil {
			return fmt.Errorf("update branch %s ref %s: %w", branchID, commitID, err)
		}
	}
	return nil
}

func (m *Migrate) selectRepoCommits(ctx context.Context, repo *graveler.RepositoryRecord) (pgx.Rows, error) {
	var mvccRepoID int64
	err := m.db.GetPrimitive(&mvccRepoID, `SELECT id FROM catalog_repositories WHERE name = $1`, repo.RepositoryID)
	if err != nil {
		return nil, fmt.Errorf("select mvcc repository id (%s): %w", repo.RepositoryID, err)
	}
	rows, err := m.db.WithContext(ctx).Query(`SELECT c.branch_id, b1.name as branch_name, c.commit_id, 
			c.previous_commit_id, c.committer, c.message, c.creation_date, c.metadata,
			c.merge_source_branch, COALESCE(b2.name,'') AS merge_source_branch_name, c.merge_source_commit, c.merge_type
		FROM catalog_commits c
		LEFT JOIN catalog_branches b1 ON b1.id=c.branch_id
		LEFT JOIN catalog_branches b2 ON b2.id=c.merge_source_branch
		WHERE b1.repository_id = $1
		ORDER BY commit_id`,
		mvccRepoID)
	if err != nil {
		return nil, fmt.Errorf("select commits (%s): %w", repo.RepositoryID, err)
	}
	return rows, nil
}

func (m *Migrate) postMigrate() error {
	m.log.Info("start analyze db")
	_, err := m.db.Exec(`
		analyze graveler_staging_kv;
		analyze graveler_commits;
		analyze graveler_tags;
		analyze graveler_branches;`)
	return err
}

func (c *commitRecord) Scan(rows pgx.Row) error {
	return rows.Scan(&c.BranchID, &c.BranchName, &c.CommitID,
		&c.PreviousCommitID, &c.Committer, &c.Message, &c.CreationDate, &c.Metadata,
		&c.MergeSourceBranch, &c.MergeSourceBranchName, &c.MergeSourceCommit, &c.MergeType)
}
