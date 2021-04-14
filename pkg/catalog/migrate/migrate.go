package migrate

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/hashicorp/go-multierror"
	"github.com/treeverse/lakefs/pkg/db"
	"github.com/treeverse/lakefs/pkg/graveler"
	"github.com/treeverse/lakefs/pkg/ident"
	"github.com/treeverse/lakefs/pkg/logging"
)

type Migrate struct {
	db              db.Database
	log             logging.Logger
	addressProvider ident.AddressProvider

	reporter Reporter
}

type Reporter interface {
	BeginRepository(repository string)
	BeginCommit(ref, message, committer, branch string)
	EndRepository(err error)
}

const (
	migrateTimestampKeyName = "migrate_commits_parents"
)

func NewMigrate(db db.Database, provider ident.AddressProvider) (*Migrate, error) {
	return &Migrate{
		db:              db,
		log:             logging.Default(),
		addressProvider: provider,
		reporter:        &nullReporter{},
	}, nil
}

func (m *Migrate) Run(ctx context.Context) error {
	_, err := m.db.Transact(ctx, func(tx db.Tx) (interface{}, error) {
		repos, err := m.listRepositories(tx)
		if err != nil {
			return nil, fmt.Errorf("list repositories: %w", err)
		}

		var merr error
		for step, repo := range repos {
			m.log.WithFields(logging.Fields{
				"step":              step,
				"repository":        repo.RepositoryID,
				"storage_namespace": repo.StorageNamespace,
				"default_branch":    repo.DefaultBranchID,
			}).Info("Start repository migrate")
			m.reporter.BeginRepository(repo.RepositoryID.String())

			if err := m.migrateRepository(tx, repo); err != nil {
				m.log.WithError(err).WithField("repository", repo.RepositoryID).Error("Migrate repository")
				merr = multierror.Append(merr, err)
			}
			m.reporter.EndRepository(err)
		}

		if merr != nil {
			return nil, merr
		}
		if err = m.postMigrate(tx); err != nil {
			m.log.WithError(err).Error("Post migrate")
			merr = multierror.Append(merr, err)
		}

		return nil, merr
	})
	return err
}

func (m *Migrate) migrateRepository(tx db.Tx, repository *graveler.RepositoryRecord) error {
	tags, err := m.listTags(tx, repository)
	if err != nil {
		return err
	}
	branches, err := m.listBranches(tx, repository)
	if err != nil {
		return err
	}

	if err := m.migrateCommits(tx, repository, tags, branches); err != nil {
		return fmt.Errorf("repository %s: %w", repository.RepositoryID, err)
	}
	return nil
}

// TagRecord holds TagID with the associated Tag data
type tagRecord struct {
	ID       graveler.TagID
	CommitID graveler.CommitID
}

func (m *Migrate) listTags(tx db.Tx, repository *graveler.RepositoryRecord) (map[graveler.CommitID][]graveler.TagID, error) {
	tagsFlat := make([]*tagRecord, 0)
	if err := tx.Select(&tagsFlat, `
			SELECT id, commit_id
			FROM graveler_tags
			WHERE repository_id = $1`, repository.RepositoryID); err != nil {
		return nil, err
	}

	tags := make(map[graveler.CommitID][]graveler.TagID)
	for _, tag := range tagsFlat {
		tags[tag.CommitID] = append(tags[tag.CommitID], tag.ID)
	}

	return tags, nil
}

func (m *Migrate) migrateCommit(tx db.Tx, repo *graveler.RepositoryRecord, commitR *commitRecord, tags []graveler.TagID, branches []graveler.BranchID, idsMapping map[string]string) (graveler.CommitID, error) {
	oldCommitID := commitR.CommitID
	commit := graveler.Commit{
		Committer:    commitR.Committer,
		Message:      commitR.Message,
		MetaRangeID:  graveler.MetaRangeID(commitR.MetaRangeID),
		CreationDate: commitR.CreationDate,
		Parents:      reverse(commitR.Parents, idsMapping),
		Metadata:     commitR.Metadata,
	}

	newCommitID := graveler.CommitID(m.addressProvider.ContentAddress(commit))
	if oldCommitID == newCommitID.String() {
		// no migration needed
		return newCommitID, nil
	}

	err := m.addCommit(tx, repo.RepositoryID, commit, newCommitID)
	if err != nil {
		return "", fmt.Errorf("adding new commit: %w", err)
	}

	if err := m.createTag(tx, repo.RepositoryID, graveler.TagID(oldCommitID), newCommitID); err != nil {
		return "", fmt.Errorf("creating new tag: %w", err)
	}

	for _, tag := range tags {
		if err := m.updateTag(tx, repo.RepositoryID, tag, newCommitID); err != nil {
			return "", fmt.Errorf("updating tag: %w", err)
		}
	}

	for _, branch := range branches {
		if err := m.updateBranch(tx, repo.RepositoryID, branch, newCommitID); err != nil {
			return "", fmt.Errorf("updating branch: %w", err)
		}
	}

	return newCommitID, nil
}

func reverse(parents []string, idsMapping map[string]string) graveler.CommitParents {
	rev := make(graveler.CommitParents, len(parents))
	for i, j := 0, len(parents)-1; i <= j; i, j = i+1, j-1 {
		rev[i], rev[j] = graveler.CommitID(idsMapping[parents[j]]), graveler.CommitID(idsMapping[parents[i]])
	}
	return rev
}

func (m *Migrate) migrateCommits(tx db.Tx, repo *graveler.RepositoryRecord, tags map[graveler.CommitID][]graveler.TagID, branches map[graveler.CommitID][]graveler.BranchID) error {
	commits, err := m.listCommitsInOrder(tx, repo)
	if err != nil {
		return err
	}

	newCommitIDMapping := map[string]string{}
	for _, commit := range commits {
		oldID := graveler.CommitID(commit.CommitID)
		newCommitID, err := m.migrateCommit(tx, repo, commit, tags[oldID], branches[oldID], newCommitIDMapping)
		if err != nil {
			return fmt.Errorf("migrating commit: %w", err)
		}
		newCommitIDMapping[string(oldID)] = string(newCommitID)
	}

	return nil
}

type commitRecord struct {
	CommitID     string            `db:"id"`
	Committer    string            `db:"committer"`
	Message      string            `db:"message"`
	MetaRangeID  string            `db:"meta_range_id"`
	CreationDate time.Time         `db:"creation_date"`
	Parents      []string          `db:"parents"`
	Metadata     map[string]string `db:"metadata"`
}

func (m *Migrate) listCommitsInOrder(tx db.Tx, repo *graveler.RepositoryRecord) ([]*commitRecord, error) {
	commits, err := m.listCommits(tx, repo)
	if err != nil {
		return nil, fmt.Errorf("list commits in order: %w", err)
	}

	firstCommit, revMap := m.buildReverseParentMap(commits)
	commitsMap := m.mapCommits(commits)

	queue := []string{firstCommit}
	orderedCommits := []string{}
	seen := map[string]bool{}
	for len(queue) > 0 {
		// pop
		curr := queue[0]
		queue = queue[1:]

		// add to result
		orderedCommits = append(orderedCommits, curr)
		seen[curr] = true

		children := revMap[curr]
		for _, child := range children {
			// for each child of the current commit, it should be
			// added to the queue only if all its parents were seen
			seenAllParents := true
			parents := commitsMap[child].Parents
			for _, p := range parents {
				if !seen[p] {
					seenAllParents = false
					break
				}
			}
			if seenAllParents {
				queue = append(queue, child)
			}
		}
	}

	orderedCommitsRecords := make([]*commitRecord, 0, len(orderedCommits))
	for _, commit := range orderedCommits {
		orderedCommitsRecords = append(orderedCommitsRecords, commitsMap[commit])
	}

	return orderedCommitsRecords, nil
}

func (m *Migrate) mapCommits(commits []*commitRecord) map[string]*commitRecord {
	commitsMap := map[string]*commitRecord{}
	for _, commit := range commits {
		commitsMap[commit.CommitID] = commit
	}
	return commitsMap
}

func (m *Migrate) buildReverseParentMap(commits []*commitRecord) (string, map[string][]string) {
	firstCommit := ""
	revMap := map[string][]string{}
	for _, commit := range commits {
		if len(commit.Parents) == 0 {
			if firstCommit != "" {
				panic("more than a single first commit")
			}
			firstCommit = commit.CommitID
			continue
		}

		for _, p := range commit.Parents {
			revMap[p] = append(revMap[p], commit.CommitID)
		}
	}
	if firstCommit == "" {
		panic("first commit not found")
	}
	return firstCommit, revMap
}

func (m *Migrate) listCommits(tx db.Tx, repo *graveler.RepositoryRecord) ([]*commitRecord, error) {
	var commits []*commitRecord
	err := tx.Select(&commits, `
			SELECT id, committer, message, creation_date, meta_range_id, parents, metadata
			FROM graveler_commits
			WHERE repository_id = $1
			ORDER BY id ASC`, repo.RepositoryID)
	if err != nil {
		return nil, fmt.Errorf("listing commits: %w", err)
	}

	return commits, nil
}

func (m *Migrate) postMigrate(tx db.Tx) error {
	m.log.Info("update db migrate timestamp")
	migrateTimestamp := time.Now().UTC().Format(time.RFC3339)
	_, err := tx.Exec(`INSERT INTO auth_installation_metadata (key_name, key_value)
			VALUES ($1,$2)
			ON CONFLICT (key_name) DO UPDATE SET key_value=EXCLUDED.key_value`,
		migrateTimestampKeyName, migrateTimestamp)
	return err
}

func (m *Migrate) SetReporter(r Reporter) {
	m.reporter = r
}

// BranchRecord holds BranchID with the associated Branch data
type branchRecord struct {
	BranchID graveler.BranchID `db:"id"`
	*graveler.Branch
}

func (m *Migrate) listBranches(tx db.Tx, repository *graveler.RepositoryRecord) (map[graveler.CommitID][]graveler.BranchID, error) {
	branchesFlat := make([]*branchRecord, 0)
	if err := tx.Select(&branchesFlat, `
			SELECT id, staging_token, commit_id
			FROM graveler_branches
			WHERE repository_id = $1`, repository.RepositoryID); err != nil {
		return nil, err
	}

	branches := make(map[graveler.CommitID][]graveler.BranchID)
	for _, branch := range branchesFlat {
		branches[branch.CommitID] = append(branches[branch.CommitID], branch.BranchID)
	}

	return branches, nil
}

func (m *Migrate) listRepositories(tx db.Tx) ([]*graveler.RepositoryRecord, error) {
	repos := make([]*graveler.RepositoryRecord, 0)
	if err := tx.Select(&repos, `
			SELECT id, storage_namespace, creation_date, default_branch
			FROM graveler_repositories`); err != nil {
		return nil, err
	}
	return repos, nil
}

func (m *Migrate) addCommit(tx db.Tx, repoID graveler.RepositoryID, commit graveler.Commit, commitID graveler.CommitID) error {
	var parents []string
	for _, parent := range commit.Parents {
		parents = append(parents, string(parent))
	}

	_, err := tx.Exec(`
				INSERT INTO graveler_commits 
				(repository_id, id, committer, message, creation_date, parents, meta_range_id, metadata)
				VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
				ON CONFLICT DO NOTHING`,
		repoID, commitID.String(), commit.Committer, commit.Message,
		commit.CreationDate.UTC(), parents, commit.MetaRangeID, commit.Metadata)
	if err != nil {
		return err
	}

	return nil
}

func (m *Migrate) createTag(tx db.Tx, repoID graveler.RepositoryID, tagID graveler.TagID, commitID graveler.CommitID) error {
	_, err := tx.Exec(`INSERT INTO graveler_tags (repository_id, id, commit_id) VALUES ($1, $2, $3)
			ON CONFLICT DO NOTHING`,
		repoID, tagID, commitID)
	return err
}

func (m *Migrate) updateTag(tx db.Tx, repoID graveler.RepositoryID, tagID graveler.TagID, commitID graveler.CommitID) error {
	_, err := tx.Exec(`INSERT INTO graveler_tags (repository_id, id, commit_id) VALUES ($1, $2, $3)
				ON CONFLICT (repository_id, id)
				DO UPDATE SET commit_id = $3`,
		repoID, tagID, commitID)
	return err
}

func (m *Migrate) updateBranch(tx db.Tx, repoID graveler.RepositoryID, branchID graveler.BranchID, commitID graveler.CommitID) error {
	_, err := tx.Exec(`
			UPDATE graveler_branches 
			SET commit_id = $3 
			WHERE repository_id = $1 AND id = $2`,
		repoID, branchID, commitID)

	return err
}

type nullReporter struct{}

func (n *nullReporter) BeginRepository(string) {}

func (n *nullReporter) BeginCommit(string, string, string, string) {}

func (n *nullReporter) EndRepository(error) {}

func CheckMigrationRequired(ctx context.Context, conn db.Database) bool {
	// check if we already run migration
	res, err := conn.Transact(ctx, func(tx db.Tx) (interface{}, error) {
		var migrationRun bool
		err := tx.GetPrimitive(&migrationRun, `SELECT EXISTS(SELECT 1 FROM auth_installation_metadata WHERE key_name=$1)`, migrateTimestampKeyName)
		if err != nil && !errors.Is(err, db.ErrNotFound) {
			return false, err
		}
		return migrationRun, nil
	}, db.ReadOnly())
	if err != nil {
		return false
	}
	return !res.(bool)
}
