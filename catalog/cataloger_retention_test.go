package catalog

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/go-test/deep"
	"github.com/jmoiron/sqlx"
	"github.com/treeverse/lakefs/db"
	"github.com/treeverse/lakefs/testutil"
)

func makeHours(hours int) *TimePeriodHours {
	ret := TimePeriodHours(hours)
	return &ret
}

func readEntriesToExpire(t *testing.T, ctx context.Context, c Cataloger, repository string, policy *Policy) ([]*ExpireResult, error) {
	rows, err := c.QueryEntriesToExpire(ctx, repository, policy)
	if err != nil {
		t.Fatalf("scan for expired failed: %s", err)
	}
	defer func() {
		if err := rows.Close(); err != nil {
			t.Fatalf("close rows from expire result %s", err)
		}
	}()
	ret := make([]*ExpireResult, 0, 10)
	for rows.Next() {
		e, err := rows.Read()
		if err != nil {
			t.Fatalf("read expired row: %s", err)
		}
		ret = append(ret, e)
	}
	return ret, nil
}

// less compares two ExpireResults, to help sort with sort.Slice.
func less(a, b *ExpireResult) bool {
	if a.Repository < b.Repository {
		return true
	}
	if a.Repository > b.Repository {
		return false
	}
	if a.Branch < b.Branch {
		return true
	}
	if a.Branch > b.Branch {
		return false
	}
	if a.InternalReference < b.InternalReference {
		return true
	}
	if a.InternalReference > b.InternalReference {
		return false
	}
	return a.PhysicalAddress < b.PhysicalAddress
}

// sortExpireResults sorts a slice of ExpireResults
func sortExpireResults(results []*ExpireResult) {
	sort.Slice(results, func(i, j int) bool { return less(results[i], results[j]) })
}

type expiryTestCase struct {
	name   string
	policy *Policy
	want   []*ExpireResult
}

func verifyExpiry(t *testing.T, ctx context.Context, c Cataloger, repository string, tests []expiryTestCase) {
	conn, _ := testutil.GetDB(t, databaseURI)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := readEntriesToExpire(t, ctx, c, repository, tt.policy)

			if err != nil {
				t.Fatalf("scan for expired failed: %s", err)
			}

			sortExpireResults(tt.want)
			sortExpireResults(got)

			if diffs := deep.Equal(tt.want, got); diffs != nil {
				t.Errorf("did not expire as expected, diffs %s", diffs)
				t.Errorf("expected %+v, got %+v", tt.want, got)
			}
		})
		// Reset "deleting" fields that were set.
		if _, err := conn.Exec("UPDATE catalog_object_dedup SET deleting=false"); err != nil {
			t.Fatalf("Failed to wipe deleting markers from catalog_object_dedup: %s", err)
		}
	}
}

func TestCataloger_ScanExpired(t *testing.T) {
	ctx := context.Background()
	c := testCataloger(t)
	defer func() { _ = c.Close() }()

	repository := testCatalogerRepo(t, ctx, c, "repository", "master")
	testCatalogerBranch(t, ctx, c, repository, "slow", "master")
	testCatalogerBranch(t, ctx, c, repository, "fast", "slow")

	if err := c.CreateEntry(ctx, repository, "master", Entry{
		Path:            "0/historical",
		PhysicalAddress: "/master/history/1",
		CreationDate:    time.Now().Add(-20 * time.Hour),
		Checksum:        "1",
	}, CreateEntryParams{}); err != nil {
		t.Fatal("Failed to create 0/historical on master", err)
	}
	if err := c.CreateEntry(ctx, repository, "master", Entry{
		Path:            "0/committed",
		PhysicalAddress: "/committed/1",
		CreationDate:    time.Now().Add(-19 * time.Hour),
		Checksum:        "2",
	}, CreateEntryParams{}); err != nil {
		t.Fatal("Failed to create 0/committed on master", err)
	}

	if _, err := c.Commit(ctx, repository, "master", "first commit", "tester", Metadata{}); err != nil {
		t.Fatal("Failed to commit first commit to master", err)
	}

	if err := c.CreateEntry(ctx, repository, "master", Entry{
		Path:            "0/historical",
		PhysicalAddress: "/master/history/2",
		CreationDate:    time.Now().Add(-19 * time.Hour),
		Checksum:        "2",
	}, CreateEntryParams{}); err != nil {
		t.Fatal("Failed to update 0/historical on master", err)
	}
	if _, err := c.Commit(ctx, repository, "master", "second commit", "tester", Metadata{}); err != nil {
		t.Fatal("Failed to commit second commit to master", err)
	}

	if err := c.CreateEntry(ctx, repository, "slow", Entry{
		Path:            "0/committed",
		PhysicalAddress: "/committed/2",
		CreationDate:    time.Now().Add(-15 * time.Hour),
		Checksum:        "3",
	}, CreateEntryParams{}); err != nil {
		t.Fatal("Failed to create 0/committed on slow", err)
	}

	if _, err := c.Commit(ctx, repository, "slow", "first slow commit", "tester", Metadata{}); err != nil {
		t.Fatal("Failed to commit to slow", err)
	}

	if err := c.CreateEntry(ctx, repository, "fast", Entry{
		Path:            "0/historical",
		PhysicalAddress: "/history/2",
		CreationDate:    time.Now().Add(-15 * time.Hour),
		Checksum:        "3",
	}, CreateEntryParams{}); err != nil {
		t.Fatal("Failed to update 0/historical on fast", err)
	}

	if _, err := c.Commit(ctx, repository, "fast", "first fast commit", "tester", Metadata{}); err != nil {
		t.Fatal("Failed to commit first fast commit", err)
	}
	if err := c.CreateEntry(ctx, repository, "fast", Entry{
		Path:            "0/historical",
		PhysicalAddress: "/history/3",
		CreationDate:    time.Now().Add(-5 * time.Hour),
		Checksum:        "4",
	}, CreateEntryParams{}); err != nil {
		t.Fatal("Failed to update 0/historical again on fast", err)
	}

	if _, err := c.Commit(ctx, repository, "fast", "second fast commit", "tester", Metadata{}); err != nil {
		t.Fatal("Failed to commit second fast commit", err)
	}

	if err := c.CreateEntry(ctx, repository, "master", Entry{
		Path:            "0/historical",
		PhysicalAddress: "/history/4",
		CreationDate:    time.Now().Add(-2 * time.Hour),
		Checksum:        "5",
	}, CreateEntryParams{}); err != nil {
		t.Fatal("Failed to update 0/historical on master", err)
	}

	// Get all expire results; we shall pick-and-choose from them for more specific tests.
	// Hard to forge expire results because of their package-specific fields, most notably
	// minCommit.
	allResults, err := readEntriesToExpire(t, ctx, c, repository, &Policy{
		Rules: []Rule{
			{Enabled: true, FilterPrefix: "", Expiration: Expiration{All: makeHours(0)}},
		},
	})
	if err != nil {
		t.Fatalf("read all expiration records failed: %s", err)
	}
	resultByPhysicalAddress := make(map[string]*ExpireResult, len(allResults))
	for _, result := range allResults {
		t.Logf("Result: %+v", result)
		resultByPhysicalAddress[result.PhysicalAddress] = result
	}
	translate := func(physicalAddress string) *ExpireResult {
		ret, ok := resultByPhysicalAddress[physicalAddress]
		if !ok {
			t.Fatalf("no ExpireResult found for expected physical path %s", physicalAddress)
		}
		return ret
	}
	masterHistorical20Hours := translate("/master/history/1")
	masterCommitted19Hours := translate("/committed/1")
	masterHistorical19Hours := translate("/master/history/2")
	slowCommitted15Hours := translate("/committed/2")
	fastCommitted15Hours := translate("/history/2")
	fastCommitted5Hours := translate("/history/3")
	masterUncommitted2Hours := translate("/history/4")

	tests := []expiryTestCase{
		{
			name: "expire nothing",
			policy: &Policy{
				Rules: []Rule{
					{
						Enabled:      true,
						FilterPrefix: "",
						Expiration:   Expiration{All: makeHours(50)},
					},
				},
			},
			want: []*ExpireResult{},
		}, {
			// (Calls the same readExpired and doesn't test much, except that the
			// right number of results were returned!)
			name: "expire all",
			policy: &Policy{
				Rules: []Rule{
					{
						Enabled:      true,
						FilterPrefix: "",
						Expiration:   Expiration{All: makeHours(0)},
					},
				},
			},
			want: []*ExpireResult{
				masterHistorical20Hours,
				masterHistorical19Hours,
				masterCommitted19Hours,
				slowCommitted15Hours,
				fastCommitted15Hours,
				fastCommitted5Hours,
				masterUncommitted2Hours,
			},
		}, {
			name: "expire uncommitted",
			policy: &Policy{
				Rules: []Rule{
					{
						Enabled:      true,
						FilterPrefix: "",
						Expiration:   Expiration{Uncommitted: makeHours(0)},
					},
				},
			},
			want: []*ExpireResult{masterUncommitted2Hours},
		}, {
			name: "expire all noncurrent",
			policy: &Policy{
				Rules: []Rule{
					{
						Enabled:      true,
						FilterPrefix: "",
						Expiration:   Expiration{Noncurrent: makeHours(0)},
					},
				},
			},
			want: []*ExpireResult{
				masterHistorical20Hours,
				fastCommitted15Hours,
			},
		}, {
			name: "expire old noncurrent",
			policy: &Policy{
				Rules: []Rule{
					{
						Enabled:      true,
						FilterPrefix: "",
						Expiration:   Expiration{Noncurrent: makeHours(18)},
					},
				},
			},
			want: []*ExpireResult{
				masterHistorical20Hours,
			},
		}, {
			name: "expire uncommitted and old noncurrent",
			policy: &Policy{
				Rules: []Rule{
					{
						Enabled:      true,
						FilterPrefix: "",
						Expiration: Expiration{
							Noncurrent:  makeHours(18),
							Uncommitted: makeHours(0),
						},
					},
				},
			},
			want: []*ExpireResult{
				masterHistorical20Hours,
				masterUncommitted2Hours,
			},
		}, {
			name: "expire by branch",
			policy: &Policy{
				Rules: []Rule{
					{
						Enabled:      true,
						FilterPrefix: "master/",
						Expiration: Expiration{
							All: makeHours(0),
						},
					},
				},
			},
			want: []*ExpireResult{
				masterHistorical20Hours,
				masterHistorical19Hours,
				masterCommitted19Hours,
				masterUncommitted2Hours,
			},
		}, {
			name: "expire noncurrent by branch",
			policy: &Policy{
				Rules: []Rule{
					{
						Enabled:      true,
						FilterPrefix: "master/",
						Expiration: Expiration{
							Noncurrent: makeHours(0),
						},
					},
				},
			},
			want: []*ExpireResult{
				masterHistorical20Hours,
			},
		}, {
			name: "expire by branch and path prefix",
			policy: &Policy{
				Rules: []Rule{
					{
						Enabled:      true,
						FilterPrefix: "master/0/",
						Expiration: Expiration{
							All: makeHours(0),
						},
					},
				},
			},
			want: []*ExpireResult{
				masterHistorical20Hours,
				masterHistorical19Hours,
				masterCommitted19Hours,
				masterUncommitted2Hours,
			},
		}, {
			name: "ignore disabled rules",
			policy: &Policy{
				Rules: []Rule{
					{
						Enabled:      false,
						FilterPrefix: "",
						Expiration: Expiration{
							All: makeHours(0),
						},
					},
				},
			},
			want: []*ExpireResult{},
		},
	}

	verifyExpiry(t, ctx, c, repository, tests)
}

func TestCataloger_ScanExpiredWithDupes(t *testing.T) {
	ctx := context.Background()
	c := testCataloger(t)
	defer func() { _ = c.Close() }()

	repository := testCatalogerRepo(t, ctx, c, "repository", "master")
	testCatalogerBranch(t, ctx, c, repository, "branch", "master")

	if err := c.CreateEntry(ctx, repository, "master", Entry{
		Path:            "0/historical",
		PhysicalAddress: "/master/one/file",
		CreationDate:    time.Now().Add(-20 * time.Hour),
		Checksum:        "1",
	}, CreateEntryParams{}); err != nil {
		t.Fatal("Failed to create 0/historical on master", err)
	}
	if err := c.CreateEntry(ctx, repository, "master", Entry{
		Path:            "0/different",
		PhysicalAddress: "/master/all/different",
		CreationDate:    time.Now().Add(-19 * time.Hour),
		Checksum:        "2",
	}, CreateEntryParams{}); err != nil {
		t.Fatal("Failed to create 0/committed on master", err)
	}
	if _, err := c.Commit(ctx, repository, "master", "first commit", "tester", Metadata{}); err != nil {
		t.Fatal("Failed to commit first commit to master", err)
	}

	if err := c.CreateEntry(ctx, repository, "branch", Entry{
		Path:            "0/different",
		PhysicalAddress: "/master/one/file",
		CreationDate:    time.Now().Add(-5 * time.Hour),
		Checksum:        "1",
	}, CreateEntryParams{}); err != nil {
		t.Fatal("Failed to create 0/different on branch", err)
	}

	// Get all expire results; we shall pick-and-choose from them for more specific tests.
	// Hard to forge expire results because of their package-specific fields, most notably
	// minCommit.

	allResults, err := readEntriesToExpire(t, ctx, c, repository, &Policy{
		Rules: []Rule{
			{Enabled: true, FilterPrefix: "", Expiration: Expiration{All: makeHours(0)}},
		},
	})
	if err != nil {
		t.Fatalf("read all expiration records failed: %s", err)
	}

	type branchAndPhysicalAddress struct {
		branch, physicalAddress string
	}
	resultByBranchAndPhysicalAddress := make(map[branchAndPhysicalAddress]*ExpireResult, len(allResults))
	for _, result := range allResults {
		t.Logf("Result: %+v", result)
		resultByBranchAndPhysicalAddress[branchAndPhysicalAddress{result.Branch, result.PhysicalAddress}] = result
	}
	translate := func(branch, physicalAddress string) *ExpireResult {
		ret, ok := resultByBranchAndPhysicalAddress[branchAndPhysicalAddress{branch, physicalAddress}]
		if !ok {
			t.Fatalf("no ExpireResult found for expected branch %s physical path %s", branch, physicalAddress)
		}
		return ret
	}

	masterHistorical := translate("master", "/master/one/file")
	masterDifferent := translate("master", "/master/all/different")
	branchDifferent := translate("branch", "/master/one/file") // Points at same object as masterHistorical

	tests := []expiryTestCase{
		{
			name: "only branch",
			policy: &Policy{
				Rules: []Rule{
					{
						Enabled:      true,
						FilterPrefix: "branch/",
						Expiration:   Expiration{All: makeHours(0)},
					},
				},
			},
			want: []*ExpireResult{},
		}, {
			name: "only master",
			policy: &Policy{
				Rules: []Rule{
					{
						Enabled:      true,
						FilterPrefix: "master/",
						Expiration:   Expiration{All: makeHours(0)},
					},
				},
			},
			want: []*ExpireResult{masterDifferent},
		}, {
			name: "expire all",
			policy: &Policy{
				Rules: []Rule{
					{
						Enabled:      true,
						FilterPrefix: "",
						Expiration:   Expiration{All: makeHours(0)},
					},
				},
			},
			want: []*ExpireResult{masterHistorical, branchDifferent, masterDifferent},
		},
	}
	verifyExpiry(t, ctx, c, repository, tests)
}

// TODO(ariels): benchmark
func TestCataloger_MarkEntriesExpired(t *testing.T) {
	const (
		numBatches = 30
		batchSize  = 100
	)
	ctx := context.Background()
	c := testCataloger(t)
	defer func() { _ = c.Close() }()
	repository := testCatalogerRepo(t, ctx, c, "repository", "master")

	for batch := 0; batch < numBatches; batch++ {
		for i := 0; i < batchSize; i++ {
			if err := c.CreateEntry(ctx, repository, "master", Entry{
				Path:            fmt.Sprintf("/path/%08d/%08d", i, batch),
				PhysicalAddress: fmt.Sprintf("/phys/%09d", batch*batchSize+i),
				Checksum:        fmt.Sprintf("%08x", i),
			}, CreateEntryParams{}); err != nil {
				t.Fatalf("failed to create entry batch %d #%d: %s", batch, i, err)
			}
		}
	}
	if _, err := c.Commit(ctx, repository, "master", "commit ALL the files to expire", "tester", Metadata{}); err != nil {
		t.Fatalf("failed to commit: %s", err)
	}

	expireResults, err := readEntriesToExpire(t, ctx, c, repository, &Policy{
		Rules: []Rule{
			{Enabled: true, FilterPrefix: "", Expiration: Expiration{All: makeHours(0)}},
		},
	})
	if err != nil {
		t.Fatalf("read all expiration records failed: %s", err)
	}

	err = c.MarkEntriesExpired(ctx, repository, expireResults)
	if err != nil {
		t.Fatalf("mark expiration records failed: %s", err)
	}

	for _, e := range expireResults {
		ref, err := ParseInternalObjectRef(e.InternalReference)
		if err != nil {
			t.Fatalf("couldn't parse returned internal object ref in %+v", e)
		}
		_, err = c.GetEntry(ctx, repository, "master", ref.Path, GetEntryParams{})
		if err == nil || !errors.Is(err, ErrExpired) {
			t.Errorf("didn't get expired entry %+v: %s", e, err)
		}

		entry, err := c.GetEntry(ctx, repository, "master", ref.Path, GetEntryParams{ReturnExpired: true})
		if err != nil || !entry.Expired {
			t.Errorf("expected expired entry when requesting expired return for %+v, got %+v", e, entry)
		}
	}
}

func getDeleting(t *testing.T, rows *sqlx.Rows) map[string]bool {
	t.Helper()
	deleting := make(map[string]bool, 2)
	for rows.Next() {
		var (
			physicalAddress string
			d               bool
		)
		if err := rows.Scan(&physicalAddress, &d); err != nil {
			t.Fatalf("failed to scan row %v: %v", rows, err)
		}
		deleting[physicalAddress] = d
	}
	return deleting
}

// TODO(ariels): benchmark
func TestCataloger_MarkObjectsForDeletion(t *testing.T) {
	ctx := context.Background()
	c := testCataloger(t)
	defer func() { _ = c.Close() }()
	repository := testCatalogerRepo(t, ctx, c, "repository", "master")

	type entryParams struct {
		entry  Entry
		params CreateEntryParams
	}

	makeDedup := func(id int) CreateEntryParams {
		return CreateEntryParams{
			Dedup: DedupParams{
				ID:               fmt.Sprintf("%x", id),
				StorageNamespace: "foo",
			},
		}
	}

	entryDedups := []entryParams{
		{
			entry: Entry{
				Path:            "all/expired/1",
				PhysicalAddress: "delete-me",
				Checksum:        "aa",
				Expired:         true,
			},
			params: makeDedup(111),
		}, {
			entry: Entry{
				Path:            "all/expired/2",
				PhysicalAddress: "delete-me:x",
				Checksum:        "aa",
				Expired:         true,
			},
			params: makeDedup(111),
		}, {
			entry: Entry{
				Path:            "some/expired/1",
				PhysicalAddress: "dont-delete-me",
				Checksum:        "bb",
				Expired:         true,
			},
			params: makeDedup(222),
		}, {
			entry: Entry{
				Path:            "some/expired/2",
				PhysicalAddress: "dont-delete-me:x",
				Checksum:        "bb",
				Expired:         false,
			},
			params: makeDedup(222),
		},
	}
	for _, ep := range entryDedups {
		if err := c.CreateEntry(ctx, repository, "master", ep.entry, ep.params); err != nil {
			t.Fatalf("failed to set up entry %v: %s", ep, err)
		}
	}
	// Expecting one dedup report for each physical address
	for i := 0; i < 2; i++ {
		select {
		case r := <-c.DedupReportChannel():
			t.Logf("Dedup report for %+v: ID %v NewPhysicalAddress %v", r.Entry, r.DedupID, r.NewPhysicalAddress)
		case <-time.After(3 * time.Second):
			t.Fatalf("timeout waiting for dedup report %v", i)
		}
	}

	count, err := c.MarkObjectsForDeletion(ctx, repository)
	t.Logf("%v objects marked for deletion", count)
	if err != nil {
		t.Errorf("failed to mark objects for deletion: %s", err)
	}
	if count != 1 {
		t.Errorf("expected 1 object marked for deletion, got %v", count)
	}

	conn, err := db.ConnectDB("pgx", c.DbConnURI)
	if err != nil {
		t.Fatalf("failed to connect to DB on %s", c.DbConnURI)
	}

	rows, err := conn.Queryx("SELECT physical_address, deleting FROM catalog_object_dedup")
	if err != nil {
		t.Errorf("failed to query catalog_object_dedup: %s", err)
	}

	deleting := getDeleting(t, rows)
	d, ok := deleting["dont-delete-me"]
	if !ok {
		t.Errorf("[internal] couldn't find dont-delete-me key in %v", deleting)
	}
	if d {
		t.Error("expected not to delete dont-delete-me but it was")
	}
	d, ok = deleting["delete-me"]
	if !ok {
		t.Errorf("[internal] couldn't find delete-me key in %v", deleting)
	}
	if !d {
		t.Error("expected to delete delete-me but it was not")
	}
}

// TODO(ariels): benchmark
func TestCataloger_DeleteOrUnmarkObjectsForDeletion(t *testing.T) {
	ctx := context.Background()
	c := testCataloger(t)
	defer func() { _ = c.Close() }()
	repository := testCatalogerRepo(t, ctx, c, "repository", "master")

	type entryParams struct {
		entry  Entry
		params CreateEntryParams
	}

	makeDedup := func(id int) CreateEntryParams {
		return CreateEntryParams{
			Dedup: DedupParams{
				// IDs must be an *even* number of digits.
				ID:               fmt.Sprintf("%08x", id),
				StorageNamespace: "foo",
			},
		}
	}

	entryDedups := []entryParams{
		{
			entry: Entry{
				Path:            "all/expired/1",
				PhysicalAddress: "delete-me",
				Checksum:        "aa",
				Expired:         true,
			},
			params: makeDedup(111),
		}, {
			entry: Entry{
				Path:            "all/expired/2",
				PhysicalAddress: "delete-me:x",
				Checksum:        "aa",
				Expired:         true,
			},
			params: makeDedup(111),
		}, {
			entry: Entry{
				Path:            "some/expired/1",
				PhysicalAddress: "dont-delete-me",
				Checksum:        "bb",
				Expired:         true,
			},
			params: makeDedup(222),
		}, {
			entry: Entry{
				Path:            "some/expired/2",
				PhysicalAddress: "dont-delete-me:x",
				Checksum:        "bb",
				Expired:         false,
			},
			params: makeDedup(222),
		}, {
			entry: Entry{
				Path:            "none/expired/1",
				PhysicalAddress: "dont-delete-me-either",
				Checksum:        "cc",
				Expired:         false,
			},
			params: makeDedup(333),
		},
	}
	for _, ep := range entryDedups {
		if err := c.CreateEntry(ctx, repository, "master", ep.entry, ep.params); err != nil {
			t.Fatalf("failed to set up entry %v: %s", ep, err)
		}
	}
	// Expecting one dedup report for each physical address
	for i := 0; i < 2; i++ {
		select {
		case r := <-c.DedupReportChannel():
			t.Logf("Dedup report for %+v: ID %v NewPhysicalAddress %v", r.Entry, r.DedupID, r.NewPhysicalAddress)
		case <-time.After(3 * time.Second):
			t.Fatalf("timeout waiting for dedup report %v", i)
		}
	}

	conn, err := db.ConnectDB("pgx", c.DbConnURI)
	if err != nil {
		t.Fatalf("failed to connect to DB on %s", c.DbConnURI)
	}

	numRows, err := conn.Exec("UPDATE catalog_object_dedup SET deleting=true WHERE physical_address IN ('delete-me', 'dont-delete-me')")
	if numRows != 2 || err != nil {
		t.Fatalf("[interna] failed to set 2 objects to state deleting: %v objects set, %v", numRows, err)
	}

	deleteRows, err := c.DeleteOrUnmarkObjectsForDeletion(ctx, repository)
	if err != nil {
		t.Fatalf("failed to DeleteOrUnmarkObjectsForDeletion: %s", err)
	}
	delete := make([]string, 0, 2)
	for deleteRows.Next() {
		deleteRow, err := deleteRows.Read()
		if err != nil {
			t.Fatalf("failed to read row from %+v: %s", deleteRows, err)
		}
		delete = append(delete, deleteRow)
	}
	sort.Strings(delete)
	expectedDelete := []string{"delete-me"}
	if diffs := deep.Equal(expectedDelete, delete); diffs != nil {
		t.Errorf("expected to delete other objects: %s\nexpected %v got %v", diffs, expectedDelete, delete)
	}

	rows, err := conn.Queryx("SELECT physical_address, deleting FROM catalog_object_dedup")
	if err != nil {
		t.Fatalf("failed to query catalog_object_dedup: %s", err)
	}
	deleting := getDeleting(t, rows)
	expectedDeleting := map[string]bool{
		"delete-me":             true,
		"dont-delete-me":        false,
		"dont-delete-me-either": false,
	}
	if diffs := deep.Equal(expectedDeleting, deleting); diffs != nil {
		t.Errorf("expected other deleting: %s\nexpected %v got %v", diffs, expectedDeleting, deleting)
	}
}
