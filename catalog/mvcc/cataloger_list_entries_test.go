package mvcc

import (
	"context"
	"crypto/sha256"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"testing"

	"github.com/davecgh/go-spew/spew"
	"github.com/go-test/deep"
	"github.com/treeverse/lakefs/catalog"
	"github.com/treeverse/lakefs/testutil"
)

func TestCataloger_ListEntries(t *testing.T) {
	ctx := context.Background()
	c := testCataloger(t)

	// produce test data
	_, err := c.CreateRepository(ctx, "repo1", "s3://bucket1", "master")
	testutil.MustDo(t, "create test repo", err)
	for i := 0; i < 5; i++ {
		n := i + 1
		filePath := fmt.Sprintf("/file%d", n)
		fileChecksum := fmt.Sprintf("%x", sha256.Sum256([]byte(filePath)))
		fileAddress := fmt.Sprintf("/addr%d", n)
		fileSize := int64(n) * 10
		testutil.MustDo(t, "create test entry",
			c.CreateEntry(ctx, "repo1", "master", catalog.Entry{
				Path:            filePath,
				Checksum:        fileChecksum,
				PhysicalAddress: fileAddress,
				Size:            fileSize,
				Metadata:        nil,
			}, catalog.CreateEntryParams{}))
		if i == 2 {
			_, err := c.Commit(ctx, "repo1", "master", "commit test files", "tester", nil)
			testutil.MustDo(t, "commit test files", err)
		}
	}
	testutil.MustDo(t, "delete the first committed file",
		c.DeleteEntry(ctx, "repo1", "master", "/file1"))

	type args struct {
		repository string
		reference  string
		path       string
		after      string
		limit      int
	}
	tests := []struct {
		name        string
		args        args
		wantEntries []catalog.Entry
		wantMore    bool
		wantErr     bool
	}{
		{
			name: "all uncommitted",
			args: args{
				repository: "repo1",
				reference:  "master",
				path:       "",
				after:      "",
				limit:      -1,
			},
			wantEntries: []catalog.Entry{
				{Path: "/file2", PhysicalAddress: "/addr2", Size: 20, Checksum: "a23eaeb64fff1004b1ef460294035633055bb49bc7b99bedc1493aab73d03f63"},
				{Path: "/file3", PhysicalAddress: "/addr3", Size: 30, Checksum: "fdfe3b8d45740319c989f33eaea4e3acbd3d7e01e0484d8e888d95bcc83d43f3"},
				{Path: "/file4", PhysicalAddress: "/addr4", Size: 40, Checksum: "49f014abae232570cc48072bac6b70531bba7e883ea04b448c6cbeed1446e6ff"},
				{Path: "/file5", PhysicalAddress: "/addr5", Size: 50, Checksum: "53c9486452c01e26833296dcf1f701379fa22f01e610dd9817d064093daab07d"},
			},
			wantMore: false,
			wantErr:  false,
		},
		{
			name: "first 2 uncommitted",
			args: args{
				repository: "repo1",
				reference:  "master",
				path:       "",
				after:      "",
				limit:      2,
			},
			wantEntries: []catalog.Entry{
				{Path: "/file2", PhysicalAddress: "/addr2", Size: 20, Checksum: "a23eaeb64fff1004b1ef460294035633055bb49bc7b99bedc1493aab73d03f63"},
				{Path: "/file3", PhysicalAddress: "/addr3", Size: 30, Checksum: "fdfe3b8d45740319c989f33eaea4e3acbd3d7e01e0484d8e888d95bcc83d43f3"},
			},
			wantMore: true,
			wantErr:  false,
		},
		{
			name: "last 2",
			args: args{
				repository: "repo1",
				reference:  "master",
				path:       "",
				after:      "/file3",
				limit:      2,
			},
			wantEntries: []catalog.Entry{
				{Path: "/file4", PhysicalAddress: "/addr4", Size: 40, Checksum: "49f014abae232570cc48072bac6b70531bba7e883ea04b448c6cbeed1446e6ff"},
				{Path: "/file5", PhysicalAddress: "/addr5", Size: 50, Checksum: "53c9486452c01e26833296dcf1f701379fa22f01e610dd9817d064093daab07d"},
			},
			wantMore: false,
			wantErr:  false,
		},
		{
			name: "committed",
			args: args{
				repository: "repo1",
				reference:  "master:HEAD",
				path:       "",
				after:      "/file1",
				limit:      -1,
			},
			wantEntries: []catalog.Entry{
				{Path: "/file2", PhysicalAddress: "/addr2", Size: 20, Checksum: "a23eaeb64fff1004b1ef460294035633055bb49bc7b99bedc1493aab73d03f63"},
				{Path: "/file3", PhysicalAddress: "/addr3", Size: 30, Checksum: "fdfe3b8d45740319c989f33eaea4e3acbd3d7e01e0484d8e888d95bcc83d43f3"},
			},
			wantMore: false,
			wantErr:  false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, gotMore, err := c.ListEntries(ctx, tt.args.repository, tt.args.reference, tt.args.path, tt.args.after, "", tt.args.limit)
			if (err != nil) != tt.wantErr {
				t.Fatalf("List() error = %v, wantErr %v", err, tt.wantErr)
			}
			// copy the Value's fields we like to compare
			var gotEntries []catalog.Entry
			for i, ent := range got {
				if ent == nil {
					t.Fatalf("Expected entry at index %d, found nil", i)
				}
				gotEntries = append(gotEntries, catalog.Entry{
					CommonLevel:     ent.CommonLevel,
					Path:            ent.Path,
					PhysicalAddress: ent.PhysicalAddress,
					Size:            ent.Size,
					Checksum:        ent.Checksum,
				})
			}

			if !reflect.DeepEqual(gotEntries, tt.wantEntries) {
				t.Errorf("List() got = %s, want = %s", spew.Sdump(gotEntries), spew.Sdump(tt.wantEntries))
			}
			if gotMore != tt.wantMore {
				t.Errorf("List() gotMore = %v, want = %v", gotMore, tt.wantMore)
			}
		})
	}
}

func TestCataloger_ListEntries_ByLevel(t *testing.T) {
	ctx := context.Background()
	c := testCataloger(t)

	// produce test data
	repo := testCatalogerRepo(t, ctx, c, "repo", "master")
	suffixList := []string{"rip0", "file1", "file2", "file2/xxx", "file3/", "file4", "file5", "file6/yyy", "file6/zzz/zzz", "file6/ccc", "file7", "file8", "file9", "filea",
		"/fileb", "//filec", "///filed", "rip"}
	for i, suffix := range suffixList {
		n := i + 1
		filePath := suffix
		fileChecksum := fmt.Sprintf("%x", sha256.Sum256([]byte(filePath)))
		fileAddress := fmt.Sprintf("/addr%d", n)
		fileSize := int64(n) * 10
		testutil.MustDo(t, "create test entry",
			c.CreateEntry(ctx, repo, "master", catalog.Entry{
				Path:            filePath,
				Checksum:        fileChecksum,
				PhysicalAddress: fileAddress,
				Size:            fileSize,
				Metadata:        nil,
			}, catalog.CreateEntryParams{}))

		if i == 3 {
			_, err := c.Commit(ctx, repo, "master", "commit test files", "tester", nil)
			testutil.MustDo(t, "commit test files", err)
		}
	}
	// delete one file
	testutil.MustDo(t, "delete rip0 file",
		c.DeleteEntry(ctx, repo, "master", "rip0"))
	testutil.MustDo(t, "delete rip file",
		c.DeleteEntry(ctx, repo, "master", "rip"))

	type args struct {
		repository string
		reference  string
		path       string
		after      string
		limit      int
	}
	tests := []struct {
		name        string
		args        args
		wantEntries []string
		wantMore    bool
		wantErr     bool
	}{
		{
			name: "all uncommitted",
			args: args{
				repository: repo,
				reference:  "master",
				path:       "",
				after:      "",
				limit:      100,
			},
			wantEntries: []string{"/", "file1", "file2", "file2/", "file3/", "file4", "file5", "file6/", "file7", "file8", "file9", "filea"},
			wantMore:    false,
			wantErr:     false,
		},
		{
			name: "first 2 uncommitted",
			args: args{
				repository: repo,
				reference:  "master",
				path:       "",
				after:      "",
				limit:      2,
			},
			wantEntries: []string{"/", "file1"},
			wantMore:    true,
			wantErr:     false,
		},
		{
			name: "2 after file3",
			args: args{
				repository: repo,
				reference:  "master",
				path:       "",
				after:      "file3",
				limit:      2,
			},
			wantEntries: []string{"file3/", "file4"},
			wantMore:    true,
			wantErr:     false,
		}, {
			name: "2 after file2/",
			args: args{
				repository: repo,
				reference:  "master",
				path:       "",
				after:      "file2/",
				limit:      2,
			},
			wantEntries: []string{"file3/", "file4"},
			wantMore:    true,
			wantErr:     false,
		},
		{
			name: "committed",
			args: args{
				repository: repo,
				reference:  "master:HEAD",
				path:       "",
				after:      "file1",
				limit:      100,
			},
			wantEntries: []string{"file2", "file2/", "rip0"},
			wantMore:    false,
			wantErr:     false,
		},
		{
			name: "slash",
			args: args{
				repository: repo,
				reference:  "master",
				path:       "/",
				after:      "",
				limit:      100,
			},
			wantEntries: []string{"//", "/fileb"},
			wantMore:    false,
			wantErr:     false,
		},
		{
			name: "double slash",
			args: args{
				repository: repo,
				reference:  "master",
				path:       "//",
				after:      "",
				limit:      100,
			},
			wantEntries: []string{"///", "//filec"},
			wantMore:    false,
			wantErr:     false,
		},
		{
			name: "under file6",
			args: args{
				repository: repo,
				reference:  "master",
				path:       "file6/",
				after:      "",
				limit:      100,
			},
			wantEntries: []string{"file6/ccc", "file6/yyy", "file6/zzz/"},
			wantMore:    false,
			wantErr:     false,
		},
		{
			name: "under file6 after ccc",
			args: args{
				repository: repo,
				reference:  "master",
				path:       "file6/",
				after:      "file6/ccc",
				limit:      100,
			},
			wantEntries: []string{"file6/yyy", "file6/zzz/"},
			wantMore:    false,
			wantErr:     false,
		},
		{
			name: "specific file",
			args: args{
				repository: repo,
				reference:  "master",
				path:       "file6/zzz/zzz",
				after:      "",
				limit:      100,
			},
			wantEntries: []string{"file6/zzz/zzz"},
			wantMore:    false,
			wantErr:     false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, gotMore, err := c.ListEntries(ctx, tt.args.repository, tt.args.reference, tt.args.path, tt.args.after, catalog.DefaultPathDelimiter, tt.args.limit)
			if (err != nil) != tt.wantErr {
				t.Fatalf("List() err = %s, expected error %t", err, tt.wantErr)
			}
			// test that directories have null entries, and vice versa
			var gotNames []string
			for _, res := range got {
				if strings.HasSuffix(res.Path, catalog.DefaultPathDelimiter) != res.CommonLevel {
					t.Errorf("%s suffix doesn't match the CommonLevel = %t", res.Path, res.CommonLevel)
				}
				if !strings.HasSuffix(res.Path, res.Path) {
					t.Errorf("Name '%s' expected to be path '%s' suffix", res.Path, res.Path)
				}
				gotNames = append(gotNames, res.Path)
			}

			if !reflect.DeepEqual(gotNames, tt.wantEntries) {
				t.Errorf("List got = %s, want = %s", spew.Sdump(gotNames), spew.Sdump(tt.wantEntries))
			}
			if gotMore != tt.wantMore {
				t.Errorf("List gotMore = %t, want = %t", gotMore, tt.wantMore)
			}
		})
	}
}

func TestCataloger_ListEntries_ByLevel_Deleted(t *testing.T) {
	ctx := context.Background()
	c := testCataloger(t)

	repo := testCatalogerRepo(t, ctx, c, "repo", "master")
	testCatalogerCreateEntry(t, ctx, c, repo, "master", "place/to/go.1", nil, "")
	testCatalogerCreateEntry(t, ctx, c, repo, "master", "place/to/go.2", nil, "")
	_, err := c.Commit(ctx, repo, "master", "commit two files", "tester", nil)

	testutil.MustDo(t, "commit 2 files", err)
	testutil.MustDo(t, "delete file 1",
		c.DeleteEntry(ctx, repo, "master", "place/to/go.1"))
	testutil.MustDo(t, "delete file 2",
		c.DeleteEntry(ctx, repo, "master", "place/to/go.2"))
	testCatalogerCreateEntry(t, ctx, c, repo, "master", "place/to/go.0", nil, "")
	testCatalogerCreateEntry(t, ctx, c, repo, "master", "place/to/go.3", nil, "")
	_, err = c.Commit(ctx, repo, "master", "commit two more files", "tester", nil)
	testutil.MustDo(t, "commit 2 files", err)

	entries, hasMore, err := c.ListEntries(ctx, repo, "master", "place/to/", "", catalog.DefaultPathDelimiter, -1)
	testutil.MustDo(t, "list entries under place/to/", err)
	if len(entries) != 2 {
		t.Errorf("Expected two entries, got = %s", spew.Sdump(entries))
	}
	if hasMore {
		t.Errorf("List() hasMore = %t, expected fasle", hasMore)
	}
}

func TestCataloger_ListByLevel_Delete(t *testing.T) {
	ctx := context.Background()
	c := testCataloger(t)

	repo := testCatalogerRepo(t, ctx, c, "repo", "master")
	for i := 0; i < 150; i++ {
		testCatalogerCreateEntry(t, ctx, c, repo, "master", "xxx/entry"+pathExt(i), nil, strconv.Itoa(i*10000))
	}
	_, err := c.Commit(ctx, repo, "master", "message", "committer1", nil)
	testutil.MustDo(t, "commit to master", err)
	testCatalogerBranch(t, ctx, c, repo, "br_1", "master")

	testCatalogerBranch(t, ctx, c, repo, "br_2", "br_1")
	for i := 0; i < 100; i++ {
		testCatalogerCreateEntry(t, ctx, c, repo, "br_2", "xxx/entry"+pathExt(i), nil, strconv.Itoa(i*10000))
	}
	for i := 0; i < 100; i++ {
		testCatalogerCreateEntry(t, ctx, c, repo, "br_1", "xxx/entry"+pathExt(i), nil, strconv.Itoa(i*10000))
	}

	_, err = c.Commit(ctx, repo, "br_1", "message", "committer1", nil)
	testutil.MustDo(t, "commit to br_1", err)

	for i := 0; i < 100; i++ {
		testutil.MustDo(t, "delete entry "+"xxx/entry"+pathExt(i),
			c.DeleteEntry(ctx, repo, "br_2", "xxx/entry"+pathExt(i)))
	}

	got, gotMore, err := c.ListEntries(ctx, repo, "br_2", "", "", "/", 20)
	testCatalogerListEntriesVerifyResponse(t, got, gotMore, err, []string{"xxx/"})
}

func TestCataloger_ListByLevel_DirectoriesAndTombstones(t *testing.T) {
	ctx := context.Background()
	c := testCataloger(t)

	repo := testCatalogerRepo(t, ctx, c, "repo", "master")
	for i := 0; i < 10; i++ {
		for j := 0; j < 10; j++ {
			testCatalogerCreateEntry(t, ctx, c, repo, "master", "xxx"+pathExt(i)+"/entry"+pathExt(j), nil, strconv.Itoa(i*10000))
		}
	}
	_, err := c.Commit(ctx, repo, "master", "message", "committer1", nil)
	testutil.MustDo(t, "commit to master", err)
	testCatalogerBranch(t, ctx, c, repo, "br_1", "master")
	for i := 0; i < 10; i += 2 {
		for j := 0; j < 10; j += 2 {
			testCatalogerCreateEntry(t, ctx, c, repo, "br_1", "xxx"+pathExt(i)+"/entry"+pathExt(j), nil, strconv.Itoa(i*10000))
		}
	}
	_, err = c.Commit(ctx, repo, "br_1", "message", "committer1", nil)
	testutil.MustDo(t, "commit to br_1", err)
	testCatalogerBranch(t, ctx, c, repo, "br_2", "br_1")
	for i := 0; i < 10; i += 3 {
		for j := 0; j < 10; j++ {
			testCatalogerCreateEntry(t, ctx, c, repo, "br_2", "xxx"+pathExt(i)+"/entry"+pathExt(j), nil, strconv.Itoa(i*10000))
		}
	}
	_, err = c.Commit(ctx, repo, "br_2", "message", "committer1", nil)
	testutil.MustDo(t, "commit to br_2", err)
	for i := 0; i < 10; i += 3 {
		for j := 0; j < 10; j++ {
			testutil.MustDo(t, "delete entry "+"xxx"+pathExt(i)+"/entry"+pathExt(j),
				c.DeleteEntry(ctx, repo, "br_2", "xxx"+pathExt(i)+"/entry"+pathExt(j)))
		}
	}
	wantEntries := []string{"xxx001/", "xxx002/", "xxx004/", "xxx005/", "xxx007/", "xxx008/"}
	got, gotMore, err := c.ListEntries(ctx, repo, "br_2", "", "", "/", 20)
	testCatalogerListEntriesVerifyResponse(t, got, gotMore, err, wantEntries)

	wantEntries = []string{"xxx002/entry000", "xxx002/entry001", "xxx002/entry002", "xxx002/entry003",
		"xxx002/entry004", "xxx002/entry005", "xxx002/entry006", "xxx002/entry007", "xxx002/entry008",
		"xxx002/entry009"}
	got, gotMore, err = c.ListEntries(ctx, repo, "br_2", "xxx002/", "", "/", 20)
	testCatalogerListEntriesVerifyResponse(t, got, gotMore, err, wantEntries)
}

func testCatalogerListEntriesVerifyResponse(t *testing.T, got []*catalog.Entry, gotMore bool, gotErr error, entries []string) {
	t.Helper()
	if gotErr != nil {
		t.Fatalf("Got expected error: %s", gotErr)
	}
	if len(entries) != len(got) {
		t.Fatalf("Got %d entries, expected %d", len(got), len(entries))
	}
	for i, p := range entries {
		if p != got[i].Path {
			t.Fatalf("Value path '%s', expected '%s'", p, got[i].Path)
		}
	}
	if gotMore {
		t.Fatalf("not expected more")
	}
}

func pathExt(i int) string {
	return fmt.Sprintf("%03d", i)
}

func TestCataloger_ListEntries_Prefix(t *testing.T) {
	ctx := context.Background()
	c := testCataloger(t)

	// produce test data
	repo := testCatalogerRepo(t, ctx, c, "repo", "master")
	prefixes := []string{"aaa", "bbb", "ccc"}
	for _, prefix := range prefixes {
		testCatalogerCreateEntry(t, ctx, c, repo, "master", prefix+"/index.txt", nil, "")
		for i := 0; i < 3; i++ {
			testCatalogerCreateEntry(t, ctx, c, repo, "master", prefix+"/file."+strconv.Itoa(i), nil, "")
		}
	}

	tests := []struct {
		name        string
		path        string
		want        []string
		wantByLevel []string
	}{
		{
			name:        "specific file",
			path:        "bbb/file.2",
			want:        []string{"bbb/file.2"},
			wantByLevel: []string{"bbb/file.2"},
		},
	}
	for _, tt := range tests {
		// without delimiter
		t.Run(tt.name, func(t *testing.T) {
			got, gotMore, err := c.ListEntries(ctx, repo, "master", tt.path, "", "", -1)
			if err != nil {
				t.Fatalf("List err = %s, expected no error", err)
			}
			gotPaths := extractEntriesPaths(got)
			if diff := deep.Equal(gotPaths, tt.wantByLevel); diff != nil {
				t.Fatal("List", diff)
			}
			if gotMore != false {
				t.Fatal("List got more should be false")
			}
		})
		// by level
		t.Run(tt.name+"-by level", func(t *testing.T) {
			got, gotMore, err := c.ListEntries(ctx, repo, "master", tt.path, "", catalog.DefaultPathDelimiter, -1)
			if err != nil {
				t.Fatalf("List by level - err = %s, expected no error", err)
			}
			gotPaths := extractEntriesPaths(got)
			if diff := deep.Equal(gotPaths, tt.wantByLevel); diff != nil {
				t.Fatal("List by level", diff)
			}
			if gotMore != false {
				t.Fatal("List got more should be false")
			}
		})
	}
}

func extractEntriesPaths(entries []*catalog.Entry) []string {
	result := make([]string, len(entries))
	for i, ent := range entries {
		result[i] = ent.Path
	}
	return result
}

func TestCataloger_ListEntries_ByLevelAfter(t *testing.T) {
	ctx := context.Background()
	c := testCataloger(t)

	// produce test data
	const namesCount = 10
	names := make([]string, namesCount)
	repo := testCatalogerRepo(t, ctx, c, "repo", "master")
	for i := 0; i < namesCount; i++ {
		p := "file." + strconv.Itoa(i)
		testCatalogerCreateEntry(t, ctx, c, repo, "master", p, nil, "")
		names[i] = p
	}

	const testLimit = 2
	delimiters := []string{"", catalog.DefaultPathDelimiter}
	for _, delimiter := range delimiters {
		t.Run("delimiter_"+strconv.FormatBool(delimiter != ""), func(t *testing.T) {
			var after string
			var namesIdx int
			for {
				entries, more, err := c.ListEntries(ctx, repo, "master", "", after, delimiter, testLimit)
				testutil.MustDo(t, "list entries", err)
				// compare the names we got so far
				for i, ent := range entries {
					if namesIdx >= len(names) {
						t.Fatalf("List exceeded range of expected names. Index %d, when %d names", namesIdx, len(names))
					} else if names[namesIdx] != ent.Path {
						t.Fatalf("List pos %d, path %s - expected %s (index %d)", i, ent.Path, names[namesIdx], namesIdx)
					}
					namesIdx += 1
				}
				// prepare for the next page if needed
				if !more {
					break
				}
				if len(entries) == 0 {
					t.Fatal("List got more, but got no entries")
				}
				after = entries[len(entries)-1].Path
			}
		})
	}
}

func TestCataloger_ListEntries_Uncommitted(t *testing.T) {
	ctx := context.Background()
	c := testCataloger(t)
	repo := testCatalogerRepo(t, ctx, c, "repo", "master")
	testCatalogerCreateEntry(t, ctx, c, repo, "master", "my_entry", nil, "abcd")
	_, err := c.Commit(ctx, repo, "master", "commit test files", "tester", nil)
	testutil.MustDo(t, "commit my_entry", err)
	testutil.MustDo(t, "delete the first committed file",
		c.DeleteEntry(ctx, repo, "master", "my_entry"))
	// an uncommitted tombstone "hides" a committed entry
	got, _, err := c.ListEntries(ctx, repo, "master", "", "", catalog.DefaultPathDelimiter, -1)
	testutil.MustDo(t, "List", err)
	if len(got) != 0 {
		t.Fatalf("List %d entries, expected none", len(got))
	}

	testCatalogerCreateEntry(t, ctx, c, repo, "master", "my_entry", nil, "abcd")
	// an uncommitted entry is detected
	got, _, err = c.ListEntries(ctx, repo, "master", "", "", catalog.DefaultPathDelimiter, -1)
	testutil.MustDo(t, "List", err)
	expectedPaths := []string{"my_entry"}
	if diff := deep.Equal(extractEntriesPaths(got), expectedPaths); diff != nil {
		t.Fatal("List", diff)
	}

	_, err = c.Commit(ctx, repo, "master", "commit test files", "tester", nil)
	testutil.MustDo(t, "commit my_entry", err)
	testutil.MustDo(t, "delete the first committed file",
		c.DeleteEntry(ctx, repo, "master", "my_entry"))
	_, err = c.Commit(ctx, repo, "master", "commit test files", "tester", nil)
	testutil.MustDo(t, "commit my_entry deletion", err)
	// deleted entry will not be displayed
	got, _, err = c.ListEntries(ctx, repo, "master", "", "", catalog.DefaultPathDelimiter, -1)
	testutil.MustDo(t, "List", err)
	if len(got) != 0 {
		t.Fatalf("List %d entries, expected none", len(got))
	}

	// an uncommitted entry is detected even if the entry is deleted
	testCatalogerCreateEntry(t, ctx, c, repo, "master", "my_entry", nil, "abcd")
	got, _, err = c.ListEntries(ctx, repo, "master", "", "", catalog.DefaultPathDelimiter, -1)
	testutil.MustDo(t, "List", err)
	if diff := deep.Equal(extractEntriesPaths(got), expectedPaths); diff != nil {
		t.Fatal("List", diff)
	}
}

func TestCataloger_ListEntries_ReadingUncommittedFromLineage(t *testing.T) {
	ctx := context.Background()
	c := testCataloger(t)
	repo := testCatalogerRepo(t, ctx, c, "repo", "master")
	for i := 0; i < 10; i++ {
		z := fmt.Sprintf("%03d", i)
		path := "my_entry" + z
		testCatalogerCreateEntry(t, ctx, c, repo, "master", path, nil, "abcd"+z)
	}
	_, err := c.Commit(ctx, repo, "master", "commit first 10 in master", "tester", nil)
	testutil.MustDo(t, "commit first 10 in master", err)

	// deletion that will not be seen by br_1, because it is not committed for br_1. so br_1 still sees this
	testutil.MustDo(t, "delete the first committed file",
		c.DeleteEntry(ctx, repo, "master", "my_entry001"))
	testCatalogerBranch(t, ctx, c, repo, "br_1", "master")
	for i := 10; i < 20; i++ {
		z := fmt.Sprintf("%03d", i)
		path := "my_entry/sub-" + z
		testCatalogerCreateEntry(t, ctx, c, repo, "br_1", path, nil, "abcd"+z)
	}

	// create unreadable in ancestor
	// committed
	for i := 20; i < 50; i++ {
		z := fmt.Sprintf("%03d", i)
		path := "my_entry" + z
		testCatalogerCreateEntry(t, ctx, c, repo, "master", path, nil, "abcd"+z)
	}
	_, err = c.Commit(ctx, repo, "master", "commit 20-50 in master", "tester", nil)
	testutil.MustDo(t, "commit 20-50 in master", err)

	// uncommitted
	for i := 50; i < 70; i++ {
		z := fmt.Sprintf("%03d", i)
		path := "my_entry" + z
		testCatalogerCreateEntry(t, ctx, c, repo, "master", path, nil, "abcd"+z)
	}
	got, _, err := c.ListEntries(ctx, repo, "br_1", "", "", catalog.DefaultPathDelimiter, -1)
	testutil.Must(t, err)
	if len(got) != 11 {
		t.Fatalf("expected 11 entries, read %d", len(got))
	}
}

func TestCataloger_ListEntries_MultipleDelete(t *testing.T) {
	ctx := context.Background()
	c := testCataloger(t)
	repo := testCatalogerRepo(t, ctx, c, "repo", "master")
	testListEntriesCreateEntries(t, ctx, c, repo, "master", 50, 100, 1)
	testCatalogerBranch(t, ctx, c, repo, "br_1", "master")
	testListEntriesCreateEntries(t, ctx, c, repo, "br_1", 51, 100, 2)

	got, _, err := c.ListEntries(ctx, repo, "br_1", "", "", catalog.DefaultPathDelimiter, -1)
	testutil.Must(t, err)
	if len(got) != 100 {
		t.Fatalf("expected 100 entries, read %d", len(got))
	}
	for i, ent := range got {
		if i%2 == 0 {
			if ent.Size != 50 {
				t.Errorf("entry %s size not 50, is %d", ent.Path, ent.Size)
			}
		} else {
			if ent.Size != 49 {
				t.Errorf("entry %s size not 50, is %d", ent.Path, ent.Size)
			}
		}
	}
	// check identifying uncommitted delete
	for i := 0; i < 100; i += 2 {
		p := fmt.Sprintf("my_entry%03d", i)
		testutil.MustDo(t, "delete files in br_1", c.DeleteEntry(ctx, repo, "br_1", p))
	}
	got, _, err = c.ListEntries(ctx, repo, "br_1", "", "", catalog.DefaultPathDelimiter, -1)
	testutil.Must(t, err)
	if len(got) != 50 {
		t.Fatalf("expected 50 entries, read %d", len(got))
	}
	for _, ent := range got {
		if ent.Size != 49 {
			t.Errorf("entry %s size not 49, is %d", ent.Path, ent.Size)
		}
	}
	// check it can identify committed tombstones
	_, err = c.Commit(ctx, repo, "br_1", "commit  br_1 after delete ", "tester", nil)
	testutil.MustDo(t, "commit br_1 after delete ", err)
	got, _, err = c.ListEntries(ctx, repo, "br_1", "", "", catalog.DefaultPathDelimiter, -1)
	testutil.Must(t, err)
	if len(got) != 50 {
		t.Fatalf("expected 50 entries, read %d", len(got))
	}
	for _, ent := range got {
		if ent.Size != 49 {
			t.Errorf("entry %s size not 49, is %d", ent.Path, ent.Size)
		}
	}
	// create tombstones for entries in master
	for i := 1; i < 100; i += 2 {
		p := fmt.Sprintf("my_entry%03d", i)
		testutil.MustDo(t, "delete files in br_1", c.DeleteEntry(ctx, repo, "br_1", p))
	}
	got, _, err = c.ListEntries(ctx, repo, "br_1", "", "", catalog.DefaultPathDelimiter, -1)
	testutil.Must(t, err)
	if len(got) != 0 {
		t.Fatalf("expected 0 entries, read %d", len(got))
	}
	// check after commit
	_, err = c.Commit(ctx, repo, "br_1", "commit br_1 after delete ", "tester", nil)
	testutil.MustDo(t, "commit br_1 after delete ", err)
	got, _, err = c.ListEntries(ctx, repo, "br_1", "", "", catalog.DefaultPathDelimiter, -1)
	testutil.Must(t, err)
	if len(got) != 0 {
		t.Fatalf("expected 0 entries, read %d", len(got))
	}
	got, _, err = c.ListEntries(ctx, repo, "master", "", "", catalog.DefaultPathDelimiter, -1)
	testutil.Must(t, err)
	if len(got) != 100 {
		t.Fatalf("expected 100 entries, read %d", len(got))
	}
}

func TestCataloger_ListEntries_IgnoreDeleteByLineage(t *testing.T) {
	ctx := context.Background()
	c := testCataloger(t)
	repo := testCatalogerRepo(t, ctx, c, "repo", "master")
	testListEntriesCreateEntries(t, ctx, c, repo, "master", 50, 100, 1)
	testCatalogerBranch(t, ctx, c, repo, "br_1", "master")
	for i := 0; i < 100; i++ {
		p := fmt.Sprintf("my_entry%03d", i)
		testutil.MustDo(t, "delete files in master", c.DeleteEntry(ctx, repo, "master", p))
	}
	// br_1 ignores uncommitted tombstones on master
	got, _, err := c.ListEntries(ctx, repo, "br_1", "", "", catalog.DefaultPathDelimiter, -1)
	testutil.Must(t, err)
	if len(got) != 100 {
		t.Fatalf("expected 100 entries on br_1, read %d", len(got))
	}
	// master is really deleted
	got, _, err = c.ListEntries(ctx, repo, "master", "", "", catalog.DefaultPathDelimiter, -1)
	testutil.Must(t, err)
	if len(got) != 0 {
		t.Fatalf("expected 0 entries on master, read %d", len(got))
	}
	_, err = c.Commit(ctx, repo, "master", "commit master", "tester", nil)
	testutil.MustDo(t, "commit master ", err)
	// br_1 ignores deleted committed entries on master
	got, _, err = c.ListEntries(ctx, repo, "br_1", "", "", catalog.DefaultPathDelimiter, -1)
	testutil.Must(t, err)
	if len(got) != 100 {
		t.Fatalf("expected 100 entries on br_1, read %d", len(got))
	}
	// now merge master to br_1
	_, err = c.Merge(ctx, repo, "master", "br_1", "tester", "merge deletions", nil)
	testutil.Must(t, err)
	got, _, err = c.ListEntries(ctx, repo, "br_1", "", "", catalog.DefaultPathDelimiter, -1)
	testutil.Must(t, err)
	if len(got) != 0 {
		t.Fatalf("expected 0 entries on br_1, read %d", len(got))
	}
}

func testListEntriesCreateEntries(t *testing.T, ctx context.Context, c catalog.Cataloger, repo, branch string, numIterations, numEntries, skip int) {
	t.Helper()
	for i := 0; i < numIterations; i++ {
		for j := 0; j < numEntries; j += skip {
			path := fmt.Sprintf("my_entry%03d", j)
			seed := strconv.Itoa(i)
			checksum := testCreateEntryCalcChecksum(path, t.Name(), seed)
			err := c.CreateEntry(ctx, repo, branch, catalog.Entry{Path: path, Checksum: checksum, PhysicalAddress: checksum, Size: int64(i)}, catalog.CreateEntryParams{})
			if err != nil {
				t.Fatalf("Failed to create entry %s on branch %s, repository %s: %s", path, branch, repo, err)
			}
		}
		msg := fmt.Sprintf("commit %s cycle %d", branch, i)
		_, err := c.Commit(ctx, repo, branch, msg, "tester", nil)
		testutil.MustDo(t, msg, err)
	}
}
