package catalog

import (
	"context"
	"crypto/sha256"
	"fmt"
	"reflect"
	"testing"

	"github.com/treeverse/lakefs/testutil"
)

func TestCataloger_ListEntries(t *testing.T) {
	ctx := context.Background()
	c := testCataloger(t)

	// produce test data
	testutil.MustDo(t, "create test repo",
		c.CreateRepository(ctx, "repo1", "bucket1", "master"))
	for i := 0; i < 5; i++ {
		n := i + 1
		filePath := fmt.Sprintf("/file%d", n)
		fileChecksum := fmt.Sprintf("%x", sha256.Sum256([]byte(filePath)))
		fileAddress := fmt.Sprintf("/addr%d", n)
		fileSize := n * 10
		testutil.MustDo(t, "create test entry",
			c.CreateEntry(ctx, "repo1", "master", filePath, fileChecksum, fileAddress, fileSize, nil))
		if i == 2 {
			_, err := c.Commit(ctx, "repo1", "master", "commit test files", "tester", nil)
			testutil.MustDo(t, "commit test files", err)
		}
	}

	type args struct {
		repository string
		branch     string
		commitID   CommitID
		path       string
		after      string
		limit      int
	}
	tests := []struct {
		name        string
		args        args
		wantEntries []Entry
		wantMore    bool
		wantErr     bool
	}{
		{
			name: "all uncommitted",
			args: args{
				repository: "repo1",
				branch:     "master",
				commitID:   UncommittedID,
				path:       "",
				after:      "",
				limit:      -1,
			},
			wantEntries: []Entry{
				{Path: "/file1", PhysicalAddress: "/addr1", Size: 10, Checksum: "7c9d66ac57c9fa91bb375256fe1541e33f9548904c3f41fcd1e1208f2f3559f1"},
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
				branch:     "master",
				commitID:   UncommittedID,
				path:       "",
				after:      "",
				limit:      2,
			},
			wantEntries: []Entry{
				{Path: "/file1", PhysicalAddress: "/addr1", Size: 10, Checksum: "7c9d66ac57c9fa91bb375256fe1541e33f9548904c3f41fcd1e1208f2f3559f1"},
				{Path: "/file2", PhysicalAddress: "/addr2", Size: 20, Checksum: "a23eaeb64fff1004b1ef460294035633055bb49bc7b99bedc1493aab73d03f63"},
			},
			wantMore: true,
			wantErr:  false,
		},
		{
			name: "last 2",
			args: args{
				repository: "repo1",
				branch:     "master",
				commitID:   UncommittedID,
				path:       "",
				after:      "/file3",
				limit:      2,
			},
			wantEntries: []Entry{
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
				branch:     "master",
				commitID:   CommittedID,
				path:       "",
				after:      "/file1",
				limit:      -1,
			},
			wantEntries: []Entry{
				{Path: "/file2", PhysicalAddress: "/addr2", Size: 20, Checksum: "a23eaeb64fff1004b1ef460294035633055bb49bc7b99bedc1493aab73d03f63"},
				{Path: "/file3", PhysicalAddress: "/addr3", Size: 30, Checksum: "fdfe3b8d45740319c989f33eaea4e3acbd3d7e01e0484d8e888d95bcc83d43f3"},
			},
			wantMore: false,
			wantErr:  false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, gotMore, err := c.ListEntries(ctx, tt.args.repository, tt.args.branch, tt.args.commitID, tt.args.path, tt.args.after, tt.args.limit)
			if (err != nil) != tt.wantErr {
				t.Fatalf("ListEntries() error = %v, wantErr %v", err, tt.wantErr)
			}
			// copy the Entry fields we like to compare
			var gotEntries []Entry
			for _, ent := range got {
				gotEntries = append(gotEntries, Entry{
					Path:            ent.Path,
					PhysicalAddress: ent.PhysicalAddress,
					Size:            ent.Size,
					Checksum:        ent.Checksum,
				})
			}

			if !reflect.DeepEqual(gotEntries, tt.wantEntries) {
				t.Errorf("ListEntries() got = %+v, want = %+v", gotEntries, tt.wantEntries)
			}
			if gotMore != tt.wantMore {
				t.Errorf("ListEntries() gotMore = %v, want = %v", gotMore, tt.wantMore)
			}
		})
	}
}
