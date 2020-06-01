package catalog

import (
	"context"
	"crypto/sha256"
	"fmt"
	"reflect"
	"testing"

	"github.com/treeverse/lakefs/testutil"
)

func TestCataloger_ListEntriesByPrefix(t *testing.T) {
	ctx := context.Background()
	cdb, _ := testutil.GetDB(t, databaseURI, "lakefs_catalog")
	c := NewCataloger(cdb)

	// produce test data
	if err := c.CreateRepo(ctx, "repo1", "bucket1", "master"); err != nil {
		t.Fatal("create repo for testing", err)
	}
	const numEntries = 5
	for i := 0; i < numEntries; i++ {
		n := i + 1
		filePath := fmt.Sprintf("/file%d", n)
		fileChecksum := fmt.Sprintf("%x", sha256.Sum256([]byte(filePath)))
		fileAddress := fmt.Sprintf("/addr%d", n)
		fileSize := n * 10
		fileStage := false
		if i%2 == 0 {
			fileStage = true
		}
		err := c.WriteEntry(ctx, "repo1", "master", filePath, fileChecksum, fileAddress, fileSize, fileStage, nil)
		if err != nil {
			t.Fatal("failed to write entry", err)
		}
	}

	isStagedTrue := true
	isStagedFalse := false

	type args struct {
		repo        string
		branch      string
		path        string
		after       string
		limit       int
		readOptions EntryReadOptions
		descend     bool
	}
	tests := []struct {
		name        string
		args        args
		wantEntries []Entry
		wantMore    bool
		wantErr     bool
	}{
		{
			name: "all unstaged",
			args: args{
				repo:        "repo1",
				branch:      "master",
				path:        "",
				after:       "",
				limit:       -1,
				readOptions: EntryReadOptions{EntryState: EntryStateUnstaged},
				descend:     false,
			},
			wantEntries: []Entry{
				{Path: "/file1", PhysicalAddress: "/addr1", Size: 10, Checksum: "7c9d66ac57c9fa91bb375256fe1541e33f9548904c3f41fcd1e1208f2f3559f1", IsStaged: &isStagedTrue},
				{Path: "/file2", PhysicalAddress: "/addr2", Size: 20, Checksum: "a23eaeb64fff1004b1ef460294035633055bb49bc7b99bedc1493aab73d03f63", IsStaged: &isStagedFalse},
				{Path: "/file3", PhysicalAddress: "/addr3", Size: 30, Checksum: "fdfe3b8d45740319c989f33eaea4e3acbd3d7e01e0484d8e888d95bcc83d43f3", IsStaged: &isStagedTrue},
				{Path: "/file4", PhysicalAddress: "/addr4", Size: 40, Checksum: "49f014abae232570cc48072bac6b70531bba7e883ea04b448c6cbeed1446e6ff", IsStaged: &isStagedFalse},
				{Path: "/file5", PhysicalAddress: "/addr5", Size: 50, Checksum: "53c9486452c01e26833296dcf1f701379fa22f01e610dd9817d064093daab07d", IsStaged: &isStagedTrue},
			},
			wantMore: false,
			wantErr:  false,
		},
		{
			name: "all staged",
			args: args{
				repo:        "repo1",
				branch:      "master",
				path:        "",
				after:       "",
				limit:       -1,
				readOptions: EntryReadOptions{EntryState: EntryStateStaged},
				descend:     false,
			},
			wantEntries: []Entry{
				{Path: "/file1", PhysicalAddress: "/addr1", Size: 10, Checksum: "7c9d66ac57c9fa91bb375256fe1541e33f9548904c3f41fcd1e1208f2f3559f1", IsStaged: &isStagedTrue},
				{Path: "/file3", PhysicalAddress: "/addr3", Size: 30, Checksum: "fdfe3b8d45740319c989f33eaea4e3acbd3d7e01e0484d8e888d95bcc83d43f3", IsStaged: &isStagedTrue},
				{Path: "/file5", PhysicalAddress: "/addr5", Size: 50, Checksum: "53c9486452c01e26833296dcf1f701379fa22f01e610dd9817d064093daab07d", IsStaged: &isStagedTrue},
			},
			wantMore: false,
			wantErr:  false,
		},
		{
			name: "all committed",
			args: args{
				repo:        "repo1",
				branch:      "master",
				path:        "",
				after:       "",
				limit:       -1,
				readOptions: EntryReadOptions{EntryState: EntryStateCommitted},
				descend:     false,
			},
			wantEntries: nil,
			wantMore:    false,
			wantErr:     false,
		},
		{
			name: "first 2",
			args: args{
				repo:        "repo1",
				branch:      "master",
				path:        "",
				after:       "",
				limit:       2,
				readOptions: EntryReadOptions{EntryState: EntryStateUnstaged},
				descend:     false,
			},
			wantEntries: []Entry{
				{Path: "/file1", PhysicalAddress: "/addr1", Size: 10, Checksum: "7c9d66ac57c9fa91bb375256fe1541e33f9548904c3f41fcd1e1208f2f3559f1", IsStaged: &isStagedTrue},
				{Path: "/file2", PhysicalAddress: "/addr2", Size: 20, Checksum: "a23eaeb64fff1004b1ef460294035633055bb49bc7b99bedc1493aab73d03f63", IsStaged: &isStagedFalse},
			},
			wantMore: true,
			wantErr:  false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, gotMore, err := c.ListEntriesByPrefix(ctx, tt.args.repo, tt.args.branch, tt.args.path, tt.args.after, tt.args.limit, tt.args.readOptions, tt.args.descend)
			if (err != nil) != tt.wantErr {
				t.Errorf("ListEntriesByPrefix() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			// copy the Entry fields we like to compare
			var gotEntries []Entry
			for _, ent := range got {
				gotEntries = append(gotEntries, Entry{
					Path:            ent.Path,
					PhysicalAddress: ent.PhysicalAddress,
					Size:            ent.Size,
					Checksum:        ent.Checksum,
					IsStaged:        ent.IsStaged,
				})
			}

			if !reflect.DeepEqual(gotEntries, tt.wantEntries) {
				t.Errorf("ListEntriesByPrefix() got = %+v, want = %+v", gotEntries, tt.wantEntries)
			}
			if gotMore != tt.wantMore {
				t.Errorf("ListEntriesByPrefix() gotMore = %v, want = %v", gotMore, tt.wantMore)
			}
		})
	}
}
