package merkle_test

import (
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/treeverse/lakefs/testutil"

	"github.com/treeverse/lakefs/index/merkle"

	"github.com/treeverse/lakefs/index/model"
)

type entryLike interface {
	GetType() string
	GetName() string
	GetAddress() string
}

func compareEntries(a, b entryLike) int {
	// directories first
	if a.GetType() != b.GetType() {
		if a.GetType() < b.GetType() {
			return -1
		} else if a.GetType() > b.GetType() {
			return 1
		}
		return 0
	}
	// then sort by name
	return strings.Compare(a.GetName(), b.GetName())
}

func TestNew(t *testing.T) {
	res := compareEntries(&model.Entry{
		Name:         "hooks",
		Address:      "abc",
		EntryType:    model.EntryTypeObject,
		CreationDate: time.Now(),
		Size:         300,
	}, &model.Entry{
		Name:         "hooks",
		Address:      "def",
		EntryType:    model.EntryTypeObject,
		CreationDate: time.Now(),
		Size:         300,
	})

	if res != 0 {
		t.Fatalf("expected 0 got %d", res)
	}

}

func TestMerkle_GetEntry(t *testing.T) {
	type fields struct {
		root string
	}
	type args struct {
		tx  merkle.TreeReader
		pth string
		typ string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    *model.Entry
		wantErr bool
	}{
		{
			name: "read file from root",
			fields: fields{
				root: "master",
			},
			args: args{
				tx: testutil.ConstructTree(map[string][]*model.Entry{
					"master": {
						{Name: "file1", Address: "file1", EntryType: model.EntryTypeObject},
					},
				}),
				pth: "file1",
				typ: model.EntryTypeObject,
			},
			want: &model.Entry{
				Name:      "file1",
				Address:   "file1",
				EntryType: model.EntryTypeObject,
			},
			wantErr: false,
		},
		{
			name: "read object from tree",
			fields: fields{
				root: "root",
			},
			args: args{
				tx: testutil.ConstructTree(map[string][]*model.Entry{
					"root": {
						{Name: "dir/", Address: "dir/", EntryType: model.EntryTypeTree},
					},
					"dir/": {
						{Name: "file1", Address: "file1", EntryType: model.EntryTypeObject},
					},
				}),
				pth: "dir/file1",
				typ: model.EntryTypeObject,
			},
			want: &model.Entry{
				Name:      "file1",
				Address:   "file1",
				EntryType: model.EntryTypeObject,
			},
			wantErr: false,
		},
		{
			name: "read tree from root",
			fields: fields{
				root: "root",
			},
			args: args{
				tx: testutil.ConstructTree(map[string][]*model.Entry{
					"root": {
						{Name: "dir/", Address: "dir/", EntryType: model.EntryTypeTree},
					},
					"dir/": {
						{Name: "file1", Address: "file1", EntryType: model.EntryTypeObject},
					},
				}),
				pth: "dir/",
				typ: model.EntryTypeTree,
			},
			want: &model.Entry{
				Name:      "dir/",
				Address:   "dir/",
				EntryType: model.EntryTypeTree,
			},
			wantErr: false,
		},
		{
			name: "read non existing file",
			fields: fields{
				root: "root",
			},
			args: args{
				tx: testutil.ConstructTree(map[string][]*model.Entry{
					"root": {
						{Name: "dir/", Address: "dir/", EntryType: model.EntryTypeTree},
					},
				}),
				pth: "file2",
				typ: model.EntryTypeObject,
			},
			want:    nil,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := merkle.New(tt.fields.root)
			got, err := m.GetEntry(tt.args.tx, tt.args.pth, tt.args.typ)
			if (err != nil) != tt.wantErr {
				t.Errorf("GetEntry() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("GetEntry() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestMerkle_GetEntries(t *testing.T) {
	type fields struct {
		root string
	}
	type args struct {
		tx  merkle.TreeReader
		pth string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    []*model.Entry
		wantErr bool
	}{
		{
			name: "read objects from root",
			fields: fields{
				root: "master",
			},
			args: args{
				tx: testutil.ConstructTree(map[string][]*model.Entry{
					"master": {
						{Name: "file1", Address: "file1", EntryType: model.EntryTypeObject},
						{Name: "file2", Address: "file2", EntryType: model.EntryTypeObject},
					},
				}),
				pth: "",
			},
			want: []*model.Entry{
				{
					Name:      "file1",
					Address:   "file1",
					EntryType: model.EntryTypeObject,
				}, {
					Name:      "file2",
					Address:   "file2",
					EntryType: model.EntryTypeObject,
				},
			},
			wantErr: false,
		},
		{
			name: "read objects from tree",
			fields: fields{
				root: "root",
			},
			args: args{
				tx: testutil.ConstructTree(map[string][]*model.Entry{
					"root": {
						{Name: "dir", Address: "dir", EntryType: model.EntryTypeTree},
					},
					"dir": {
						{Name: "file1", Address: "file1", EntryType: model.EntryTypeObject},
						{Name: "file2", Address: "file2", EntryType: model.EntryTypeObject},
						{Name: "file3", Address: "file3", EntryType: model.EntryTypeObject},
					},
				}),
				pth: "dir",
			},
			want: []*model.Entry{
				{
					Name:      "file1",
					Address:   "file1",
					EntryType: model.EntryTypeObject,
				},
				{
					Name:      "file2",
					Address:   "file2",
					EntryType: model.EntryTypeObject,
				},
				{
					Name:      "file3",
					Address:   "file3",
					EntryType: model.EntryTypeObject,
				},
			},
			wantErr: false,
		},
		{
			name: "read tree and object under tree",
			fields: fields{
				root: "root",
			},
			args: args{
				tx: testutil.ConstructTree(map[string][]*model.Entry{
					"root": {
						{Name: "dir", Address: "dir", EntryType: model.EntryTypeTree},
					},
					"dir": {
						{Name: "file1", Address: "file1", EntryType: model.EntryTypeObject},
						{Name: "file2", Address: "file2", EntryType: model.EntryTypeObject},
						{Name: "dir2", Address: "dir2", EntryType: model.EntryTypeTree},
					},
					"dir2": {
						{Name: "file4", Address: "file4", EntryType: model.EntryTypeObject},
					},
				}),
				pth: "dir",
			},
			want: []*model.Entry{
				{
					Name:      "dir2",
					Address:   "dir2",
					EntryType: model.EntryTypeTree,
				},
				{
					Name:      "file1",
					Address:   "file1",
					EntryType: model.EntryTypeObject,
				},
				{
					Name:      "file2",
					Address:   "file2",
					EntryType: model.EntryTypeObject,
				},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := merkle.New(tt.fields.root)
			got, err := m.GetEntries(tt.args.tx, tt.args.pth)
			if (err != nil) != tt.wantErr {
				t.Errorf("GetEntries() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("GetEntries() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func pstr(s string) *string {
	return &s
}

func pint64(s int64) *int64 {
	return &s
}

func TestMerkle_Update(t *testing.T) {
	n := time.Now()
	tests := []struct {
		name        string
		initialWS   []*model.WorkspaceEntry
		editEntries []*model.WorkspaceEntry
		wantedWS    []*model.WorkspaceEntry
		wantErr     bool
	}{
		{
			name: "add one objects to root",

			initialWS: []*model.WorkspaceEntry{
				{
					Path:              "file1",
					EntryName:         pstr("file1"),
					EntryAddress:      pstr("123456789"),
					EntryType:         pstr(model.EntryTypeObject),
					EntryCreationDate: &n,
					Tombstone:         false,
				},
			},
			editEntries: []*model.WorkspaceEntry{
				{
					Path:              "file2",
					EntryName:         pstr("file2"),
					EntryAddress:      pstr("123456789"),
					EntryType:         pstr(model.EntryTypeObject),
					EntryCreationDate: &n,
					Tombstone:         false,
				},
			},

			wantedWS: []*model.WorkspaceEntry{
				{
					Path:              "file1",
					EntryName:         pstr("file1"),
					EntryAddress:      pstr("123456789"),
					EntryType:         pstr(model.EntryTypeObject),
					EntryCreationDate: &n,
					Tombstone:         false,
				},
				{
					Path:              "file2",
					EntryName:         pstr("file2"),
					EntryAddress:      pstr("123456789"),
					EntryType:         pstr(model.EntryTypeObject),
					EntryCreationDate: &n,
					Tombstone:         false,
				},
			},
		},
		{
			name: "add objects with size",

			initialWS: []*model.WorkspaceEntry{
				{
					Path:              "a/file1",
					EntryName:         pstr("file1"),
					EntryAddress:      pstr("123456789"),
					EntryType:         pstr(model.EntryTypeObject),
					EntrySize:         pint64(500),
					EntryCreationDate: &n,
					Tombstone:         false,
				},
			},
			editEntries: []*model.WorkspaceEntry{
				{
					Path:              "file2",
					EntryName:         pstr("file2"),
					EntryAddress:      pstr("123456789"),
					EntryType:         pstr(model.EntryTypeObject),
					EntrySize:         pint64(500),
					EntryCreationDate: &n,
					Tombstone:         false,
				},
				{
					Path:              "a/file3",
					EntryName:         pstr("file3"),
					EntryAddress:      pstr("123456789"),
					EntryType:         pstr(model.EntryTypeObject),
					EntrySize:         pint64(1000),
					EntryCreationDate: &n,
					Tombstone:         false,
				},
			},

			wantedWS: []*model.WorkspaceEntry{
				{
					Path:              "a/file1",
					EntryName:         pstr("file1"),
					EntryAddress:      pstr("123456789"),
					EntryType:         pstr(model.EntryTypeObject),
					EntrySize:         pint64(500),
					EntryCreationDate: &n,
					Tombstone:         false,
				},
				{
					Path:              "file2",
					EntryName:         pstr("file2"),
					EntryAddress:      pstr("123456789"),
					EntryType:         pstr(model.EntryTypeObject),
					EntrySize:         pint64(500),
					EntryCreationDate: &n,
					Tombstone:         false,
				},
				{
					Path:              "a/file3",
					EntryName:         pstr("file3"),
					EntryAddress:      pstr("123456789"),
					EntryType:         pstr(model.EntryTypeObject),
					EntrySize:         pint64(1000),
					EntryCreationDate: &n,
					Tombstone:         false,
				},
			},
		},
		{
			name: "add object to tree",

			initialWS: []*model.WorkspaceEntry{

				{
					Path:              "a/file1",
					EntryName:         pstr("file1"),
					EntryAddress:      pstr("123456789"),
					EntryType:         pstr(model.EntryTypeObject),
					EntryCreationDate: &n,
					Tombstone:         false,
				},
				{
					Path:              "file2",
					EntryName:         pstr("file2"),
					EntryAddress:      pstr("123456789"),
					EntryType:         pstr(model.EntryTypeObject),
					EntryCreationDate: &n,
					Tombstone:         false,
				},
			},
			editEntries: []*model.WorkspaceEntry{

				{
					Path:              "a/file3",
					EntryName:         pstr("file3"),
					EntryAddress:      pstr("123456789"),
					EntryType:         pstr(model.EntryTypeObject),
					EntryCreationDate: &n,
					Tombstone:         false,
				},
			},

			wantedWS: []*model.WorkspaceEntry{
				{
					Path:              "a/file1",
					EntryName:         pstr("file1"),
					EntryAddress:      pstr("123456789"),
					EntryType:         pstr(model.EntryTypeObject),
					EntryCreationDate: &n,
					Tombstone:         false,
				},
				{
					Path:              "file2",
					EntryName:         pstr("file2"),
					EntryAddress:      pstr("123456789"),
					EntryType:         pstr(model.EntryTypeObject),
					EntryCreationDate: &n,
					Tombstone:         false,
				},
				{
					Path:              "a/file3",
					EntryName:         pstr("file3"),
					EntryAddress:      pstr("123456789"),
					EntryType:         pstr(model.EntryTypeObject),
					EntryCreationDate: &n,
					Tombstone:         false,
				},
			},
		},
		{
			name: "add object to deep tree",

			initialWS: []*model.WorkspaceEntry{

				{
					Path:              "a/file1",
					EntryName:         pstr("file1"),
					EntryAddress:      pstr("123456789"),
					EntryType:         pstr(model.EntryTypeObject),
					EntryCreationDate: &n,
					Tombstone:         false,
				},
				{
					Path:              "a/z/file2",
					EntryName:         pstr("file2"),
					EntryAddress:      pstr("123456789"),
					EntryType:         pstr(model.EntryTypeObject),
					EntryCreationDate: &n,
					Tombstone:         false,
				},
				{
					Path:              "a/b/file3",
					EntryName:         pstr("file3"),
					EntryAddress:      pstr("123456789"),
					EntryType:         pstr(model.EntryTypeObject),
					EntryCreationDate: &n,
					Tombstone:         false,
				},
			},
			editEntries: []*model.WorkspaceEntry{

				{
					Path:              "a/z/file4",
					EntryName:         pstr("file4"),
					EntryAddress:      pstr("123456789"),
					EntryType:         pstr(model.EntryTypeObject),
					EntryCreationDate: &n,

					Tombstone: false,
				},
				{
					Path:              "a/b/file5",
					EntryName:         pstr("file5"),
					EntryAddress:      pstr("123456789"),
					EntryType:         pstr(model.EntryTypeObject),
					EntryCreationDate: &n,
					Tombstone:         false,
				},
			},

			wantedWS: []*model.WorkspaceEntry{
				{
					Path:              "a/file1",
					EntryName:         pstr("file1"),
					EntryAddress:      pstr("123456789"),
					EntryType:         pstr(model.EntryTypeObject),
					EntryCreationDate: &n,
					Tombstone:         false,
				},
				{
					Path:              "a/z/file2",
					EntryName:         pstr("file2"),
					EntryAddress:      pstr("123456789"),
					EntryType:         pstr(model.EntryTypeObject),
					EntryCreationDate: &n,
					Tombstone:         false,
				},
				{
					Path:              "a/b/file3",
					EntryName:         pstr("file3"),
					EntryAddress:      pstr("123456789"),
					EntryType:         pstr(model.EntryTypeObject),
					EntryCreationDate: &n,
					Tombstone:         false,
				},
				{
					Path:              "a/z/file4",
					EntryName:         pstr("file4"),
					EntryAddress:      pstr("123456789"),
					EntryType:         pstr(model.EntryTypeObject),
					EntryCreationDate: &n,
					Tombstone:         false,
				},
				{
					Path:              "a/b/file5",
					EntryName:         pstr("file5"),
					EntryAddress:      pstr("123456789"),
					EntryType:         pstr(model.EntryTypeObject),
					EntryCreationDate: &n,
					Tombstone:         false,
				},
			},
		},
		{
			name: "remove one object from root",

			initialWS: []*model.WorkspaceEntry{
				{
					Path:              "a/file1",
					EntryName:         pstr("file1"),
					EntryAddress:      pstr("123456789"),
					EntryType:         pstr(model.EntryTypeObject),
					EntryCreationDate: &n,
					Tombstone:         false,
				},
				{
					Path:              "file_remove",
					EntryName:         pstr("file_remove"),
					EntryAddress:      pstr("123456788"),
					EntryType:         pstr(model.EntryTypeObject),
					EntryCreationDate: &n,
					Tombstone:         false,
				},
			},
			editEntries: []*model.WorkspaceEntry{
				{
					Path:              "file_remove",
					EntryName:         pstr("file_remove"),
					EntryAddress:      pstr("123456788"),
					EntryType:         pstr(model.EntryTypeObject),
					EntryCreationDate: &n,
					Tombstone:         true,
				},
			},

			wantedWS: []*model.WorkspaceEntry{
				{
					Path:              "a/file1",
					EntryName:         pstr("file1"),
					EntryAddress:      pstr("123456789"),
					EntryType:         pstr(model.EntryTypeObject),
					EntryCreationDate: &n,
					Tombstone:         false,
				},
			},
		},
		{
			name: "remove one objects from tree with other objects",

			initialWS: []*model.WorkspaceEntry{
				{
					Path:              "a/file1",
					EntryName:         pstr("file1"),
					EntryAddress:      pstr("123456789"),
					EntryType:         pstr(model.EntryTypeObject),
					EntryCreationDate: &n,
					Tombstone:         false,
				},
				{
					Path:              "a/file_remove",
					EntryName:         pstr("file_remove"),
					EntryAddress:      pstr("123456788"),
					EntryType:         pstr(model.EntryTypeObject),
					EntryCreationDate: &n,
					Tombstone:         false,
				},
			},
			editEntries: []*model.WorkspaceEntry{
				{
					Path:              "a/file_remove",
					EntryName:         pstr("file_remove"),
					EntryAddress:      pstr("123456788"),
					EntryType:         pstr(model.EntryTypeObject),
					EntryCreationDate: &n,
					Tombstone:         true,
				},
			},

			wantedWS: []*model.WorkspaceEntry{
				{
					Path:              "a/file1",
					EntryName:         pstr("file1"),
					EntryAddress:      pstr("123456789"),
					EntryType:         pstr(model.EntryTypeObject),
					EntryCreationDate: &n,
					Tombstone:         false,
				},
			},
		},
		{
			name: "remove one file from tree with no other files",

			initialWS: []*model.WorkspaceEntry{
				{
					Path:              "file1",
					EntryName:         pstr("file1"),
					EntryAddress:      pstr("123456789"),
					EntryType:         pstr(model.EntryTypeObject),
					EntryCreationDate: &n,
					Tombstone:         false,
				},
				{
					Path:              "a/file_remove",
					EntryName:         pstr("file_remove"),
					EntryAddress:      pstr("123456788"),
					EntryType:         pstr(model.EntryTypeObject),
					EntryCreationDate: &n,
					Tombstone:         false,
				},
			},
			editEntries: []*model.WorkspaceEntry{
				{
					Path:              "a/file_remove",
					EntryName:         pstr("file_remove"),
					EntryAddress:      pstr("123456788"),
					EntryType:         pstr(model.EntryTypeObject),
					EntryCreationDate: &n,
					Tombstone:         true,
				},
			},

			wantedWS: []*model.WorkspaceEntry{
				{
					Path:              "file1",
					EntryName:         pstr("file1"),
					EntryAddress:      pstr("123456789"),
					EntryType:         pstr(model.EntryTypeObject),
					EntryCreationDate: &n,
					Tombstone:         false,
				},
			},
		},
		{
			name: "remove non existing object",

			initialWS: []*model.WorkspaceEntry{
				{
					Path:              "file1",
					EntryName:         pstr("file1"),
					EntryAddress:      pstr("123456789"),
					EntryType:         pstr(model.EntryTypeObject),
					EntryCreationDate: &n,
					Tombstone:         false,
				},
			},
			editEntries: []*model.WorkspaceEntry{
				{
					Path:              "no_file",
					EntryName:         pstr("no_file"),
					EntryAddress:      pstr("123456789"),
					EntryType:         pstr(model.EntryTypeObject),
					EntryCreationDate: &n,
					Tombstone:         true,
				},
			},

			wantedWS: []*model.WorkspaceEntry{
				{
					Path:              "file1",
					EntryName:         pstr("file1"),
					EntryAddress:      pstr("123456789"),
					EntryType:         pstr(model.EntryTypeObject),
					EntryCreationDate: &n,
					Tombstone:         false,
				},
			},
			wantErr: false,
		},
		{
			name: "remove tree",

			initialWS: []*model.WorkspaceEntry{
				{
					Path:              "a/file2",
					EntryName:         pstr("file2"),
					EntryAddress:      pstr("123456789"),
					EntryType:         pstr(model.EntryTypeObject),
					EntryCreationDate: &n,
					Tombstone:         false,
				},
				{
					Path:              "a/file3",
					EntryName:         pstr("file3"),
					EntryAddress:      pstr("123456789"),
					EntryType:         pstr(model.EntryTypeObject),
					EntryCreationDate: &n,
					Tombstone:         false,
				},
				{
					Path:              "file1",
					EntryName:         pstr("file1"),
					EntryAddress:      pstr("123456789"),
					EntryType:         pstr(model.EntryTypeObject),
					EntryCreationDate: &n,
					Tombstone:         false,
				},
				{
					Path:              "file4",
					EntryName:         pstr("file4"),
					EntryAddress:      pstr("123456789"),
					EntryType:         pstr(model.EntryTypeObject),
					EntryCreationDate: &n,
					Tombstone:         false,
				},
			},
			editEntries: []*model.WorkspaceEntry{
				{
					Path:              "a/",
					EntryName:         pstr("a/"),
					EntryType:         pstr(model.EntryTypeTree),
					EntryCreationDate: &n,
					Tombstone:         true,
				},
			},

			wantedWS: []*model.WorkspaceEntry{
				{
					Path:              "file1",
					EntryName:         pstr("file1"),
					EntryAddress:      pstr("123456789"),
					EntryType:         pstr(model.EntryTypeObject),
					EntryCreationDate: &n,
					Tombstone:         false,
				},
				{
					Path:              "file4",
					EntryName:         pstr("file4"),
					EntryAddress:      pstr("123456789"),
					EntryType:         pstr(model.EntryTypeObject),
					EntryCreationDate: &n,
					Tombstone:         false,
				},
			},
		},
		{
			name: "remove non existing tree",

			initialWS: []*model.WorkspaceEntry{
				{
					Path:              "file1",
					EntryName:         pstr("file1"),
					EntryAddress:      pstr("123456789"),
					EntryType:         pstr(model.EntryTypeObject),
					EntryCreationDate: &n,
					Tombstone:         false,
				},
			},
			editEntries: []*model.WorkspaceEntry{
				{
					Path:              "a/",
					EntryName:         pstr("a/"),
					EntryAddress:      pstr("a/"),
					EntryType:         pstr(model.EntryTypeTree),
					EntryCreationDate: &n,
					Tombstone:         true,
				},
			},
			wantedWS: []*model.WorkspaceEntry{
				{
					Path:              "file1",
					EntryName:         pstr("file1"),
					EntryAddress:      pstr("123456789"),
					EntryType:         pstr(model.EntryTypeObject),
					EntryCreationDate: &n,
					Tombstone:         false,
				},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tree := testutil.ConstructTree(map[string][]*model.Entry{
				"master": {},
			})
			m := merkle.New("master")
			initialMerkle, err := m.Update(tree, tt.initialWS)
			if err != nil {
				t.Fatal(err)
				return
			}
			got, err := initialMerkle.Update(tree, tt.editEntries)
			if err != nil {
				if !tt.wantErr {
					// wanted errors should only be on editEntries
					t.Fatal(err)
				}
				return
			}
			wantTree := testutil.ConstructTree(map[string][]*model.Entry{
				"master": {},
			})
			m2 := merkle.New("master")
			want, err := m2.Update(wantTree, tt.wantedWS)
			if err != nil {
				t.Fatal(err)
				return
			}

			if tt.wantErr {
				t.Errorf("Update() expected an error but didn't get one")
				return
			}
			if !reflect.DeepEqual(got, want) {
				t.Errorf("Update() got = %v, want %v", got, want)
			}
		})
	}
}
