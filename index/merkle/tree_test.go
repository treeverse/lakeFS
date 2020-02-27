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
	GetType() model.Entry_Type
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
		Name:      "hooks",
		Address:   "abc",
		Type:      model.Entry_OBJECT,
		Timestamp: time.Now().Unix(),
		Size:      300,
	}, &model.Entry{
		Name:      "hooks",
		Address:   "def",
		Type:      model.Entry_OBJECT,
		Timestamp: time.Now().Unix(),
		Size:      300,
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
		typ model.Entry_Type
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
						{Name: "file1", Address: "file1", Type: model.Entry_OBJECT},
					},
				}),
				pth: "file1",
				typ: model.Entry_OBJECT,
			},
			want: &model.Entry{
				Name:    "file1",
				Address: "file1",
				Type:    model.Entry_OBJECT,
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
						{Name: "dir/", Address: "dir/", Type: model.Entry_TREE},
					},
					"dir/": {
						{Name: "file1", Address: "file1", Type: model.Entry_OBJECT},
					},
				}),
				pth: "dir/file1",
				typ: model.Entry_OBJECT,
			},
			want: &model.Entry{
				Name:    "file1",
				Address: "file1",
				Type:    model.Entry_OBJECT,
			},
			wantErr: false,
		},
		{
			name: "read tree from root ",
			fields: fields{
				root: "root",
			},
			args: args{
				tx: testutil.ConstructTree(map[string][]*model.Entry{
					"root": {
						{Name: "dir/", Address: "dir/", Type: model.Entry_TREE},
					},
					"dir/": {
						{Name: "file1", Address: "file1", Type: model.Entry_OBJECT},
					},
				}),
				pth: "dir/",
				typ: model.Entry_TREE,
			},
			want: &model.Entry{
				Name:    "dir/",
				Address: "dir/",
				Type:    model.Entry_TREE,
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
						{Name: "dir/", Address: "dir/", Type: model.Entry_TREE},
					},
				}),
				pth: "file2",
				typ: model.Entry_OBJECT,
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
						{Name: "file1", Address: "file1", Type: model.Entry_OBJECT},
						{Name: "file2", Address: "file2", Type: model.Entry_OBJECT},
					},
				}),
				pth: "",
			},
			want: []*model.Entry{
				{
					Name:    "file1",
					Address: "file1",
					Type:    model.Entry_OBJECT,
				}, {
					Name:    "file2",
					Address: "file2",
					Type:    model.Entry_OBJECT,
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
						{Name: "dir", Address: "dir", Type: model.Entry_TREE},
					},
					"dir": {
						{Name: "file1", Address: "file1", Type: model.Entry_OBJECT},
						{Name: "file2", Address: "file2", Type: model.Entry_OBJECT},
						{Name: "file3", Address: "file3", Type: model.Entry_OBJECT},
					},
				}),
				pth: "dir",
			},
			want: []*model.Entry{
				{
					Name:    "file1",
					Address: "file1",
					Type:    model.Entry_OBJECT,
				},
				{
					Name:    "file2",
					Address: "file2",
					Type:    model.Entry_OBJECT,
				},
				{
					Name:    "file3",
					Address: "file3",
					Type:    model.Entry_OBJECT,
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
						{Name: "dir", Address: "dir", Type: model.Entry_TREE},
					},
					"dir": {
						{Name: "file1", Address: "file1", Type: model.Entry_OBJECT},
						{Name: "file2", Address: "file2", Type: model.Entry_OBJECT},
						{Name: "dir2", Address: "dir2", Type: model.Entry_TREE},
					},
					"dir2": {
						{Name: "file4", Address: "file4", Type: model.Entry_OBJECT},
					},
				}),
				pth: "dir",
			},
			want: []*model.Entry{
				{
					Name:    "dir2",
					Address: "dir2",
					Type:    model.Entry_TREE,
				},
				{
					Name:    "file1",
					Address: "file1",
					Type:    model.Entry_OBJECT,
				},
				{
					Name:    "file2",
					Address: "file2",
					Type:    model.Entry_OBJECT,
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

func TestMerkle_Update(t *testing.T) {

	tests := []struct {
		name        string
		initialWS   []*model.WorkspaceEntry
		editEntries []*model.WorkspaceEntry
		wantedWS    []*model.WorkspaceEntry
	}{
		{
			name: "add one objects to root",

			initialWS: []*model.WorkspaceEntry{
				{
					Path:      "",
					Entry:     &model.Entry{Name: "file1", Address: "123456789", Type: model.Entry_OBJECT},
					Tombstone: false,
				},
			},
			editEntries: []*model.WorkspaceEntry{
				{
					Path:      "",
					Entry:     &model.Entry{Name: "file2", Address: "123456789", Type: model.Entry_OBJECT},
					Tombstone: false,
				},
			},

			wantedWS: []*model.WorkspaceEntry{
				{
					Path:      "",
					Entry:     &model.Entry{Name: "file1", Address: "123456789", Type: model.Entry_OBJECT},
					Tombstone: false,
				},
				{
					Path:      "",
					Entry:     &model.Entry{Name: "file2", Address: "123456789", Type: model.Entry_OBJECT},
					Tombstone: false,
				},
			},
		},
		{
			name: "add objects to root and objects to tree",

			initialWS: []*model.WorkspaceEntry{
				{
					Path:      "a/",
					Entry:     &model.Entry{Name: "file1", Address: "123456789", Type: model.Entry_OBJECT},
					Tombstone: false,
				},
			},
			editEntries: []*model.WorkspaceEntry{
				{
					Path:      "",
					Entry:     &model.Entry{Name: "file2", Address: "123456789", Type: model.Entry_OBJECT},
					Tombstone: false,
				},
				{
					Path:      "a/",
					Entry:     &model.Entry{Name: "file3", Address: "123456789", Type: model.Entry_OBJECT},
					Tombstone: false,
				},
			},

			wantedWS: []*model.WorkspaceEntry{
				{
					Path:      "a/",
					Entry:     &model.Entry{Name: "file1", Address: "123456789", Type: model.Entry_OBJECT},
					Tombstone: false,
				},
				{
					Path:      "",
					Entry:     &model.Entry{Name: "file2", Address: "123456789", Type: model.Entry_OBJECT},
					Tombstone: false,
				},
				{
					Path:      "a/",
					Entry:     &model.Entry{Name: "file3", Address: "123456789", Type: model.Entry_OBJECT},
					Tombstone: false,
				},
			},
		},
		{
			name: "add object to tree",

			initialWS: []*model.WorkspaceEntry{

				{
					Path:      "a/",
					Entry:     &model.Entry{Name: "file1", Address: "123456789", Type: model.Entry_OBJECT},
					Tombstone: false,
				},
				{
					Path:      "",
					Entry:     &model.Entry{Name: "file2", Address: "123456789", Type: model.Entry_OBJECT},
					Tombstone: false,
				},
			},
			editEntries: []*model.WorkspaceEntry{

				{
					Path:      "a/",
					Entry:     &model.Entry{Name: "file3", Address: "123456789", Type: model.Entry_OBJECT},
					Tombstone: false,
				},
			},

			wantedWS: []*model.WorkspaceEntry{
				{
					Path:      "a/",
					Entry:     &model.Entry{Name: "file1", Address: "123456789", Type: model.Entry_OBJECT},
					Tombstone: false,
				},
				{
					Path:      "",
					Entry:     &model.Entry{Name: "file2", Address: "123456789", Type: model.Entry_OBJECT},
					Tombstone: false,
				},
				{
					Path:      "a/",
					Entry:     &model.Entry{Name: "file3", Address: "123456789", Type: model.Entry_OBJECT},
					Tombstone: false,
				},
			},
		},
		{
			name: "add object to deep tree",

			initialWS: []*model.WorkspaceEntry{

				{
					Path:      "a/",
					Entry:     &model.Entry{Name: "file1", Address: "123456789", Type: model.Entry_OBJECT},
					Tombstone: false,
				},
				{
					Path:      "a/z/",
					Entry:     &model.Entry{Name: "file2", Address: "123456789", Type: model.Entry_OBJECT},
					Tombstone: false,
				},
				{
					Path:      "a/b/",
					Entry:     &model.Entry{Name: "file3", Address: "123456789", Type: model.Entry_OBJECT},
					Tombstone: false,
				},
			},
			editEntries: []*model.WorkspaceEntry{

				{
					Path:      "a/z/",
					Entry:     &model.Entry{Name: "file4", Address: "123456789", Type: model.Entry_OBJECT},
					Tombstone: false,
				},
				{
					Path:      "a/b/",
					Entry:     &model.Entry{Name: "file5", Address: "123456789", Type: model.Entry_OBJECT},
					Tombstone: false,
				},
			},

			wantedWS: []*model.WorkspaceEntry{
				{
					Path:      "a/",
					Entry:     &model.Entry{Name: "file1", Address: "123456789", Type: model.Entry_OBJECT},
					Tombstone: false,
				},
				{
					Path:      "a/z/",
					Entry:     &model.Entry{Name: "file2", Address: "123456789", Type: model.Entry_OBJECT},
					Tombstone: false,
				},
				{
					Path:      "a/b/",
					Entry:     &model.Entry{Name: "file3", Address: "123456789", Type: model.Entry_OBJECT},
					Tombstone: false,
				},
				{
					Path:      "a/z/",
					Entry:     &model.Entry{Name: "file4", Address: "123456789", Type: model.Entry_OBJECT},
					Tombstone: false,
				},
				{
					Path:      "a/b/",
					Entry:     &model.Entry{Name: "file5", Address: "123456789", Type: model.Entry_OBJECT},
					Tombstone: false,
				},
			},
		},
		{
			name: "remove one objects from root",

			initialWS: []*model.WorkspaceEntry{
				{
					Path:      "a/",
					Entry:     &model.Entry{Name: "file1", Address: "123456789", Type: model.Entry_OBJECT},
					Tombstone: false,
				},
				{
					Path:      "",
					Entry:     &model.Entry{Name: "file_remove", Address: "123456788", Type: model.Entry_OBJECT},
					Tombstone: false,
				},
			},
			editEntries: []*model.WorkspaceEntry{
				{
					Path:      "",
					Entry:     &model.Entry{Name: "file_remove", Address: "123456788", Type: model.Entry_OBJECT},
					Tombstone: true,
				},
			},

			wantedWS: []*model.WorkspaceEntry{
				{
					Path:      "a/",
					Entry:     &model.Entry{Name: "file1", Address: "123456789", Type: model.Entry_OBJECT},
					Tombstone: false,
				},
			},
		},
		{
			name: "remove one objects from tree with other objects",

			initialWS: []*model.WorkspaceEntry{
				{
					Path:      "a/",
					Entry:     &model.Entry{Name: "file1", Address: "123456789", Type: model.Entry_OBJECT},
					Tombstone: false,
				},
				{
					Path:      "a/",
					Entry:     &model.Entry{Name: "file_remove", Address: "123456788", Type: model.Entry_OBJECT},
					Tombstone: false,
				},
			},
			editEntries: []*model.WorkspaceEntry{
				{
					Path:      "a/",
					Entry:     &model.Entry{Name: "file_remove", Address: "123456788", Type: model.Entry_OBJECT},
					Tombstone: true,
				},
			},

			wantedWS: []*model.WorkspaceEntry{
				{
					Path:      "a/",
					Entry:     &model.Entry{Name: "file1", Address: "123456789", Type: model.Entry_OBJECT},
					Tombstone: false,
				},
			},
		},
		{
			name: "remove one file from tree with no other files",

			initialWS: []*model.WorkspaceEntry{
				{
					Path:      "",
					Entry:     &model.Entry{Name: "file1", Address: "123456789", Type: model.Entry_OBJECT},
					Tombstone: false,
				},
				{
					Path:      "a/",
					Entry:     &model.Entry{Name: "file_remove", Address: "123456788", Type: model.Entry_OBJECT},
					Tombstone: false,
				},
			},
			editEntries: []*model.WorkspaceEntry{
				{
					Path:      "a/",
					Entry:     &model.Entry{Name: "file_remove", Address: "123456788", Type: model.Entry_OBJECT},
					Tombstone: true,
				},
			},

			wantedWS: []*model.WorkspaceEntry{
				{
					Path:      "",
					Entry:     &model.Entry{Name: "file1", Address: "123456789", Type: model.Entry_OBJECT},
					Tombstone: false,
				},
			},
		},
		{
			name: "remove non existing object",

			initialWS: []*model.WorkspaceEntry{
				{
					Path:      "",
					Entry:     &model.Entry{Name: "file1", Address: "123456789", Type: model.Entry_OBJECT},
					Tombstone: false,
				},
			},
			editEntries: []*model.WorkspaceEntry{
				{
					Path:      "",
					Entry:     &model.Entry{Name: "no_file", Address: "123456789", Type: model.Entry_OBJECT},
					Tombstone: true,
				},
			},

			wantedWS: []*model.WorkspaceEntry{
				{
					Path:      "",
					Entry:     &model.Entry{Name: "file1", Address: "123456789", Type: model.Entry_OBJECT},
					Tombstone: false,
				},
			},
		},
		{
			name: "remove tree ",

			initialWS: []*model.WorkspaceEntry{
				{
					Path:      "a/",
					Entry:     &model.Entry{Name: "file2", Address: "123456789", Type: model.Entry_OBJECT},
					Tombstone: false,
				},
				{
					Path:      "a/",
					Entry:     &model.Entry{Name: "file3", Address: "123456789", Type: model.Entry_OBJECT},
					Tombstone: false,
				},
				{
					Path:      "",
					Entry:     &model.Entry{Name: "file1", Address: "123456789", Type: model.Entry_OBJECT},
					Tombstone: false,
				},
				{
					Path:      "",
					Entry:     &model.Entry{Name: "file4", Address: "123456789", Type: model.Entry_OBJECT},
					Tombstone: false,
				},
			},
			editEntries: []*model.WorkspaceEntry{
				{
					Path:      "a/",
					Entry:     &model.Entry{Name: "a/", Type: model.Entry_TREE},
					Tombstone: true,
				},
			},

			wantedWS: []*model.WorkspaceEntry{
				{
					Path:      "",
					Entry:     &model.Entry{Name: "file1", Address: "123456789", Type: model.Entry_OBJECT},
					Tombstone: false,
				},
				{
					Path:      "",
					Entry:     &model.Entry{Name: "file4", Address: "123456789", Type: model.Entry_OBJECT},
					Tombstone: false,
				},
			},
		},
		{
			name: "remove non existing tree",

			initialWS: []*model.WorkspaceEntry{
				{
					Path:      "",
					Entry:     &model.Entry{Name: "file1", Address: "123456789", Type: model.Entry_OBJECT},
					Tombstone: false,
				},
			},
			editEntries: []*model.WorkspaceEntry{
				{
					Path:      "a/",
					Entry:     &model.Entry{Name: "a/", Address: "a/", Type: model.Entry_TREE},
					Tombstone: true,
				},
			},
			wantedWS: []*model.WorkspaceEntry{
				{
					Path:      "",
					Entry:     &model.Entry{Name: "file1", Address: "123456789", Type: model.Entry_OBJECT},
					Tombstone: false,
				},
			},
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
				t.Fatal(err)
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
			if !reflect.DeepEqual(got, want) {
				t.Errorf("Update() got = %v, want %v", got, want)
			}
		})
	}
}
