package actions_test

import (
	"context"
	"errors"
	"io/ioutil"
	"path"
	"testing"

	"github.com/go-test/deep"
	"github.com/golang/mock/gomock"
	"github.com/treeverse/lakefs/actions"
	"github.com/treeverse/lakefs/actions/mock"
	"gopkg.in/yaml.v3"
)

func TestAction_ReadAction(t *testing.T) {
	tests := []struct {
		name     string
		filename string
		wantErr  bool
	}{
		{name: "full", filename: "action_full.yaml", wantErr: false},
		{name: "required", filename: "action_required.yaml", wantErr: false},
		{name: "duplicate id", filename: "action_duplicate_id.yaml", wantErr: true},
		{name: "invalid id", filename: "action_invalid_id.yaml", wantErr: true},
		{name: "invalid hook type", filename: "action_invalid_type.yaml", wantErr: true},
		{name: "invalid yaml", filename: "action_invalid_yaml.yaml", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data, err := ioutil.ReadFile(path.Join("testdata", tt.filename))
			if err != nil {
				t.Fatalf("Failed to load testdata %s, err=%s", tt.filename, err)
			}
			act, err := actions.ParseAction(data)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseAction() error = %v, wantErr %t", err, tt.wantErr)
			}
			if err == nil && act == nil {
				t.Error("ParseAction() no error, missing Action")
			}
		})
	}
}

func TestAction_Match(t *testing.T) {
	tests := []struct {
		name    string
		on      actions.OnEvents
		spec    actions.MatchSpec
		want    bool
		wantErr bool
	}{
		{
			name:    "none - on pre-merge without branch",
			on:      actions.OnEvents{},
			spec:    actions.MatchSpec{EventType: actions.EventTypePreMerge},
			want:    false,
			wantErr: false,
		},
		{
			name:    "none - on invalid event type",
			on:      actions.OnEvents{},
			spec:    actions.MatchSpec{EventType: "nothing"},
			want:    false,
			wantErr: true,
		},
		{
			name:    "pre-merge - on pre-merge without branch",
			on:      actions.OnEvents{PreMerge: &actions.ActionOn{}},
			spec:    actions.MatchSpec{EventType: actions.EventTypePreMerge},
			want:    true,
			wantErr: false,
		},
		{
			name:    "pre-merge - on pre-commit without branch",
			on:      actions.OnEvents{PreMerge: &actions.ActionOn{}},
			spec:    actions.MatchSpec{EventType: actions.EventTypePreCommit},
			want:    false,
			wantErr: false,
		},
		{
			name:    "pre-commit - on pre-merge without branch",
			on:      actions.OnEvents{PreCommit: &actions.ActionOn{}},
			spec:    actions.MatchSpec{EventType: actions.EventTypePreMerge},
			want:    false,
			wantErr: false,
		},
		{
			name:    "pre-commit - on pre-commit without branch",
			on:      actions.OnEvents{PreCommit: &actions.ActionOn{}},
			spec:    actions.MatchSpec{EventType: actions.EventTypePreCommit},
			want:    true,
			wantErr: false,
		},
		{
			name:    "both - on pre-commit without branch",
			on:      actions.OnEvents{PreCommit: &actions.ActionOn{}, PreMerge: &actions.ActionOn{}},
			spec:    actions.MatchSpec{EventType: actions.EventTypePreCommit},
			want:    true,
			wantErr: false,
		},
		{
			name:    "both - on pre-merge without branch",
			on:      actions.OnEvents{PreCommit: &actions.ActionOn{}, PreMerge: &actions.ActionOn{}},
			spec:    actions.MatchSpec{EventType: actions.EventTypePreMerge},
			want:    true,
			wantErr: false,
		},
		{
			name:    "pre-commit master - on pre-commit master",
			on:      actions.OnEvents{PreCommit: &actions.ActionOn{Branches: []string{"master"}}},
			spec:    actions.MatchSpec{EventType: actions.EventTypePreCommit, Branch: "master"},
			want:    true,
			wantErr: false,
		},
		{
			name:    "pre-commit master - on pre-commit masterer",
			on:      actions.OnEvents{PreCommit: &actions.ActionOn{Branches: []string{"master"}}},
			spec:    actions.MatchSpec{EventType: actions.EventTypePreCommit, Branch: "masterer"},
			want:    false,
			wantErr: false,
		},
		{
			name:    "pre-commit ends with feature - on pre-commit new-feature",
			on:      actions.OnEvents{PreCommit: &actions.ActionOn{Branches: []string{"*-feature"}}},
			spec:    actions.MatchSpec{EventType: actions.EventTypePreCommit, Branch: "new-feature"},
			want:    true,
			wantErr: false,
		},
		{
			name:    "pre-commit branch a1 or b1 - on pre-commit b1",
			on:      actions.OnEvents{PreCommit: &actions.ActionOn{Branches: []string{"a1", "b1"}}},
			spec:    actions.MatchSpec{EventType: actions.EventTypePreCommit, Branch: "b1"},
			want:    true,
			wantErr: false,
		},
		{
			name:    "pre-commit branch invalid - on pre-commit master",
			on:      actions.OnEvents{PreCommit: &actions.ActionOn{Branches: []string{"\\"}}},
			spec:    actions.MatchSpec{EventType: actions.EventTypePreCommit, Branch: "master"},
			want:    false,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := &actions.Action{
				Name: tt.name,
				On:   tt.on,
			}
			got, err := a.Match(tt.spec)
			if (err != nil) != tt.wantErr {
				t.Errorf("Match() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("Match() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestLoadActions(t *testing.T) {
	tests := []struct {
		name            string
		configureSource func(*gomock.Controller) actions.Source
		want            []*actions.Action
		wantErr         bool
	}{
		{
			name: "listing fails",
			configureSource: func(ctrl *gomock.Controller) actions.Source {
				source := mock.NewMockSource(ctrl)
				source.EXPECT().List(gomock.Any()).Return(nil, errors.New("failed"))
				return source
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "load fails",
			configureSource: func(ctrl *gomock.Controller) actions.Source {
				source := mock.NewMockSource(ctrl)
				ref := actions.FileRef{Path: "one-path", Address: "one-addr"}
				source.EXPECT().List(gomock.Any()).Return([]actions.FileRef{ref}, nil)
				source.EXPECT().Load(gomock.Any(), gomock.Eq(ref)).Return(nil, errors.New("failed"))
				return source
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "first load fails, second succeed",
			configureSource: func(ctrl *gomock.Controller) actions.Source {
				source := mock.NewMockSource(ctrl)
				ref1 := actions.FileRef{Path: "path_1", Address: "addr_1"}
				ref2 := actions.FileRef{Path: "path_2", Address: "addr_2"}
				source.EXPECT().List(gomock.Any()).Return([]actions.FileRef{ref1, ref2}, nil)
				source.EXPECT().Load(gomock.Any(), gomock.Eq(ref1)).Return(yaml.Marshal(actions.Action{
					Name: "some-action",
					On: actions.OnEvents{
						PreCommit: &actions.ActionOn{Branches: []string{"master"}},
					},
					Hooks: []actions.ActionHook{
						{
							ID:   "hook_id",
							Type: "webhook",
						},
					},
				}))
				source.EXPECT().Load(gomock.Any(), gomock.Eq(ref2)).Return(nil, errors.New("failed"))
				return source
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "load success",
			configureSource: func(ctrl *gomock.Controller) actions.Source {
				source := mock.NewMockSource(ctrl)
				ref1 := actions.FileRef{Path: "path_1", Address: "addr_1"}
				source.EXPECT().List(gomock.Any()).Return([]actions.FileRef{ref1}, nil)
				source.EXPECT().Load(gomock.Any(), gomock.Eq(ref1)).Return(yaml.Marshal(actions.Action{
					Name: "some-action",
					On: actions.OnEvents{
						PreCommit: &actions.ActionOn{Branches: []string{"master"}},
					},
					Hooks: []actions.ActionHook{
						{
							ID:   "hook_id_1",
							Type: "webhook",
						},
						{
							ID:   "hook_id_2",
							Type: "webhook",
						},
					},
				}))
				return source
			},
			want: []*actions.Action{
				{
					Name: "some-action",
					On: actions.OnEvents{
						PreCommit: &actions.ActionOn{Branches: []string{"master"}},
					},
					Hooks: []actions.ActionHook{
						{
							ID:         "hook_id_1",
							Type:       "webhook",
							Properties: map[string]string{},
						},
						{
							ID:         "hook_id_2",
							Type:       "webhook",
							Properties: map[string]string{},
						},
					},
				},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()
			source := tt.configureSource(ctrl)

			ctx := context.Background()
			res, err := actions.LoadActions(ctx, source)
			if (err != nil) != tt.wantErr {
				t.Errorf("LoadActions() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if diff := deep.Equal(res, tt.want); diff != nil {
				t.Error("LoadActions() found diff", diff)
			}
		})
	}
}
