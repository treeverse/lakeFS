package actions_test

import (
	"context"
	"errors"
	"os"
	"path"
	"testing"

	"github.com/go-test/deep"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/actions"
	"github.com/treeverse/lakefs/pkg/actions/mock"
	"github.com/treeverse/lakefs/pkg/graveler"
	"gopkg.in/yaml.v3"
)

func TestAction_ReadAction(t *testing.T) {
	tests := []struct {
		name     string
		filename string
		errStr   string
		validate func(*testing.T, *actions.Action)
	}{
		{name: "full", filename: "action_full.yaml", validate: validateActionFull},
		{name: "secrets", filename: "action_secrets.yaml"},
		{name: "required", filename: "action_required.yaml"},
		{name: "duplicate id", filename: "action_duplicate_id.yaml", errStr: "duplicate ID"},
		{name: "invalid id", filename: "action_invalid_id.yaml", errStr: "missing ID: invalid action"},
		{name: "invalid hook type", filename: "action_invalid_type.yaml", errStr: "type 'no_temp' unknown: invalid action"},
		{name: "invalid event type", filename: "action_invalid_event.yaml", errStr: "event 'not-a-valid-event' is not supported: invalid action"},
		{name: "invalid yaml", filename: "action_invalid_yaml.yaml", errStr: "yaml: unmarshal errors"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data, err := os.ReadFile(path.Join("testdata", tt.filename))
			if err != nil {
				t.Fatalf("Failed to load testdata %s, err=%s", tt.filename, err)
			}
			act, err := actions.ParseAction(data)
			require.Equal(t, err != nil, tt.errStr != "")
			if err != nil {
				require.Contains(t, err.Error(), tt.errStr)
			}
			if err == nil && act == nil {
				t.Error("ParseAction() no error, missing Action")
			}
			if tt.validate != nil {
				tt.validate(t, act)
			}
		})
	}
}

func validateActionFull(t *testing.T, act *actions.Action) {
	t.Helper()
	require.Contains(t, act.On, graveler.EventTypePreMerge)
	require.Contains(t, act.On, graveler.EventTypePreCommit)
	require.Contains(t, act.On, graveler.EventTypePostCommit)
	require.NotContains(t, act.On, graveler.EventTypePostMerge)
}

func TestAction_Match(t *testing.T) {
	tests := []struct {
		name    string
		on      map[graveler.EventType]*actions.ActionOn
		spec    actions.MatchSpec
		want    bool
		wantErr bool
	}{
		{
			name:    "none - on pre-merge without branch",
			on:      map[graveler.EventType]*actions.ActionOn{},
			spec:    actions.MatchSpec{EventType: graveler.EventTypePreMerge},
			want:    false,
			wantErr: false,
		},
		{
			name:    "pre-merge - on pre-merge without branch",
			on:      map[graveler.EventType]*actions.ActionOn{graveler.EventTypePreMerge: {}},
			spec:    actions.MatchSpec{EventType: graveler.EventTypePreMerge},
			want:    true,
			wantErr: false,
		},
		{
			name:    "pre-merge - on pre-commit without branch",
			on:      map[graveler.EventType]*actions.ActionOn{graveler.EventTypePreMerge: {}},
			spec:    actions.MatchSpec{EventType: graveler.EventTypePreCommit},
			want:    false,
			wantErr: false,
		},
		{
			name:    "pre-commit - on pre-merge without branch",
			on:      map[graveler.EventType]*actions.ActionOn{graveler.EventTypePreCommit: {}},
			spec:    actions.MatchSpec{EventType: graveler.EventTypePreMerge},
			want:    false,
			wantErr: false,
		},
		{
			name:    "pre-commit - on pre-commit without branch",
			on:      map[graveler.EventType]*actions.ActionOn{graveler.EventTypePreCommit: {}},
			spec:    actions.MatchSpec{EventType: graveler.EventTypePreCommit},
			want:    true,
			wantErr: false,
		},
		{
			name: "both - on pre-commit without branch",
			on: map[graveler.EventType]*actions.ActionOn{
				graveler.EventTypePreCommit: {},
				graveler.EventTypePreMerge:  {},
			},
			spec:    actions.MatchSpec{EventType: graveler.EventTypePreCommit},
			want:    true,
			wantErr: false,
		},
		{
			name: "both - on pre-merge without branch",
			on: map[graveler.EventType]*actions.ActionOn{
				graveler.EventTypePreCommit: {},
				graveler.EventTypePreMerge:  {},
			},
			spec:    actions.MatchSpec{EventType: graveler.EventTypePreMerge},
			want:    true,
			wantErr: false,
		},
		{
			name: "pre-commit main - on pre-commit main",
			on: map[graveler.EventType]*actions.ActionOn{
				graveler.EventTypePreCommit: {Branches: []string{"main"}},
			},
			spec:    actions.MatchSpec{EventType: graveler.EventTypePreCommit, BranchID: "main"},
			want:    true,
			wantErr: false,
		},
		{
			name: "pre-commit main - on pre-commit x",
			on: map[graveler.EventType]*actions.ActionOn{
				graveler.EventTypePreCommit: {Branches: []string{"main"}},
			},
			spec:    actions.MatchSpec{EventType: graveler.EventTypePreCommit, BranchID: "x"},
			want:    false,
			wantErr: false,
		},
		{
			name: "pre-commit ends with feature - on pre-commit new-feature",
			on: map[graveler.EventType]*actions.ActionOn{
				graveler.EventTypePreCommit: {Branches: []string{"*-feature"}},
			},
			spec:    actions.MatchSpec{EventType: graveler.EventTypePreCommit, BranchID: "new-feature"},
			want:    true,
			wantErr: false,
		},
		{
			name: "pre-commit branch a1 or b1 - on pre-commit b1",
			on: map[graveler.EventType]*actions.ActionOn{
				graveler.EventTypePreCommit: {Branches: []string{"a1", "b1"}},
			},
			spec:    actions.MatchSpec{EventType: graveler.EventTypePreCommit, BranchID: "b1"},
			want:    true,
			wantErr: false,
		},
		{
			name: "pre-commit branch invalid - on pre-commit main",
			on: map[graveler.EventType]*actions.ActionOn{
				graveler.EventTypePreCommit: {Branches: []string{"\\"}},
			},
			spec:    actions.MatchSpec{EventType: graveler.EventTypePreCommit, BranchID: "main"},
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
				source.EXPECT().List(gomock.Any(), gomock.Any()).Return(nil, errors.New("failed"))
				return source
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "load fails",
			configureSource: func(ctrl *gomock.Controller) actions.Source {
				source := mock.NewMockSource(ctrl)
				source.EXPECT().List(gomock.Any(), gomock.Any()).Return([]string{"one"}, nil)
				source.EXPECT().Load(gomock.Any(), gomock.Any(), gomock.Eq("one")).Return(nil, errors.New("failed"))
				return source
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "first load fails, second succeed",
			configureSource: func(ctrl *gomock.Controller) actions.Source {
				source := mock.NewMockSource(ctrl)
				const ref1 = "path_1"
				const ref2 = "path_2"
				source.EXPECT().List(gomock.Any(), gomock.Any()).Return([]string{ref1, ref2}, nil)
				source.EXPECT().Load(gomock.Any(), gomock.Any(), gomock.Eq(ref1)).Return(yaml.Marshal(actions.Action{
					Name: "some-action",
					On: map[graveler.EventType]*actions.ActionOn{
						graveler.EventTypePreCommit: {Branches: []string{"main"}},
					},
					Hooks: []actions.ActionHook{
						{
							ID:   "hook_id",
							Type: "webhook",
						},
					},
				}))
				source.EXPECT().Load(gomock.Any(), gomock.Any(), gomock.Eq(ref2)).Return(nil, errors.New("failed"))
				return source
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "load success",
			configureSource: func(ctrl *gomock.Controller) actions.Source {
				source := mock.NewMockSource(ctrl)
				const ref1 = "path_1"
				source.EXPECT().List(gomock.Any(), gomock.Any()).Return([]string{ref1}, nil)
				source.EXPECT().Load(gomock.Any(), gomock.Any(), gomock.Eq(ref1)).Return(yaml.Marshal(actions.Action{
					Name: "some-action",
					On: map[graveler.EventType]*actions.ActionOn{
						graveler.EventTypePreCommit: {Branches: []string{"main"}},
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
					On: map[graveler.EventType]*actions.ActionOn{
						graveler.EventTypePreCommit: {Branches: []string{"main"}},
					},
					Hooks: []actions.ActionHook{
						{
							ID:         "hook_id_1",
							Type:       "webhook",
							Properties: map[string]interface{}{},
						},
						{
							ID:         "hook_id_2",
							Type:       "webhook",
							Properties: map[string]interface{}{},
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

			ctx := context.Background()
			source := tt.configureSource(ctrl)
			var record graveler.HookRecord
			res, err := actions.LoadActions(ctx, source, record)
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

func TestMatchedActions(t *testing.T) {
	tests := []struct {
		name    string
		actions []*actions.Action
		spec    actions.MatchSpec
		want    []*actions.Action
		wantErr bool
	}{
		{
			name:    "empty",
			actions: nil,
			spec:    actions.MatchSpec{},
			want:    nil,
			wantErr: false,
		},
		{
			name: "all",
			actions: []*actions.Action{
				{Name: "act1", On: map[graveler.EventType]*actions.ActionOn{
					graveler.EventTypePreCommit: {},
				}},
				{Name: "act2", On: map[graveler.EventType]*actions.ActionOn{
					graveler.EventTypePreCommit: nil,
				}},
			},
			spec: actions.MatchSpec{
				EventType: graveler.EventTypePreCommit,
			},
			want: []*actions.Action{
				{Name: "act1", On: map[graveler.EventType]*actions.ActionOn{
					graveler.EventTypePreCommit: {},
				}},
				{Name: "act2", On: map[graveler.EventType]*actions.ActionOn{
					graveler.EventTypePreCommit: nil,
				}},
			},
			wantErr: false,
		},
		{
			name: "none",
			actions: []*actions.Action{
				{Name: "act1", On: map[graveler.EventType]*actions.ActionOn{
					graveler.EventTypePreCommit: {},
				}},
				{Name: "act2", On: map[graveler.EventType]*actions.ActionOn{
					graveler.EventTypePreCommit: {},
				}},
			},
			spec: actions.MatchSpec{
				EventType: graveler.EventTypePreMerge,
			},
			want:    nil,
			wantErr: false,
		},
		{
			name: "one",
			actions: []*actions.Action{
				{Name: "act1", On: map[graveler.EventType]*actions.ActionOn{
					graveler.EventTypePreCommit: {},
				}},
				{Name: "act2", On: map[graveler.EventType]*actions.ActionOn{
					graveler.EventTypePreMerge: {},
				}},
			},
			spec: actions.MatchSpec{
				EventType: graveler.EventTypePreMerge,
			},
			want: []*actions.Action{
				{Name: "act2", On: map[graveler.EventType]*actions.ActionOn{
					graveler.EventTypePreMerge: {},
				}},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := actions.MatchedActions(tt.actions, tt.spec)
			if (err != nil) != tt.wantErr {
				t.Errorf("MatchActions() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if diff := deep.Equal(got, tt.want); diff != nil {
				t.Error("MatchActions() found diff", diff)
			}
		})
	}
}
