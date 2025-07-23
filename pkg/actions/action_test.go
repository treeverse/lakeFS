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
		{name: "invalid parameter in tag event", filename: "action_invalid_param_tag_actions.yaml", errStr: "'branches' is not supported in tag event types"},
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
	require.Contains(t, act.On, graveler.EventTypePrepareCommit)
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
			name: "prepare-commit main - on prepare-commit x",
			on: map[graveler.EventType]*actions.ActionOn{
				graveler.EventTypePrepareCommit: {Branches: []string{"main"}},
			},
			spec: actions.MatchSpec{EventType: graveler.EventTypePrepareCommit, BranchID: "x"},
			want: false,
		},
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
		{
			name:    "pre create tag - on pre-create tag without branch",
			on:      map[graveler.EventType]*actions.ActionOn{graveler.EventTypePreCreateTag: {}},
			spec:    actions.MatchSpec{EventType: graveler.EventTypePreCreateTag},
			want:    true,
			wantErr: false,
		},
		{
			name:    "pre create tag main - on pre-create main",
			on:      map[graveler.EventType]*actions.ActionOn{graveler.EventTypePreCreateTag: {}},
			spec:    actions.MatchSpec{EventType: graveler.EventTypePreCreateTag, BranchID: "main"},
			want:    true,
			wantErr: false,
		},
		{
			name:    "post create tag main - on post-create main",
			on:      map[graveler.EventType]*actions.ActionOn{graveler.EventTypePostCreateTag: {}},
			spec:    actions.MatchSpec{EventType: graveler.EventTypePostCreateTag, BranchID: "main"},
			want:    true,
			wantErr: false,
		},
		{
			name:    "pre delete tag main - on pre-delete main",
			on:      map[graveler.EventType]*actions.ActionOn{graveler.EventTypePreDeleteTag: {}},
			spec:    actions.MatchSpec{EventType: graveler.EventTypePreDeleteTag, BranchID: "main"},
			want:    true,
			wantErr: false,
		},
		{
			name:    "post delete tag main - on post-delete main",
			on:      map[graveler.EventType]*actions.ActionOn{graveler.EventTypePostDeleteTag: {}},
			spec:    actions.MatchSpec{EventType: graveler.EventTypePostDeleteTag, BranchID: "main"},
			want:    true,
			wantErr: false,
		},
		{
			name:    "pre create branch main - on pre-create main",
			on:      map[graveler.EventType]*actions.ActionOn{graveler.EventTypePreCreateBranch: {Branches: []string{"main"}}},
			spec:    actions.MatchSpec{EventType: graveler.EventTypePreCreateBranch, BranchID: "main"},
			want:    true,
			wantErr: false,
		},
		{
			name:    "post create branch main - on post-create main",
			on:      map[graveler.EventType]*actions.ActionOn{graveler.EventTypePostCreateBranch: {Branches: []string{"main"}}},
			spec:    actions.MatchSpec{EventType: graveler.EventTypePostCreateBranch, BranchID: "main"},
			want:    true,
			wantErr: false,
		},
		{
			name:    "pre delete branch main - on pre-delete main",
			on:      map[graveler.EventType]*actions.ActionOn{graveler.EventTypePreDeleteBranch: {Branches: []string{"main"}}},
			spec:    actions.MatchSpec{EventType: graveler.EventTypePreDeleteBranch, BranchID: "main"},
			want:    true,
			wantErr: false,
		},
		{
			name:    "post delete branch main - on post-delete main",
			on:      map[graveler.EventType]*actions.ActionOn{graveler.EventTypePostDeleteBranch: {Branches: []string{"main"}}},
			spec:    actions.MatchSpec{EventType: graveler.EventTypePostDeleteBranch, BranchID: "main"},
			want:    true,
			wantErr: false,
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

func TestParseActions(t *testing.T) {
	tests := []struct {
		name     string
		data     string
		want     []*actions.Action
		wantErr  bool
		errStr   string
		validate func(*testing.T, []*actions.Action)
	}{
		{
			name: "single action - backward compatibility",
			data: `name: test action
on:
  pre-commit:
hooks:
  - id: test_hook
    type: lua
    properties:
      script: print("test")`,
			want: []*actions.Action{
				{
					Name: "test action",
					On: map[graveler.EventType]*actions.ActionOn{
						graveler.EventTypePreCommit: nil,
					},
					Hooks: []actions.ActionHook{
						{
							ID:   "test_hook",
							Type: "lua",
							Properties: actions.Properties{
								"script": "print(\"test\")",
							},
						},
					},
				},
			},
			wantErr: false,
		},
		{
			name: "multiple actions with --- separators",
			data: `---
name: first action
on:
  pre-commit:
hooks:
  - id: first_hook
    type: lua
    properties:
      script: print("first")
---
name: second action
on:
  post-commit:
hooks:
  - id: second_hook
    type: webhook
    properties:
      url: "http://example.com"`,
			want: []*actions.Action{
				{
					Name: "first action",
					On: map[graveler.EventType]*actions.ActionOn{
						graveler.EventTypePreCommit: nil,
					},
					Hooks: []actions.ActionHook{
						{
							ID:   "first_hook",
							Type: "lua",
							Properties: actions.Properties{
								"script": "print(\"first\")",
							},
						},
					},
				},
				{
					Name: "second action",
					On: map[graveler.EventType]*actions.ActionOn{
						graveler.EventTypePostCommit: nil,
					},
					Hooks: []actions.ActionHook{
						{
							ID:   "second_hook",
							Type: "webhook",
							Properties: actions.Properties{
								"url": "http://example.com",
							},
						},
					},
				},
			},
			wantErr: false,
		},
		{
			name: "invalid YAML",
			data: `name: test action
on:
  pre-commit:
  invalid: [`,
			wantErr: true,
			errStr:  "failed to decode YAML document",
		},
		{
			name: "one valid, one invalid action",
			data: `---
name: valid action
on:
  pre-commit:
hooks:
  - id: valid_hook
    type: lua
    properties:
      script: print("valid")
---
name: invalid action
on:
  pre-commit:
hooks:
  - id: invalid_hook
    type: invalid_type`,
			wantErr: true,
			errStr:  "invalid action 'invalid action'",
			validate: func(t *testing.T, actions []*actions.Action) {
				// Should still return the valid action even if one is invalid
				require.Len(t, actions, 1)
				require.Equal(t, "valid action", actions[0].Name)
			},
		},
		{
			name:    "empty file",
			data:    "",
			want:    nil,
			wantErr: false,
		},
		{
			name: "only --- separators",
			data: `---
---
---`,
			want:    nil,
			wantErr: false,
		},
		{
			name: "three actions with mixed event types",
			data: `---
name: pre action
on:
  pre-commit:
hooks:
  - id: pre_hook
    type: lua
    properties:
      script: print("pre")
---
name: post action
on:
  post-commit:
hooks:
  - id: post_hook
    type: webhook
    properties:
      url: "http://post.example.com"
---
name: merge action
on:
  pre-merge:
hooks:
  - id: merge_hook
    type: lua
    properties:
      script: print("merge")`,
			wantErr: false,
			validate: func(t *testing.T, actions []*actions.Action) {
				require.Len(t, actions, 3)

				// Check first action
				require.Equal(t, "pre action", actions[0].Name)
				require.Contains(t, actions[0].On, graveler.EventTypePreCommit)

				// Check second action
				require.Equal(t, "post action", actions[1].Name)
				require.Contains(t, actions[1].On, graveler.EventTypePostCommit)

				// Check third action
				require.Equal(t, "merge action", actions[2].Name)
				require.Contains(t, actions[2].On, graveler.EventTypePreMerge)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := actions.ParseActions([]byte(tt.data))

			if tt.wantErr {
				require.Error(t, err)
				if tt.errStr != "" {
					require.Contains(t, err.Error(), tt.errStr)
				}
			} else {
				require.NoError(t, err)
			}

			if tt.validate != nil {
				tt.validate(t, got)
			} else if !tt.wantErr {
				if diff := deep.Equal(got, tt.want); diff != nil {
					t.Error("ParseActions() found diff", diff)
				}
			}
		})
	}
}

func TestLoadActionsWithUnifiedFiles(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockSource := mock.NewMockSource(ctrl)

	// Test data for unified file
	unifiedFileData := `---
name: unified action 1
on:
  pre-commit:
hooks:
  - id: hook1
    type: lua
    properties:
      script: print("unified1")
---
name: unified action 2
on:
  post-commit:
hooks:
  - id: hook2
    type: webhook
    properties:
      url: "http://example.com"`

	// Test data for single action file
	singleFileData := `name: single action
on:
  pre-merge:
hooks:
  - id: single_hook
    type: lua
    properties:
      script: print("single")`

	record := graveler.HookRecord{}

	t.Run("mixed files - unified and single", func(t *testing.T) {
		mockSource.EXPECT().List(gomock.Any(), record).Return([]string{"unified.yaml", "single.yaml"}, nil)
		mockSource.EXPECT().Load(gomock.Any(), record, "unified.yaml").Return([]byte(unifiedFileData), nil)
		mockSource.EXPECT().Load(gomock.Any(), record, "single.yaml").Return([]byte(singleFileData), nil)

		actions, err := actions.LoadActions(context.Background(), mockSource, record)
		require.NoError(t, err)
		require.Len(t, actions, 3)

		// Check that all actions are loaded
		actionNames := make(map[string]bool)
		for _, action := range actions {
			actionNames[action.Name] = true
		}

		require.True(t, actionNames["unified action 1"])
		require.True(t, actionNames["unified action 2"])
		require.True(t, actionNames["single action"])
	})

	t.Run("only unified file", func(t *testing.T) {
		mockSource.EXPECT().List(gomock.Any(), record).Return([]string{"unified.yaml"}, nil)
		mockSource.EXPECT().Load(gomock.Any(), record, "unified.yaml").Return([]byte(unifiedFileData), nil)

		actions, err := actions.LoadActions(context.Background(), mockSource, record)
		require.NoError(t, err)
		require.Len(t, actions, 2)

		actionNames := make(map[string]bool)
		for _, action := range actions {
			actionNames[action.Name] = true
		}

		require.True(t, actionNames["unified action 1"])
		require.True(t, actionNames["unified action 2"])
	})
}
