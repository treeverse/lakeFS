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
				{Name: "act1", On: actions.OnEvents{PreCommit: &actions.ActionOn{}}},
				{Name: "act2", On: actions.OnEvents{PreCommit: &actions.ActionOn{}}},
			},
			spec: actions.MatchSpec{
				EventType: actions.EventTypePreCommit,
			},
			want: []*actions.Action{
				{Name: "act1", On: actions.OnEvents{PreCommit: &actions.ActionOn{}}},
				{Name: "act2", On: actions.OnEvents{PreCommit: &actions.ActionOn{}}},
			},
			wantErr: false,
		},
		{
			name: "none",
			actions: []*actions.Action{
				{Name: "act1", On: actions.OnEvents{PreCommit: &actions.ActionOn{}}},
				{Name: "act2", On: actions.OnEvents{PreCommit: &actions.ActionOn{}}},
			},
			spec: actions.MatchSpec{
				EventType: actions.EventTypePreMerge,
			},
			want:    nil,
			wantErr: false,
		},
		{
			name: "one",
			actions: []*actions.Action{
				{Name: "act1", On: actions.OnEvents{PreCommit: &actions.ActionOn{}}},
				{Name: "act2", On: actions.OnEvents{PreMerge: &actions.ActionOn{}}},
			},
			spec: actions.MatchSpec{
				EventType: actions.EventTypePreMerge,
			},
			want: []*actions.Action{
				{Name: "act2", On: actions.OnEvents{PreMerge: &actions.ActionOn{}}},
			},
			wantErr: false,
		},
		{
			name: "invalid",
			actions: []*actions.Action{
				{Name: "act1", On: actions.OnEvents{PreCommit: &actions.ActionOn{}}},
				{Name: "act2", On: actions.OnEvents{PreMerge: &actions.ActionOn{Branches: []string{"\\"}}}},
			},
			spec: actions.MatchSpec{
				EventType: actions.EventTypePreMerge,
				Branch:    "main",
			},
			want:    nil,
			wantErr: true,
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
