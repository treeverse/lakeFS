package permissions_test

import (
	"slices"
	"testing"

	"github.com/treeverse/lakefs/pkg/permissions"
)

func TestAllActions(t *testing.T) {
	actions := permissions.Actions

	if !slices.Contains(actions, permissions.ReadUserAction) {
		t.Errorf("Expected actions %v to include %s", actions, permissions.ReadUserAction)
	}

	if !slices.Contains(actions, permissions.ReadActionsAction) {
		t.Errorf("Expected actions %v to include %s", actions, permissions.ReadActionsAction)
	}

	if !slices.Contains(actions, permissions.HardResetBranchAction) {
		t.Errorf("Expected actions %v to include %s", actions, permissions.HardResetBranchAction)
	}

	if slices.Contains(actions, "IsValidAction") {
		t.Errorf("Expected actions %v not to include IsValidAction", actions)
	}
}

func TestHardResetBranchAction(t *testing.T) {
	// Test that the HardResetBranchAction constant has the correct value
	expectedAction := "fs:HardResetBranch"
	if permissions.HardResetBranchAction != expectedAction {
		t.Errorf("Expected HardResetBranchAction to be %s, got %s", expectedAction, permissions.HardResetBranchAction)
	}

	// Test that the action is valid according to IsValidAction
	err := permissions.IsValidAction(permissions.HardResetBranchAction)
	if err != nil {
		t.Errorf("Expected HardResetBranchAction to be valid, got error: %v", err)
	}

	// Test that the action is included in the Actions slice
	if !slices.Contains(permissions.Actions, permissions.HardResetBranchAction) {
		t.Errorf("Expected HardResetBranchAction to be included in Actions slice")
	}
}
