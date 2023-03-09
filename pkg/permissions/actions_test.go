package permissions_test

import (
	"golang.org/x/exp/slices"
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

	if slices.Contains(actions, "IsValidAction") {
		t.Errorf("Expected actions %v not to include IsValidAction", actions)
	}
}
