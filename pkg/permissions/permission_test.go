package permissions_test

import (
	"testing"
	"github.com/stretchr/testify/assert"

	"github.com/treeverse/lakefs/pkg/permissions"
)

func TestArnTemplate(t *testing.T) {
	repoId := "someRepo"
	branchId := "someBranch"

	templateRes := permissions.FillTemplate(permissions.BranchArnTemplate, (map[string]string{
		"repoID":   repoId,
		"branchID": branchId,
	}))
	assert.Equal(t, templateRes, permissions.BranchArn(repoId, branchId))
}

func TestFillPermissionTemplate(t *testing.T) {
	repoId := "someRepo"
	srcPath := "obj1"
	destPath := "obj2"

	permission := permissions.CopyObjectPermissions()
	permissions.FillPermissionTemplate(&permission, map[string]string{
		permissions.RepoID:  repoId,
		permissions.SrcKey:  srcPath,
		permissions.DestKey: destPath,
	})

	compareTo := permissions.Node{
		Type: permissions.NodeTypeAnd,
		Nodes: []permissions.Node{
			{
				Permission: permissions.Permission{
					Action:   permissions.ReadObjectAction,
					Resource: permissions.ObjectArn(repoId, srcPath),
				},
			},
			{
				Permission: permissions.Permission{
					Action:   permissions.WriteObjectAction,
					Resource: permissions.ObjectArn(repoId, destPath),
				},
			},
		},
	}
	assert.Equal(t, permission, compareTo)
}
