package auth

import (
	"errors"
	"fmt"

	"github.com/treeverse/lakefs/pkg/auth/model"
	"github.com/treeverse/lakefs/pkg/permissions"
)

var (
	ErrStatementNotFound = errors.New("statement not found")
)

// statementForPolicyType holds the Statement for a policy by its name,
// without the required ARN.
var statementByName = map[string]model.Statement{
	"AllAccess": {
		Action: []string{"fs:*", "auth:*", "ci:*", "retention:*", "branches:*"},
		Effect: model.StatementEffectAllow,
	},
	"FSFullAccess": {
		Action: []string{
			"fs:*",
		},
		Effect: model.StatementEffectAllow,
	},
	"FSReadWrite": {
		Action: []string{
			"fs:Read*",
			"fs:List*",
			permissions.WriteObjectAction,
			permissions.DeleteObjectAction,
			permissions.RevertBranchAction,
			permissions.CreateBranchAction,
			permissions.CreateTagAction,
			permissions.DeleteBranchAction,
			permissions.DeleteTagAction,
			permissions.CreateCommitAction,
			permissions.CreateMetaRangeAction,
		},
		Effect: model.StatementEffectAllow,
	},
	"FSReadConfig": {
		Action: []string{
			permissions.ReadConfig,
		},
		Effect: model.StatementEffectAllow,
	},
	"FSRead": {
		Action: []string{
			"fs:List*",
			"fs:Read*",
		},

		Effect: model.StatementEffectAllow,
	},
	"RepoManagementRead": {
		Action: []string{
			"ci:Read*",
			"retention:Get*",
			"branches:Get*",
			permissions.ReadConfig,
		},

		Effect: model.StatementEffectAllow,
	},
	"AuthManageOwnCredentials": {
		Action: []string{
			permissions.CreateCredentialsAction,
			permissions.DeleteCredentialsAction,
			permissions.ListCredentialsAction,
			permissions.ReadCredentialsAction,
		},
		Effect: model.StatementEffectAllow,
	},
}

// GetActionsForPolicyType returns the actions for police type typ.
func GetActionsForPolicyType(typ string) ([]string, error) {
	statement, ok := statementByName[typ]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrStatementNotFound, typ)
	}
	actions := make([]string, len(statement.Action))
	copy(actions, statement.Action)
	return actions, nil
}

func GetActionsForPolicyTypeOrDie(typ string) []string {
	ret, err := GetActionsForPolicyType(typ)
	if err != nil {
		panic(err)
	}
	return ret
}

// MakeStatementForPolicyType returns statements for policy type typ,
// limited to resources.
func MakeStatementForPolicyType(typ string, resources []string) (model.Statements, error) {
	statement, ok := statementByName[typ]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrStatementNotFound, typ)
	}
	statements := make(model.Statements, len(resources))
	for i, resource := range resources {
		if statement.Resource == "" {
			statements[i] = statement
			statements[i].Resource = resource
		}
	}
	return statements, nil
}

func MakeStatementForPolicyTypeOrDie(typ string, resources []string) model.Statements {
	statements, err := MakeStatementForPolicyType(typ, resources)
	if err != nil {
		panic(err)
	}
	return statements
}
