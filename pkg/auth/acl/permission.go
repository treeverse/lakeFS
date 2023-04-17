package acl

import (
	"fmt"

	"github.com/treeverse/lakefs/pkg/auth"
	"github.com/treeverse/lakefs/pkg/auth/model"
	"github.com/treeverse/lakefs/pkg/permissions"
)

const (
	// ACLRead allows reading the specified repositories, as well as
	// managing own credentials.
	ACLRead model.ACLPermission = "Read"
	// ACLWrite allows reading and writing the specified repositories,
	// as well as managing own credentials.
	ACLWrite model.ACLPermission = "Write"
	// ACLSuper allows reading, writing, and all other actions on the
	// specified repositories, as well as managing own credentials.
	ACLSuper model.ACLPermission = "Super"
	// ACLAdmin allows all operations, including all reading, writing,
	// and all other actions on all repositories, and managing
	// authorization and credentials of all users.
	ACLAdmin model.ACLPermission = "Admin"
)

var (
	ACLPermissions = []model.ACLPermission{ACLRead, ACLWrite, ACLSuper, ACLAdmin}

	ownUserARN = []string{permissions.UserArn("${user}")}
	all        = []string{permissions.All}

	ErrBadACLPermission = fmt.Errorf("%w: Bad ACL permission", model.ErrValidationError)
)

func ACLToStatement(acl model.ACL) (model.Statements, error) {
	var (
		statements model.Statements
		err        error
	)

	switch acl.Permission {
	case ACLRead:
		statements, err = auth.MakeStatementForPolicyType("FSRead", all)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", acl.Permission, ErrBadACLPermission)
		}
		readConfigStatement, err := auth.MakeStatementForPolicyType("FSReadConfig", all)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", acl.Permission, ErrBadACLPermission)
		}

		ownCredentialsStatement, err := auth.MakeStatementForPolicyType("AuthManageOwnCredentials", ownUserARN)
		if err != nil {
			return nil, err
		}
		statements = append(append(statements, readConfigStatement...), ownCredentialsStatement...)
	case ACLWrite:
		statements, err = auth.MakeStatementForPolicyType("FSReadWrite", all)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", acl.Permission, ErrBadACLPermission)
		}

		ownCredentialsStatement, err := auth.MakeStatementForPolicyType("AuthManageOwnCredentials", ownUserARN)
		if err != nil {
			return nil, err
		}

		ciStatement, err := auth.MakeStatementForPolicyType("RepoManagementRead", all)
		if err != nil {
			return nil, fmt.Errorf("%s: get RepoManagementRead: %w", acl.Permission, ErrBadACLPermission)
		}

		statements = append(statements, append(ownCredentialsStatement, ciStatement...)...)
	case ACLSuper:
		statements, err = auth.MakeStatementForPolicyType("FSFullAccess", all)
		if err != nil {
			return nil, fmt.Errorf("%s: get FSFullAccess: %w", acl.Permission, ErrBadACLPermission)
		}

		ownCredentialsStatement, err := auth.MakeStatementForPolicyType("AuthManageOwnCredentials", ownUserARN)
		if err != nil {
			return nil, fmt.Errorf("%s: get AuthManageOwnCredentials: %w", acl.Permission, ErrBadACLPermission)
		}

		ciStatement, err := auth.MakeStatementForPolicyType("RepoManagementRead", all)
		if err != nil {
			return nil, fmt.Errorf("%s: get RepoManagementRead: %w", acl.Permission, ErrBadACLPermission)
		}

		statements = append(statements, append(ownCredentialsStatement, ciStatement...)...)
	case ACLAdmin:
		statements, err = auth.MakeStatementForPolicyType("AllAccess", []string{permissions.All})
		if err != nil {
			return nil, fmt.Errorf("%s: %w", acl.Permission, ErrBadACLPermission)
		}
	default:
		return nil, fmt.Errorf("%w \"%s\"", ErrBadACLPermission, acl.Permission)
	}

	return statements, nil
}
