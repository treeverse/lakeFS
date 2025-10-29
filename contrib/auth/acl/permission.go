package acl

import (
	"fmt"

	"github.com/treeverse/lakefs/pkg/auth"
	"github.com/treeverse/lakefs/pkg/auth/model"
	"github.com/treeverse/lakefs/pkg/permissions"
)

const (
	// ReadPermission allows reading the specified repositories, as well as
	// managing own credentials.
	ReadPermission model.ACLPermission = "Read"
	// WritePermission allows reading and writing the specified repositories,
	// as well as managing own credentials.
	WritePermission model.ACLPermission = "Write"
	// SuperPermission allows reading, writing, and all other actions on the
	// specified repositories, as well as managing own credentials.
	SuperPermission model.ACLPermission = "Super"
	// AdminPermission allows all operations, including all reading, writing,
	// and all other actions on all repositories, and managing
	// authorization and credentials of all users.
	AdminPermission model.ACLPermission = "Admin"
)

var (
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
	case ReadPermission:
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
	case WritePermission:
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
	case SuperPermission:
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
	case AdminPermission:
		statements, err = auth.MakeStatementForPolicyType("AllAccess", []string{permissions.All})
		if err != nil {
			return nil, fmt.Errorf("%s: %w", acl.Permission, ErrBadACLPermission)
		}
	default:
		return nil, fmt.Errorf("%w \"%s\"", ErrBadACLPermission, acl.Permission)
	}

	return statements, nil
}
