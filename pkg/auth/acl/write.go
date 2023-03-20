package acl

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/treeverse/lakefs/pkg/auth"
	"github.com/treeverse/lakefs/pkg/auth/model"
	"github.com/treeverse/lakefs/pkg/logging"

	"github.com/hashicorp/go-multierror"
)

const (
	ACLAdminsGroup  = "Admins"
	ACLSupersGroup  = "Supers"
	ACLWritersGroup = "Writers"
	ACLReadersGroup = "Readers"
)

func WriteGroupACL(ctx context.Context, svc auth.Service, groupName string, acl model.ACL, creationTime time.Time, warnIfCreate bool) error {
	log := logging.FromContext(ctx).WithField("group", groupName)

	statements, err := ACLToStatement(acl)
	if err != nil {
		return fmt.Errorf("%s: translate ACL %+v to statements: %w", groupName, acl, err)
	}

	aclPolicyName := ACLPolicyName(groupName)

	policy := &model.Policy{
		CreatedAt:   creationTime,
		DisplayName: aclPolicyName,
		Statement:   statements,
		ACL:         acl,
	}

	policyJSON, err := json.MarshalIndent(policy, "", "  ")
	if err != nil {
		return err
	}

	log.WithField("policy", fmt.Sprintf("%+v", policy)).
		WithField("policyJSON", string(policyJSON)).
		Debug("Set policy derived from ACL")

	err = svc.WritePolicy(ctx, policy, true)
	if errors.Is(err, auth.ErrNotFound) {
		if warnIfCreate {
			log.WithField("group", groupName).
				Info("Define an ACL for the first time because none was defined (bad migrate?)")
		}
		err = svc.WritePolicy(ctx, policy, false)
	}
	if err != nil {
		return fmt.Errorf("write policy %s %+v for group %s: %w", aclPolicyName, policy, groupName, err)
	}

	// Detach any existing policies from group
	existingPolicies, _, err := svc.ListGroupPolicies(ctx, groupName, &model.PaginationParams{
		Amount: -1,
	})
	if err != nil {
		return fmt.Errorf("list existing group policies for group %s: %w", groupName, err)
	}

	err = svc.AttachPolicyToGroup(ctx, aclPolicyName, groupName)
	if errors.Is(err, auth.ErrAlreadyExists) {
		err = nil
	}
	if err != nil {
		return fmt.Errorf("attach policy %s to group %s: %w", aclPolicyName, groupName, err)
	}

	for _, existingPolicy := range existingPolicies {
		if existingPolicy.DisplayName != aclPolicyName {
			oneErr := svc.DetachPolicyFromGroup(ctx, existingPolicy.DisplayName, groupName)
			if oneErr != nil {
				err = multierror.Append(
					err,
					fmt.Errorf("detach policy %s from group %s: %w", existingPolicy.DisplayName, groupName, oneErr),
				)
			}
		}
	}
	if err != nil {
		return err
	}
	return nil
}
