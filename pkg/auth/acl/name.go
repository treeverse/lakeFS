package acl

import "strings"

const ACLPolicyPrefix = "ACL(_-_)"

// ACLPolicyName returns the policy identifier for the ACL for groupID.
func ACLPolicyName(groupID string) string {
	return ACLPolicyPrefix + groupID
}

func IsACLPolicyName(policyName string) bool {
	return strings.HasPrefix(policyName, ACLPolicyPrefix)
}
