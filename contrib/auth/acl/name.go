package acl

import "strings"

const PolicyPrefix = "ACL(_-_)"

// PolicyName returns the policy identifier for the ACL for groupID.
func PolicyName(groupID string) string {
	return PolicyPrefix + groupID
}

func IsPolicyName(policyName string) bool {
	return strings.HasPrefix(policyName, PolicyPrefix)
}
