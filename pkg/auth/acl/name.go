package acl

// ACLPolicyName returns the policy identifier for the ACL for groupID.
func ACLPolicyName(groupID string) string {
	return "ACL:" + groupID
}
