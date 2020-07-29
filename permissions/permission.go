package permissions

const (
	fSArnPrefix   = "arn:lakefs:fs:::"
	authArnPrefix = "arn:lakefs:auth:::"

	All = "*"
)

type Permission struct {
	Action   string
	Resource string
}

func RepoArn(repoID string) string {
	return fSArnPrefix + "repository/" + repoID
}

func ObjectArn(repoID, key string) string {
	return fSArnPrefix + "repository/" + repoID + "/object/" + key
}

func BranchArn(repoID, branchID string) string {
	return fSArnPrefix + "repository/" + repoID + "/branch/" + branchID
}

func UserArn(userID string) string {
	return authArnPrefix + "user/" + userID
}

func GroupArn(groupID string) string {
	return authArnPrefix + "group/" + groupID
}

func PolicyArn(policyID string) string {
	return authArnPrefix + "policy/" + policyID
}
