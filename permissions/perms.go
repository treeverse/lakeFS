package permissions

import "fmt"

const (
	FSArnFmt   = "arn:lakefs:fs:::%s"
	AuthFmtArn = "arn:lakefs:auth:::%s"
	All        = "*"
)

type Permission struct {
	Action   Action
	Resource string
}

func RepoArn(repoId string) string {
	return fmt.Sprintf(FSArnFmt, fmt.Sprintf("repository/%s", repoId))
}

func ObjectArn(repoId, key string) string {
	return fmt.Sprintf(FSArnFmt, fmt.Sprintf("repository/%s/object/%s", repoId, key))
}

func BranchArn(repoId, branchId string) string {
	return fmt.Sprintf(FSArnFmt, fmt.Sprintf("repository/%s/branch/%s", repoId, branchId))
}

func UserArn(userId string) string {
	return fmt.Sprintf(AuthFmtArn, fmt.Sprintf("user/%s", userId))
}

func CredentialsArn(userId string) string {
	return fmt.Sprintf(AuthFmtArn, fmt.Sprintf("credentials/%s", userId))
}

func GroupArn(groupId string) string {
	return fmt.Sprintf(AuthFmtArn, fmt.Sprintf("group/%s", groupId))
}

func PolicyArn(policyId string) string {
	return fmt.Sprintf(AuthFmtArn, fmt.Sprintf("policy/%s", policyId))
}
