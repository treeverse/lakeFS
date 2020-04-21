package permissions

import (
	"fmt"
)

const (
	AllReposArn = "arn:treeverse:repos:::*"
	RepoFmtArn  = "arn:treeverse:repos:::%s"
)

type Action struct {
	Permission Permission
	Arn        string
}

func repoArn(repoId string) string {
	return fmt.Sprintf(RepoFmtArn, repoId)
}

// basic registry of actions
func ListRepos() Action {
	return Action{
		Permission: ManageRepos,
		Arn:        AllReposArn,
	}
}

func GetRepo(repoId string) Action {
	return Action{
		Permission: ReadRepo,
		Arn:        AllReposArn,
	}
}

func GetCommit(repoId string) Action {
	return Action{
		Permission: ReadRepo,
		Arn:        repoArn(repoId),
	}
}

func Commit(repoId string) Action {
	return Action{
		Permission: WriteRepo,
		Arn:        repoArn(repoId),
	}
}

func CreateRepo() Action {
	return Action{
		Permission: WriteRepo,
		Arn:        AllReposArn,
	}
}

func DeleteRepo(repoId string) Action {
	return Action{
		Permission: WriteRepo,
		Arn:        repoArn(repoId),
	}
}

func ListBranches(repoId string) Action {
	return Action{
		Permission: ReadRepo,
		Arn:        repoArn(repoId),
	}
}

func DiffBranches(repoId string) Action {
	return Action{
		Permission: ReadRepo,
		Arn:        repoArn(repoId),
	}
}

func GetBranch(repoId string) Action {
	return Action{
		Permission: ReadRepo,
		Arn:        repoArn(repoId),
	}
}

func CreateBranch(repoId string) Action {
	return Action{
		Permission: WriteRepo,
		Arn:        repoArn(repoId),
	}
}

func MergeIntoBranch(repoId string) Action {
	return Action{
		Permission: WriteRepo,
		Arn:        repoArn(repoId),
	}
}

func DeleteBranch(repoId string) Action {
	return Action{
		Permission: WriteRepo,
		Arn:        repoArn(repoId),
	}
}

func GetObject(repoId string) Action {
	return Action{
		Permission: ReadRepo,
		Arn:        repoArn(repoId),
	}
}

func DeleteObject(repoId string) Action {
	return Action{
		Permission: WriteRepo,
		Arn:        repoArn(repoId),
	}
}

func ListObjects(repoId string) Action {
	return Action{
		Permission: ReadRepo,
		Arn:        repoArn(repoId),
	}
}

func WriteObject(repoId string) Action {
	return Action{
		Permission: WriteRepo,
		Arn:        repoArn(repoId),
	}
}
