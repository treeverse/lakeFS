package permissions

import "fmt"

const (
	AllReposArn = "arn:lakefs:repos:::*"
	RepoFmtArn  = "arn:lakefs:repos:::%s"
	RbacArn     = "arn:lakefs:rbac:::*"
)

type Permission struct {
	Action   Action
	Resource string
}

func repoArn(repoId string) string {
	return fmt.Sprintf(RepoFmtArn, repoId)
}

// basic registry of actions
func ListRepos() Permission {
	return Permission{
		Action:   ManageRepos,
		Resource: AllReposArn,
	}
}

func GetRepo(repoId string) Permission {
	return Permission{
		Action:   ReadRepo,
		Resource: AllReposArn,
	}
}

func GetCommit(repoId string) Permission {
	return Permission{
		Action:   ReadRepo,
		Resource: repoArn(repoId),
	}
}

func Commit(repoId string) Permission {
	return Permission{
		Action:   WriteRepo,
		Resource: repoArn(repoId),
	}
}

func CreateRepo() Permission {
	return Permission{
		Action:   WriteRepo,
		Resource: AllReposArn,
	}
}

func DeleteRepo(repoId string) Permission {
	return Permission{
		Action:   WriteRepo,
		Resource: repoArn(repoId),
	}
}

func ListBranches(repoId string) Permission {
	return Permission{
		Action:   ReadRepo,
		Resource: repoArn(repoId),
	}
}

func DiffBranches(repoId string) Permission {
	return Permission{
		Action:   ReadRepo,
		Resource: repoArn(repoId),
	}
}

func GetBranch(repoId string) Permission {
	return Permission{
		Action:   ReadRepo,
		Resource: repoArn(repoId),
	}
}

func CreateBranch(repoId string) Permission {
	return Permission{
		Action:   WriteRepo,
		Resource: repoArn(repoId),
	}
}

func MergeIntoBranch(repoId string) Permission {
	return Permission{
		Action:   WriteRepo,
		Resource: repoArn(repoId),
	}
}

func DeleteBranch(repoId string) Permission {
	return Permission{
		Action:   WriteRepo,
		Resource: repoArn(repoId),
	}
}

func GetObject(repoId string) Permission {
	return Permission{
		Action:   ReadRepo,
		Resource: repoArn(repoId),
	}
}

func DeleteObject(repoId string) Permission {
	return Permission{
		Action:   WriteRepo,
		Resource: repoArn(repoId),
	}
}

func ListObjects(repoId string) Permission {
	return Permission{
		Action:   ReadRepo,
		Resource: repoArn(repoId),
	}
}

func WriteObject(repoId string) Permission {
	return Permission{
		Action:   WriteRepo,
		Resource: repoArn(repoId),
	}
}

func ManageAuth() Permission {
	return Permission{
		Action:   ManageRBAC,
		Resource: RbacArn,
	}
}

func ValidateAction() {

}
