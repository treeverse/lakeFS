package permissions

import "fmt"

const (
	AllReposArn            = "arn:lakefs:repos:::*"
	RepoFmtArn             = "arn:lakefs:repos:::%s"
	AllAuthArn             = "arn:lakefs:auth:::*"
	AuthUserCredentialsArn = "arn:lakefs:auth:::credentials/%s"
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
		Action:   ManageReposAction,
		Resource: AllReposArn,
	}
}

func GetRepo(repoId string) Permission {
	return Permission{
		Action:   ReadRepoAction,
		Resource: repoArn(repoId),
	}
}

func GetCommit(repoId string) Permission {
	return Permission{
		Action:   ReadRepoAction,
		Resource: repoArn(repoId),
	}
}

func Commit(repoId string) Permission {
	return Permission{
		Action:   WriteRepoAction,
		Resource: repoArn(repoId),
	}
}

func CreateRepo() Permission {
	return Permission{
		Action:   ManageReposAction,
		Resource: AllReposArn,
	}
}

func DeleteRepo(repoId string) Permission {
	return Permission{
		Action:   ManageReposAction,
		Resource: repoArn(repoId),
	}
}

func ListBranches(repoId string) Permission {
	return Permission{
		Action:   ReadRepoAction,
		Resource: repoArn(repoId),
	}
}

func DiffBranches(repoId string) Permission {
	return Permission{
		Action:   ReadRepoAction,
		Resource: repoArn(repoId),
	}
}

func GetBranch(repoId string) Permission {
	return Permission{
		Action:   ReadRepoAction,
		Resource: repoArn(repoId),
	}
}

func CreateBranch(repoId string) Permission {
	return Permission{
		Action:   WriteRepoAction,
		Resource: repoArn(repoId),
	}
}

func MergeIntoBranch(repoId string) Permission {
	return Permission{
		Action:   WriteRepoAction,
		Resource: repoArn(repoId),
	}
}

func DeleteBranch(repoId string) Permission {
	return Permission{
		Action:   WriteRepoAction,
		Resource: repoArn(repoId),
	}
}

func GetObject(repoId string) Permission {
	return Permission{
		Action:   ReadRepoAction,
		Resource: repoArn(repoId),
	}
}

func DeleteObject(repoId string) Permission {
	return Permission{
		Action:   WriteRepoAction,
		Resource: repoArn(repoId),
	}
}

func ListObjects(repoId string) Permission {
	return Permission{
		Action:   ReadRepoAction,
		Resource: repoArn(repoId),
	}
}

func WriteObject(repoId string) Permission {
	return Permission{
		Action:   WriteRepoAction,
		Resource: repoArn(repoId),
	}
}

func ReadAuth() Permission {
	return Permission{
		Action:   ReadAuthAction,
		Resource: AllAuthArn,
	}
}

func WriteAuth() Permission {
	return Permission{
		Action:   WriteAuthAction,
		Resource: AllAuthArn,
	}
}

func ReadAuthCredentials(user string) Permission {
	return Permission{
		Action:   ReadAuthAction,
		Resource: fmt.Sprintf(AuthUserCredentialsArn, user),
	}
}

func WriteAuthCredentials(user string) Permission {
	return Permission{
		Action:   WriteAuthAction,
		Resource: fmt.Sprintf(AuthUserCredentialsArn, user),
	}
}
