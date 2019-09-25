package indexer

// resolve a branchspec. could be 1 of 3 options:
// pointer from branch: master~1, master~2, etc (2 commits prior to current master)
// commit hash, or prefix of one: 1b161e5c1fa7425e73043362938b9824 (has to be >=32 hexadecimal characters)

type SpecType int

const (
	SpecTypeBranch SpecType = iota
	SpecTypeCommit
	SpecTypeBranchPointer
)

func ResolveType(spec string) SpecType {
	return SpecTypeBranch
}
