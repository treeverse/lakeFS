package catalog

type RelationType string

const (
	RelationTypeNone       RelationType = "none"
	RelationTypeFromFather              = "from_father"
	RelationTypeFromSon                 = "from_son"
	RelationTypeNotDirect               = "non_direct"
)
