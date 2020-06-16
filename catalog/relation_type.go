package catalog

type RelationType string

const (
	RelationTypeNone       RelationType = "none"
	RelationTypeFromFather RelationType = "from_father"
	RelationTypeFromSon    RelationType = "from_son"
	RelationTypeNotDirect  RelationType = "non_direct"
)
