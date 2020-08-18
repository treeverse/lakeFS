package catalog

type RelationType string

const (
	RelationTypeNone       RelationType = "none"
	RelationTypeFromParent RelationType = "from_parent"
	RelationTypeFromChild  RelationType = "from_child"
	RelationTypeNotDirect  RelationType = "non_direct"
)
