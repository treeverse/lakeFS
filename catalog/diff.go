package catalog

type DifferenceType int

const (
	DifferenceTypeAdded DifferenceType = iota
	DifferenceTypeRemoved
	DifferenceTypeChanged
	DifferenceTypeConflict
	DifferenceTypeNone
)

type Difference struct {
	DBEntry                // Partially filled. Path is always set.
	Type    DifferenceType `db:"diff_type"`
}

type DiffResultRecord struct {
	TargetEntryNotInDirectBranch bool // the entry is reflected via lineage, NOT in the branch itself
	Difference
	EntryCtid *string // CTID of the modified/added entry. Do not use outside of cataloger diff-by-iterators. https://github.com/treeverse/lakeFS/issues/831
}

func (d Difference) String() string {
	var symbol string
	switch d.Type {
	case DifferenceTypeAdded:
		symbol = "+"
	case DifferenceTypeRemoved:
		symbol = "-"
	case DifferenceTypeChanged:
		symbol = "~"
	case DifferenceTypeConflict:
		symbol = "x"
	}
	return symbol + " " + d.Path
}

type Differences []Difference

func (d Differences) Equal(other Differences) bool {
	if len(d) != len(other) {
		return false
	}
	for _, item := range d {
		m := false
		for _, otherItem := range other {
			if otherItem.Path == item.Path {
				m = otherItem.Type == item.Type
				break
			}
		}
		if !m {
			return false
		}
	}
	return true
}
