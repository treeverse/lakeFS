package catalog

type DifferenceType int

const (
	DifferenceTypeAdded DifferenceType = iota
	DifferenceTypeRemoved
	DifferenceTypeChanged
	DifferenceTypeConflict
)

type Difference struct {
	Entry                // Partially filled.
	Type  DifferenceType `db:"diff_type"`
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

func (d Differences) CountByType() map[DifferenceType]int {
	result := make(map[DifferenceType]int)
	for i := range d {
		typ := d[i].Type
		if count, ok := result[typ]; !ok {
			result[typ] = 1
		} else {
			result[typ] = count + 1
		}
	}
	return result
}

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
