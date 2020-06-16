package catalog

type DifferenceType int

const (
	DifferenceTypeAdded DifferenceType = iota
	DifferenceTypeRemoved
	DifferenceTypeChanged
	DifferenceTypeConflict
)

type Difference struct {
	Type DifferenceType `db:"diff_type"`
	Path string         `db:"path"`
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

//func (d Differences) CountByType() map[DifferenceType]int {
//	result := make(map[DifferenceType]int)
//	for i := range d {
//		typ := d[i].Type
//		if count, ok := result[typ]; !ok {
//			result[typ] = 1
//		} else {
//			result[typ] = count + 1
//		}
//	}
//	return result
//}
