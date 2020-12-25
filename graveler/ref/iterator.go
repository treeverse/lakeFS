package ref

const (
	iteratorOffsetGE = ">="
	iteratorOffsetGT = ">"
)

func iteratorOffsetCondition(initial bool) string {
	if initial {
		return iteratorOffsetGE
	}
	return iteratorOffsetGT
}
