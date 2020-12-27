package ref

import "errors"

const (
	iteratorOffsetGE = ">="
	iteratorOffsetGT = ">"
)

var ErrIteratorClosed = errors.New("iterator already closed")

func iteratorOffsetCondition(initial bool) string {
	if initial {
		return iteratorOffsetGE
	}
	return iteratorOffsetGT
}
