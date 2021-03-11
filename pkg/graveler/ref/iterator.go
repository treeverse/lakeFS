package ref

import "errors"

const (
	iteratorOffsetGE = ">="
	iteratorOffsetGT = ">"
)

type iteratorState int

const (
	iteratorStateInit iteratorState = iota
	iteratorStateQuerying
	iteratorStateDone
)

var ErrIteratorClosed = errors.New("iterator already closed")

func iteratorOffsetCondition(initial bool) string {
	if initial {
		return iteratorOffsetGE
	}
	return iteratorOffsetGT
}
