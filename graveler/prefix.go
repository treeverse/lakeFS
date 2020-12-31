package graveler

import "math"

func UpperBoundForPrefix(prefix []byte) []byte {
	idx := len(prefix) - 1
	for idx >= 0 && prefix[idx] == math.MaxUint8 {
		idx--
	}
	if idx == -1 {
		return nil
	}
	upperBound := make([]byte, idx+1)
	copy(upperBound, prefix[:idx+1])
	upperBound[idx]++
	return upperBound
}
