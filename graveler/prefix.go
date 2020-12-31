package graveler

import "math"

// UpperBoundForPrefix returns, given a prefix `p`, an array 'q' such that a byte array `s` starts with `p`
// if and only if p <= s < q. Namely, it returns an exclusive upper bound for the set of all byte arrays
// that start with this prefix.
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
