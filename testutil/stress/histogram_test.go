package stress_test

import (
	"testing"

	"github.com/treeverse/lakefs/testutil/stress"
)

func TestHistogram_Add(t *testing.T) {
	tbl := []struct {
		Name     string
		Buckets  []int64
		Inputs   []int64
		Expected string
	}{
		{
			Name:     "simple_add",
			Buckets:  []int64{1, 5, 10},
			Inputs:   []int64{1, 343, 2, 1},
			Expected: "1\t2\n5\t3\n10\t3\nmin\t1\nmax\t343\ntotal\t4\n",
		},
		{
			Name:     "one_bucket",
			Buckets:  []int64{1},
			Inputs:   []int64{1, 343, 2, 1},
			Expected: "1\t2\nmin\t1\nmax\t343\ntotal\t4\n",
		},
		{
			Name:     "no_input",
			Buckets:  []int64{1, 5, 10},
			Inputs:   []int64{},
			Expected: "1\t0\n5\t0\n10\t0\nmin\t0\nmax\t0\ntotal\t0\n",
		},
	}

	for _, cas := range tbl {
		t.Run(cas.Name, func(t *testing.T) {
			h := stress.NewHistogram(cas.Buckets)
			for _, v := range cas.Inputs {
				h.Add(v)
			}
			str := h.String()
			if str != cas.Expected {
				t.Fatalf("expected '%s' got '%s'", cas.Expected, h.String())
			}
		})
	}
}
