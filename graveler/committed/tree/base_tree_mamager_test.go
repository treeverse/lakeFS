package tree

import (
	"fmt"
	//"compress/gzip"
	//"encoding/csv"
	//"fmt"
	//"log"
	//"os"
	"testing"

	"github.com/treeverse/lakefs/forest/tree/mocks"
)

func TestBaseTreeSimple(t *testing.T) {
	InitTreesRepository(&mocks.SstMgr{})
	baseTreeManager, err := treesRepository.newBaseTreeManager("tree_e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855")
	if err != nil {
		panic(err)
	}
	x := baseTreeManager.wasLastPartProcessed()
	fmt.Print(x)
	x1, x2, x3 := baseTreeManager.getBasePartForPath("mrp/similargroup/data/mobile-analytics/daily/est_model/prior_v2d/year=19/month=01")
	if x1.Next() {
		fmt.Print(x1.Value().Path)
	}
	_ = x2
	_ = x3

}
