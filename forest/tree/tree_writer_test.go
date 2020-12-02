package tree

import (
	"compress/gzip"
	"encoding/csv"
	"fmt"
	"log"
	"os"
	"testing"
	"time"

	"github.com/treeverse/lakefs/catalog/rocks"

	"github.com/treeverse/lakefs/forest/tree/mocks"
)

func TestSimpleTree(t *testing.T) {
	InitTreesRepository(&mocks.SstMgr{NumToRead: 10})
	b := mocks.NewBatchCloser()
	//tw := TreeWriter{closeAsync: b,
	//	isSplitPathFunc: testSplitter}
	tw := TreeWriter{closeAsync: b}
	input := make(chan []string, 10)
	go readGzip(input)
	var lastKey string
	//var repeatCount int
	var lineCount int
	for inp := range input {
		lineCount++
		if lineCount%100_000 == 0 {
			timeStr := time.Now().Format("15 04:05")
			fmt.Printf("%s: lines number %d\n", timeStr, lineCount)
		}
		if inp[0] < lastKey {
			panic(" unsorted keys:" + lastKey + inp[0])
		}
		lastKey = inp[0]
		//if inp[0] == lastKey{
		//	repeatCount++
		//	inp[0] += strconv.Itoa(repeatCount)
		//} else {
		//	lastKey = inp[0]
		//	repeatCount = 0
		//}
		r := rocks.EntryRecord{
			Path: rocks.Path(inp[0]),
		}
		if err := tw.writeEntry(r); err != nil {
			log.Fatal(err)
		}
	}
	x := make(TreeType, 0)
	tw.finalizeTree(x)
	//for i := 0; i < 10; i++ {
	//	t := rocks.EntryRecord{Path: rocks.Path(strconv.Itoa(i)),
	//		Entry: &rocks.Entry{}}
	//	tw.writeEntry(t)
	//}
}

//func testSplitter(path rocks.Path, rowNum int) bool {
//	return true
//}

func readGzip(output chan []string) {
	defer close(output)
	f, err := os.Open(`simmilar.gz`)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()
	gr, err := gzip.NewReader(f)
	if err != nil {
		log.Fatal(err)
	}
	defer gr.Close()

	cr := csv.NewReader(gr)
	l, _ := cr.Read() // skip header
	_ = l
	for {
		rec, err := cr.Read()
		if err != nil {
			//	log.Fatal(err)
			break
		}
		out := make([]string, 2)
		//t := rec[1] + "," + rec[2]
		t := rec[2]
		out[0] = t
		//out[1]= rec[3] + "," + rec[7]

		out[1] = t[len(t)-50:]
		output <- out
	}

}
