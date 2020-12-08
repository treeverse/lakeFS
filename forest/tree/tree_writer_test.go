package tree

import (
	"compress/gzip"
	"encoding/csv"
	"fmt"
	"log"
	"os"
	"testing"

	"github.com/treeverse/lakefs/catalog/rocks"

	"github.com/treeverse/lakefs/forest/tree/mocks"
)

func TestSimpleTree(t *testing.T) {
	InitTreesRepository(&mocks.SstMgr{})
	b := mocks.NewBatchCloser()
	//tw := TreeWriter{closeAsync: b,
	//	isSplitKeyFunc: testSplitter}
	tw := TreeWriter{closeAsync: b,
		splitFactor: 150_003}
	input := make(chan []string, 10)
	go readGzip(input)
	var lastKey string
	//var repeatCount int
	var lineCount, lastLineCount int
	for inp := range input {
		if lastKey == inp[0] {
			continue
		}
		lineCount++
		if inp[0] < lastKey {
			panic(" unsorted keys:" + lastKey + inp[0])
		}
		lastKey = inp[0]

		r := rocks.EntryRecord{
			Path: rocks.Path(inp[0]),
		}
		if err := tw.writeEntry(r); err != nil {
			log.Fatal(err)
		}
		if !tw.hasOpenWriter() {
			diff := lineCount - lastLineCount
			lastLineCount = lineCount
			fmt.Printf("writer closed line %d diff %d\n", lineCount, diff)
		}
	}
	_, err := tw.finalizeTree(nil)
	if err != nil {
		panic(err)
	}

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
