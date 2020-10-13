package export

import (
	"encoding/json"
	"github.com/treeverse/lakefs/block"
	"github.com/treeverse/lakefs/block/mem"
	"github.com/treeverse/lakefs/parade"
	"github.com/treeverse/lakefs/testutil"
	"io/ioutil"
	"strings"
	"testing"
)

func TestExportHandler_Handle(t *testing.T) {
	tests := []struct {
		name           string
		Body           TaskBody
		Action         string
		blockstoreType string
	}{
		{
			name:   "copy on mem",
			Action: actionCopy,
			Body: TaskBody{
				DestinationNamespace: "local://external-bucket",
				DestinationID:        "one/two",
				SourceNamespace:      "local://lakefs-buck",
				SourceID:             "one/two",
			},
			blockstoreType: mem.BlockstoreType,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			adapter := testutil.NewBlockAdapterByType(t, &block.NoOpTranslator{}, tt.blockstoreType)
			sourcePointer := block.ObjectPointer{
				StorageNamespace: tt.Body.SourceNamespace,
				Identifier:       tt.Body.SourceID,
			}
			destinationPointer := block.ObjectPointer{
				StorageNamespace: tt.Body.DestinationNamespace,
				Identifier:       tt.Body.DestinationID,
			}
			// add to
			testData := "this is the test Data"
			testReader := strings.NewReader(testData)

			err := adapter.Put(sourcePointer, testReader.Size(), testReader, block.PutOpts{})
			if err != nil {
				t.Fatal(err)
			}
			h := NewHandler(adapter)
			taskBody, err := json.Marshal(tt.Body)
			if err != nil {
				t.Fatal(err)
			}
			taskBodyStr := string(taskBody)
			task := parade.OwnedTaskData{
				Action: tt.Action,
				Body:   &taskBodyStr,
			}
			if res := h.Handle(task.Action, task.Body); res.StatusCode != parade.TaskCompleted {
				t.Errorf("expected status code: %s, got: %s", parade.TaskCompleted, res.StatusCode)
			}

			// read Destination
			reader, err := adapter.Get(destinationPointer, testReader.Size())
			if err != nil {
				t.Error(err)
			}
			val, err := ioutil.ReadAll(reader)
			if err != nil {
				t.Error(err)
			}
			if string(val) != testData {
				t.Errorf("expected %s, got %s\n", testData, string(val))
			}
			// todo(guys): add tests delete
		})
	}
}
