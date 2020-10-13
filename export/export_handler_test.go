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
		{
			name:   "delete on mem",
			Action: actionDelete,
			Body: TaskBody{
				DestinationNamespace: "local://external-bucket",
				DestinationID:        "one/two",
			},
			blockstoreType: mem.BlockstoreType,
		},
		{
			name:   "touch on mem",
			Action: actionTouch,
			Body: TaskBody{
				DestinationNamespace: "local://external-bucket",
				DestinationID:        "one/two",
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
			if tt.Action == actionCopy {
				err := adapter.Put(sourcePointer, testReader.Size(), testReader, block.PutOpts{})
				if err != nil {
					t.Fatal(err)
				}
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
				if tt.Action == actionDelete {
					return
				}
				t.Fatal(err)
			}
			if tt.Action == actionDelete {
				t.Fatalf("expected to get error on get in action delete")
			}
			val, err := ioutil.ReadAll(reader)
			if err != nil {
				t.Fatal(err)
			}
			expect := testData
			if tt.Action == actionTouch {
				expect = ""
			}
			if string(val) != expect {
				t.Errorf("expected %s, got %s\n", testData, string(val))
			}
		})
	}
}
