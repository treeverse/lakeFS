package export

import (
	"encoding/json"
	"io/ioutil"
	"strings"
	"testing"

	"github.com/treeverse/lakefs/block"
	"github.com/treeverse/lakefs/block/mem"
	"github.com/treeverse/lakefs/parade"
	"github.com/treeverse/lakefs/testutil"
)

func TestCopy(t *testing.T) {
	adapter := testutil.NewBlockAdapterByType(t, &block.NoOpTranslator{}, mem.BlockstoreType)
	sourcePointer := block.ObjectPointer{
		StorageNamespace: "mem://lakeFS-bucket",
		Identifier:       "/one/two",
	}
	destinationPointer := block.ObjectPointer{
		StorageNamespace: "mem://external-bucket",
		Identifier:       "/one/two",
	}
	from := sourcePointer.StorageNamespace + sourcePointer.Identifier
	to := destinationPointer.StorageNamespace + destinationPointer.Identifier

	testData := "this is the test Data"
	testReader := strings.NewReader(testData)
	err := adapter.Put(sourcePointer, testReader.Size(), testReader, block.PutOpts{})
	if err != nil {
		t.Fatal(err)
	}

	h := NewHandler(adapter, nil, nil)
	taskBody, err := json.Marshal(&CopyData{
		From: from,
		To:   to,
	})
	if err != nil {
		t.Fatal(err)
	}
	taskBodyStr := string(taskBody)
	task := parade.OwnedTaskData{
		Action: CopyAction,
		Body:   &taskBodyStr,
	}
	if res := h.Handle(task.Action, task.Body); res.StatusCode != parade.TaskCompleted {
		t.Errorf("expected status code: %s, got: %s", parade.TaskCompleted, res.StatusCode)
	}
	// read Destination
	reader, err := adapter.Get(destinationPointer, testReader.Size())

	if err != nil {
		t.Fatal(err)
	}

	val, err := ioutil.ReadAll(reader)
	if err != nil {
		t.Fatal(err)
	}
	expect := testData
	if string(val) != expect {
		t.Errorf("expected %s, got %s\n", testData, string(val))
	}
}

func TestDelete(t *testing.T) {
	adapter := testutil.NewBlockAdapterByType(t, &block.NoOpTranslator{}, mem.BlockstoreType)

	destinationPointer := block.ObjectPointer{
		StorageNamespace: "mem://external-bucket",
		Identifier:       "/one/two",
	}
	path := destinationPointer.StorageNamespace + destinationPointer.Identifier

	testData := "this is the test Data"
	testReader := strings.NewReader(testData)
	err := adapter.Put(destinationPointer, testReader.Size(), testReader, block.PutOpts{})
	if err != nil {
		t.Fatal(err)
	}

	h := NewHandler(adapter, nil, nil)
	taskBody, err := json.Marshal(&DeleteData{
		File: path,
	})
	if err != nil {
		t.Fatal(err)
	}
	taskBodyStr := string(taskBody)
	task := parade.OwnedTaskData{
		Action: DeleteAction,
		Body:   &taskBodyStr,
	}
	if res := h.Handle(task.Action, task.Body); res.StatusCode != parade.TaskCompleted {
		t.Errorf("expected status code: %s, got: %s", parade.TaskCompleted, res.StatusCode)
	}
	// read Destination
	_, err = adapter.Get(destinationPointer, testReader.Size())
	if err == nil {
		t.Errorf("expected path get err file not found")
	}

}

func TestTouch(t *testing.T) {
	adapter := testutil.NewBlockAdapterByType(t, &block.NoOpTranslator{}, mem.BlockstoreType)
	destinationPointer := block.ObjectPointer{
		StorageNamespace: "mem://external-bucket",
		Identifier:       "/one/two",
	}
	path := destinationPointer.StorageNamespace + destinationPointer.Identifier

	testData := "this is the test Data"
	testReader := strings.NewReader(testData)
	err := adapter.Put(destinationPointer, testReader.Size(), testReader, block.PutOpts{})
	if err != nil {
		t.Fatal(err)
	}

	h := NewHandler(adapter, nil, nil)
	taskBody, err := json.Marshal(&SuccessData{
		File: path,
	})
	if err != nil {
		t.Fatal(err)
	}
	taskBodyStr := string(taskBody)
	task := parade.OwnedTaskData{
		Action: TouchAction,
		Body:   &taskBodyStr,
	}
	if res := h.Handle(task.Action, task.Body); res.StatusCode != parade.TaskCompleted {
		t.Errorf("expected status code: %s, got: %s", parade.TaskCompleted, res.StatusCode)
	}
	// read Destination
	reader, err := adapter.Get(destinationPointer, testReader.Size())

	if err != nil {
		t.Fatal(err)
	}

	val, err := ioutil.ReadAll(reader)
	if err != nil {
		t.Fatal(err)
	}
	expect := ""
	if string(val) != expect {
		t.Errorf("expected %s, got %s\n", testData, string(val))
	}
}
