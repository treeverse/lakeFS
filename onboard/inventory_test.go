package onboard_test

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/treeverse/lakefs/onboard"
	"reflect"
	"testing"
)

func mockReadRows(_ context.Context, _ s3iface.S3API, inventoryBucketName string, file onboard.ManifestFile) ([]onboard.InventoryObject, error) {
	if inventoryBucketName != "example-bucket" {
		return nil, fmt.Errorf("wrong bucket name: %s", inventoryBucketName)
	}
	return rows(fileContents[file.Key]...), nil
}

var fileContents = map[string][]string{
	"f1": {"f1row1", "f1row2"},
	"f2": {"f2row1", "f2row2"},
	"f3": {"f3row1", "f3row2"},
	"f4": {"f4row1", "f4row2", "f4row3", "f4row4", "f4row5", "f4row6", "f4row7"},
	"f5": {"a1", "a3", "a5"},
	"f6": {"a2", "a4", "a6", "a7"},
}

func TestFetch(t *testing.T) {
	manifest := onboard.Manifest{
		InventoryBucketArn: "arn:aws:s3:::example-bucket",
	}
	testdata := []struct {
		InventoryFiles  []string
		Sort            bool
		ExpectedObjects []string
	}{
		{
			InventoryFiles:  []string{"f1", "f2", "f3"},
			ExpectedObjects: []string{"f1row1", "f1row2", "f2row1", "f2row2", "f3row1", "f3row2"},
		},
		{
			InventoryFiles:  []string{},
			ExpectedObjects: []string{},
		},
		{
			InventoryFiles:  []string{"f3", "f2", "f1"},
			ExpectedObjects: []string{"f3row1", "f3row2", "f2row1", "f2row2", "f1row1", "f1row2"},
		},
		{
			InventoryFiles:  []string{"f4"},
			ExpectedObjects: []string{"f4row1", "f4row2", "f4row3", "f4row4", "f4row5", "f4row6", "f4row7"},
		},
		{
			InventoryFiles:  []string{"f1", "f4"},
			ExpectedObjects: []string{"f1row1", "f1row2", "f4row1", "f4row2", "f4row3", "f4row4", "f4row5", "f4row6", "f4row7"},
		},
		{
			InventoryFiles:  []string{"f3", "f2", "f1"},
			Sort:            true,
			ExpectedObjects: []string{"f1row1", "f1row2", "f2row1", "f2row2", "f3row1", "f3row2"},
		},
		{
			InventoryFiles:  []string{"f5", "f6"},
			Sort:            true,
			ExpectedObjects: []string{"a1", "a2", "a3", "a4", "a5", "a6", "a7"},
		},
	}

	manifestURL := "s3://example-bucket/manifest1.json"
	for _, test := range testdata {
		inv, err := onboard.NewS3InventoryFactory(&mockS3Client{
			FilesByManifestURL: map[string][]string{manifestURL: test.InventoryFiles},
			DestBucket:         "example-bucket",
		}).NewInventory(manifestURL)
		if err != nil {
			t.Fatalf("failed to create inventory: %v", err)
		}
		inv.(*onboard.S3Inventory).RowReader = mockReadRows
		manifest.Files = files(test.InventoryFiles...)
		err = inv.Fetch(context.Background(), test.Sort)
		if err != nil {
			t.Fatalf("error: %v", err)
		}
		if len(inv.Objects()) != len(test.ExpectedObjects) {
			t.Fatalf("unexpected number of objects in inventory. expected=%d, got=%d", len(test.ExpectedObjects), len(inv.Objects()))
		}
		if !reflect.DeepEqual(keys(inv.Objects()), test.ExpectedObjects) {
			t.Fatalf("objects in inventory differrent than expected. expected=%v, got=%v", test.ExpectedObjects, keys(inv.Objects()))
		}
	}
}

func TestDiff(t *testing.T) {
	data := []struct {
		LeftInv             []onboard.InventoryObject
		RightInv            []onboard.InventoryObject
		ExpectedDiffAdded   []string
		ExpectedDiffDeleted []string
	}{
		{
			LeftInv:             rows("a1", "a2", "a3"),
			RightInv:            rows("a1", "a3", "b4"),
			ExpectedDiffAdded:   []string{"b4"},
			ExpectedDiffDeleted: []string{"a2"},
		},
		{
			LeftInv:             rows("a1", "a2", "a3"),
			RightInv:            rows("a1", "a2", "a3"),
			ExpectedDiffAdded:   []string{},
			ExpectedDiffDeleted: []string{},
		},
		{
			LeftInv:             rows("a1", "a2", "a3"),
			RightInv:            rows("b1", "b2", "b3", "b4", "b5", "b6"),
			ExpectedDiffAdded:   []string{"b1", "b2", "b3", "b4", "b5", "b6"},
			ExpectedDiffDeleted: []string{"a1", "a2", "a3"},
		},
		{
			LeftInv:             rows("a1", "a3", "a4"),
			RightInv:            rows("a1", "a2", "a3", "a4"),
			ExpectedDiffAdded:   []string{"a2"},
			ExpectedDiffDeleted: []string{},
		},
		{
			LeftInv:             rows("a1", "a2", "a3", "a4"),
			RightInv:            rows("a1", "a2", "a4"),
			ExpectedDiffAdded:   []string{},
			ExpectedDiffDeleted: []string{"a3"},
		},
		{
			LeftInv:             rows("a1", "a2", "a3", "a4", "a5"),
			RightInv:            rows("b1", "b2"),
			ExpectedDiffAdded:   []string{"b1", "b2"},
			ExpectedDiffDeleted: []string{"a1", "a2", "a3", "a4", "a5"},
		},
		{
			LeftInv:             rows(),
			RightInv:            rows("b1", "b2"),
			ExpectedDiffAdded:   []string{"b1", "b2"},
			ExpectedDiffDeleted: []string{},
		},
		{
			LeftInv:             rows("b1", "b2"),
			RightInv:            rows(),
			ExpectedDiffAdded:   []string{},
			ExpectedDiffDeleted: []string{"b1", "b2"},
		},
	}
	for _, test := range data {
		diff := onboard.CalcDiff(test.LeftInv, test.RightInv)
		if !reflect.DeepEqual(keys(diff.AddedOrChanged), test.ExpectedDiffAdded) {
			t.Fatalf("diff added object different than expected. expected: %v, got: %v", test.ExpectedDiffAdded, keys(diff.AddedOrChanged))
		}
		if !reflect.DeepEqual(keys(diff.Deleted), test.ExpectedDiffDeleted) {
			t.Fatalf("diff deleted object different than expected. expected: %v, got: %v", test.ExpectedDiffDeleted, keys(diff.Deleted))
		}
	}
}
