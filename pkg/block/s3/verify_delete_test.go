package s3

import (
	"bytes"
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/treeverse/lakefs/pkg/block"
	"github.com/treeverse/lakefs/pkg/block/params"
)

// TestVerifyDeletedSimple - basic test without external dependencies
func TestVerifyDeletedSimple(t *testing.T) {
	ctx := context.Background()

	// Setup MinIO adapter
	s3Params := params.S3{
		Region:         "us-east-1",
		Endpoint:       "http://localhost:9000",
		ForcePathStyle: true,
		Credentials: params.S3Credentials{
			AccessKeyID:     "minioadmin",
			SecretAccessKey: "minioadmin",
		},
		SkipVerifyCertificateTestOnly: true,
	}

	adapter, err := NewAdapter(ctx, s3Params)
	if err != nil {
		t.Fatalf("Failed to create adapter: %v", err)
	}

	bucketName := "test-verify-bucket"
	testKey := fmt.Sprintf("test-file-%d.txt", time.Now().Unix())

	obj := block.ObjectPointer{
		StorageID:        "test",
		StorageNamespace: "s3://" + bucketName,
		Identifier:       testKey,
		IdentifierType:   block.IdentifierTypeRelative,
	}

	// Upload test object
	content := []byte("test content for verification")
	_, err = adapter.Put(ctx, obj, int64(len(content)), bytes.NewReader(content), block.PutOpts{})
	if err != nil {
		t.Fatalf("Failed to upload: %v", err)
	}

	// Verify it exists
	exists, err := adapter.Exists(ctx, obj)
	if err != nil {
		t.Fatalf("Failed to check existence: %v", err)
	}
	if !exists {
		t.Fatal("Object should exist before delete")
	}

	// Delete the object
	err = adapter.Remove(ctx, obj)
	if err != nil {
		t.Fatalf("Failed to delete: %v", err)
	}

	// Run verification
	stillExists, err := adapter.VerifyDeleted(ctx, []block.ObjectPointer{obj})
	if err != nil {
		t.Fatalf("Verification failed: %v", err)
	}
	if len(stillExists) > 0 {
		t.Fatalf("Expected 0 objects to remain, got %d", len(stillExists))
	}

	t.Log("✅ Verification test passed!")
}

func TestVerifyDeletedSimple_MultipleObjects(t *testing.T) {
	ctx := context.Background()

	s3Params := params.S3{
		Region:         "us-east-1",
		Endpoint:       "http://localhost:9000",
		ForcePathStyle: true,
		Credentials: params.S3Credentials{
			AccessKeyID:     "minioadmin",
			SecretAccessKey: "minioadmin",
		},
		SkipVerifyCertificateTestOnly: true,
	}

	adapter, err := NewAdapter(ctx, s3Params)
	if err != nil {
		t.Fatalf("Failed to create adapter: %v", err)
	}

	bucketName := "test-verify-bucket"
	numObjects := 10

	objs := make([]block.ObjectPointer, numObjects)
	for i := 0; i < numObjects; i++ {
		objs[i] = block.ObjectPointer{
			StorageID:        "test",
			StorageNamespace: "s3://" + bucketName,
			Identifier:       fmt.Sprintf("multi-test-%d-%d.txt", time.Now().Unix(), i),
			IdentifierType:   block.IdentifierTypeRelative,
		}

		content := []byte(fmt.Sprintf("content %d", i))
		_, err := adapter.Put(ctx, objs[i], int64(len(content)), bytes.NewReader(content), block.PutOpts{})
		if err != nil {
			t.Fatalf("Failed to upload object %d: %v", i, err)
		}
	}

	// Delete all
	for i, obj := range objs {
		err := adapter.Remove(ctx, obj)
		if err != nil {
			t.Fatalf("Failed to delete object %d: %v", i, err)
		}
	}

	// Verify all deleted
	stillExists, err := adapter.VerifyDeleted(ctx, objs)
	if err != nil {
		t.Fatalf("Verification failed: %v", err)
	}
	if len(stillExists) > 0 {
		t.Fatalf("Expected 0 objects to remain, got %d", len(stillExists))
	}

	t.Logf("✅ Verified deletion of %d objects!", numObjects)
}