package s3_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/block"
	"github.com/treeverse/lakefs/pkg/block/blocktest"
	"github.com/treeverse/lakefs/pkg/block/params"
	s3a "github.com/treeverse/lakefs/pkg/block/s3"
)

func getChecksumBlockAdapter(t *testing.T, endpoint, accessKeyID, secretAccessKey string, opts ...s3a.AdapterOption) *s3a.Adapter {
	t.Helper()
	adapter, err := s3a.NewAdapter(t.Context(), params.S3{
		Region:               "us-east-1",
		Endpoint:             endpoint,
		ForcePathStyle:       true,
		DiscoverBucketRegion: false,
		Credentials: params.S3Credentials{
			AccessKeyID:     accessKeyID,
			SecretAccessKey: secretAccessKey,
		},
	}, append(opts, s3a.WithNowFactory(blocktest.NowMockDefault))...)
	require.NoError(t, err, "cannot create s3 adapter")
	return adapter
}

// TestS3AdapterUploadNotFound verifies that operations on an unknown or already
// consumed upload surface block.ErrDataNotFound rather than a generic error.
func TestS3AdapterUploadNotFound(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	adapter := getChecksumBlockAdapter(t, blockURL, minioTestAccessKeyID, minioTestSecretAccessKey)
	obj := block.ObjectPointer{
		StorageNamespace: "s3://" + bucketName,
		Identifier:       "checksum/no-such-upload",
		IdentifierType:   block.IdentifierTypeRelative,
	}

	err := adapter.AbortMultiPartUpload(ctx, obj, "no-such-upload-id")
	require.ErrorIs(t, err, block.ErrDataNotFound)

	_, err = adapter.CompleteMultiPartUpload(ctx, obj, "no-such-upload-id", &block.MultipartUploadCompletion{
		Part: []block.MultipartPart{{PartNumber: 1, ETag: "etag"}},
	})
	require.ErrorIs(t, err, block.ErrDataNotFound)
}

// TestS3AdapterChecksumCapabilityProbe verifies full-object checksum requests
// against a store without default checksums (the suite's MinIO): reject before any
// part upload, learn the capability for discovery, leave regular uploads unaffected.
// The positive path needs a store with default checksums and is covered by esti
// (TestPresignMultipartUploadFullObjectChecksum).
func TestS3AdapterChecksumCapabilityProbe(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	adapter := getChecksumBlockAdapter(t, blockURL, minioTestAccessKeyID, minioTestSecretAccessKey)
	obj := block.ObjectPointer{
		StorageNamespace: "s3://" + bucketName,
		Identifier:       "checksum/no-default-checksums",
		IdentifierType:   block.IdentifierTypeRelative,
	}

	t.Run("disable knob turns discovery off", func(t *testing.T) {
		disabled := getChecksumBlockAdapter(t, blockURL, minioTestAccessKeyID, minioTestSecretAccessKey,
			s3a.WithDisablePreSignedMultipartChecksum(true))
		info := disabled.GetStorageNamespaceInfo("")
		require.True(t, info.PreSignSupportMultipart, "knob must not affect presign multipart itself")
		require.False(t, info.MultipartChecksumSupport)
	})

	t.Run("unsupported algorithm rejected at create", func(t *testing.T) {
		_, err := adapter.CreateMultiPartUpload(ctx, obj, nil, block.CreateMultiPartUploadOpts{
			ChecksumAlgorithm: "CRC32",
			ChecksumType:      block.ChecksumTypeFullObject,
		})
		require.ErrorIs(t, err, block.ErrOperationNotSupported)
	})

	t.Run("store without default checksums fails at create", func(t *testing.T) {
		require.True(t, adapter.GetStorageNamespaceInfo("").MultipartChecksumSupport,
			"capability is advertised until the store proves otherwise")
		opts := block.CreateMultiPartUploadOpts{
			ChecksumAlgorithm: block.ChecksumAlgorithmCRC64NVME,
			ChecksumType:      block.ChecksumTypeFullObject,
		}
		_, err := adapter.CreateMultiPartUpload(ctx, obj, nil, opts)
		require.ErrorIs(t, err, block.ErrOperationNotSupported)

		// the probe outcome is learned: discovery flips to false and further
		// requests are rejected from cache
		require.False(t, adapter.GetStorageNamespaceInfo("").MultipartChecksumSupport,
			"failed probe must flip the discovery flag")
		_, err = adapter.CreateMultiPartUpload(ctx, obj, nil, opts)
		require.ErrorIs(t, err, block.ErrOperationNotSupported)

		// uploads without checksum validation are unaffected
		createResp, err := adapter.CreateMultiPartUpload(ctx, obj, nil, block.CreateMultiPartUploadOpts{})
		require.NoError(t, err)
		_ = adapter.AbortMultiPartUpload(ctx, obj, createResp.UploadID)
	})
}
