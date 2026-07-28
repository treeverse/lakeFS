package api

import (
	"testing"

	"github.com/go-openapi/swag"
	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/api/apigen"
	"github.com/treeverse/lakefs/pkg/block"
)

func TestFullObjectChecksumFromBody(t *testing.T) {
	crc64nvme := apigen.ChecksumAlgorithm_CRC64NVME
	fullObject := apigen.ChecksumType_FULL_OBJECT
	// "6naeR5H+cx0=" is a base64-encoded 8-byte value
	validValue := "6naeR5H+cx0="

	tests := []struct {
		name       string
		body       apigen.CompletePresignMultipartUploadJSONRequestBody
		want       *block.FullObjectChecksum
		wantErrMsg string
	}{
		{
			name: "no checksum fields",
			body: apigen.CompletePresignMultipartUploadJSONRequestBody{},
			want: nil,
		},
		{
			name: "checksum with algorithm",
			body: apigen.CompletePresignMultipartUploadJSONRequestBody{
				ChecksumAlgorithm: &crc64nvme,
				Checksum:          swag.String(validValue),
			},
			want: &block.FullObjectChecksum{
				Algorithm: block.ChecksumAlgorithmCRC64NVME,
				Type:      block.ChecksumTypeFullObject,
				Value:     validValue,
			},
		},
		{
			name: "checksum with algorithm and explicit type",
			body: apigen.CompletePresignMultipartUploadJSONRequestBody{
				ChecksumAlgorithm: &crc64nvme,
				ChecksumType:      &fullObject,
				Checksum:          swag.String(validValue),
			},
			want: &block.FullObjectChecksum{
				Algorithm: block.ChecksumAlgorithmCRC64NVME,
				Type:      block.ChecksumTypeFullObject,
				Value:     validValue,
			},
		},
		{
			name: "everything with object size",
			body: apigen.CompletePresignMultipartUploadJSONRequestBody{
				ChecksumAlgorithm: &crc64nvme,
				ChecksumType:      &fullObject,
				Checksum:          swag.String(validValue),
				MpuObjectSize:     swag.Int64(42),
			},
			want: &block.FullObjectChecksum{
				Algorithm:     block.ChecksumAlgorithmCRC64NVME,
				Type:          block.ChecksumTypeFullObject,
				Value:         validValue,
				MpuObjectSize: swag.Int64(42),
			},
		},
		{
			name: "object size only",
			body: apigen.CompletePresignMultipartUploadJSONRequestBody{
				MpuObjectSize: swag.Int64(42),
			},
			want: &block.FullObjectChecksum{
				Type:          block.ChecksumTypeFullObject,
				MpuObjectSize: swag.Int64(42),
			},
		},
		{
			name: "unsupported checksum type",
			body: apigen.CompletePresignMultipartUploadJSONRequestBody{
				ChecksumAlgorithm: &crc64nvme,
				ChecksumType:      apiutilPtr(apigen.ChecksumType("COMPOSITE")),
				Checksum:          swag.String(validValue),
			},
			wantErrMsg: "only FULL_OBJECT checksum_type is supported",
		},
		{
			name: "checksum without algorithm",
			body: apigen.CompletePresignMultipartUploadJSONRequestBody{
				Checksum: swag.String(validValue),
			},
			wantErrMsg: "checksum requires checksum_algorithm",
		},
		{
			name: "algorithm without checksum",
			body: apigen.CompletePresignMultipartUploadJSONRequestBody{
				ChecksumAlgorithm: &crc64nvme,
			},
			wantErrMsg: "checksum_algorithm requires checksum",
		},
		{
			name: "type without checksum and algorithm",
			body: apigen.CompletePresignMultipartUploadJSONRequestBody{
				ChecksumType: &fullObject,
			},
			wantErrMsg: "checksum_type requires checksum and checksum_algorithm",
		},
		{
			name: "unknown algorithm",
			body: apigen.CompletePresignMultipartUploadJSONRequestBody{
				ChecksumAlgorithm: apiutilPtr(apigen.ChecksumAlgorithm("CRC32")),
				Checksum:          swag.String(validValue),
			},
			wantErrMsg: `invalid checksum_algorithm "CRC32"`,
		},
		{
			name: "checksum not base64",
			body: apigen.CompletePresignMultipartUploadJSONRequestBody{
				ChecksumAlgorithm: &crc64nvme,
				Checksum:          swag.String("not base64!"),
			},
			wantErrMsg: "checksum must be base64-encoded",
		},
		{
			name: "checksum wrong length",
			body: apigen.CompletePresignMultipartUploadJSONRequestBody{
				ChecksumAlgorithm: &crc64nvme,
				Checksum:          swag.String("AAAAAA=="), // 4 bytes, CRC64NVME takes 8
			},
			wantErrMsg: "checksum of algorithm CRC64NVME must encode 8 bytes",
		},
		{
			name: "negative object size",
			body: apigen.CompletePresignMultipartUploadJSONRequestBody{
				MpuObjectSize: swag.Int64(-1),
			},
			wantErrMsg: "mpu_object_size cannot be negative",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, msg := fullObjectChecksumFromBody(tt.body)
			require.Equal(t, tt.wantErrMsg, msg)
			require.Equal(t, tt.want, got)
		})
	}
}

func apiutilPtr[T any](v T) *T {
	return &v
}
